"""Frozen GoogleSQL export of the independently authored dbt baseline.

This module never imports the spine's accounting graph and performs no cloud
operations. Parsing and local diagnostic execution do not establish BigQuery
parity. The future runner must verify the source identity and every dbt test.
"""
from datetime import date, timedelta
from decimal import Decimal
import hashlib
from importlib.metadata import version as package_version
import json
import os
from pathlib import Path
import platform
import re
import stat
import subprocess

from jinja2 import Environment, StrictUndefined
import sqlglot
from sqlglot import exp
import yaml

from tl.bigquery.compiler import lower, parse
from tl.bigquery.config import Binding
from tl.bigquery.schema import FIELDS, visible
from tl.stream.events import Catalog, ValidationError, canonical, iso, timestamp


ROOT = Path(__file__).resolve().parents[2]
TEMPLATES = ROOT / 'baseline/bigquery'
PORT_ORIGINS = {
    'dim_date': '86b4232a2dccb2509ed8aa68951d59b3cf7b5f58943eed115ea7138ea83a8006',
    'dim_customer': '561719abf6b76d14f28ea2b46eb5d9973ed0facae89fada6784ef353462edf83',
}


def _hash(data):
    return hashlib.sha256(data).hexdigest()


def _sha(value, label):
    if not isinstance(value, str) or not re.fullmatch('[0-9a-f]{64}', value):
        raise ValidationError(f'{label} must be a SHA-256 hex string')
    return value


def _identifier(value):
    if not isinstance(value, str) or not re.fullmatch('[A-Za-z_][A-Za-z0-9_]{0,127}', value):
        raise ValidationError('unsafe baseline dataset identifier')
    return value


def _day(value, label):
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
        raise ValidationError(f'{label} must be an ISO date')
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValidationError(f'invalid {label}') from exc


def _runtime():
    runtime = dict(python=platform.python_version(), jinja2=package_version('Jinja2'), sqlglot=package_version('sqlglot'))
    for line in (TEMPLATES / 'requirements-port.txt').read_text(encoding='utf-8').splitlines():
        if not line.strip() or line.startswith('#'):
            continue
        name, required = line.strip().split('==')
        if runtime.get(name.lower()) != required:
            raise ValidationError('baseline export requires its pinned Jinja2 and SQLGlot runtime')
    return runtime


def _paired_unnest(tree):
    """Zip literal SELECT-list arrays without a Cartesian product.

    The original journal is the accounting policy. Its array element expressions
    are copied from its AST into one ARRAY<STRUCT>; no postings are reimplemented.
    Unequal arrays and other expansion shapes fail instead of guessing padding.
    """
    for select in list(tree.find_all(exp.Select)):
        expansions = [v for v in select.find_all(exp.Explode) if v.find_ancestor(exp.Select) is select]
        if len(expansions) == 1:
            value = expansions[0]
            if not isinstance(value.this, exp.Array):
                raise ValidationError('baseline UNNEST requires a literal array')
            unnest = exp.Unnest(expressions=[value.this.copy()],
                alias=exp.TableAlias(columns=[exp.to_identifier('_tl_element')]))
            value.replace(exp.column('_tl_element'))
            if select.args.get('from_'):
                select.set('joins', [*(select.args.get('joins') or []), exp.Join(this=unnest, kind='CROSS')])
            else:
                select.set('from_', exp.From(this=unnest))
            continue
        pairs = []
        for index, projection in enumerate(select.expressions):
            value = projection.this if isinstance(projection, exp.Alias) else projection
            if isinstance(value, exp.Explode):
                if not isinstance(value.this, exp.Array):
                    raise ValidationError('baseline paired UNNEST requires literal arrays')
                pairs.append((index, projection, value.this.expressions))
        if not pairs:
            if sum(v.find_ancestor(exp.Select) is select for v in select.find_all(exp.Explode)) > 1:
                raise ValidationError('unsupported nested paired SELECT-list expansion')
            continue
        if len(pairs) != 2 or len(pairs[0][2]) != len(pairs[1][2]) or not pairs[0][2]:
            raise ValidationError('baseline paired UNNEST requires two equal nonempty arrays')
        if any(t.alias_or_name == '_tl_pair' for t in select.find_all(exp.Table)):
            raise ValidationError('paired UNNEST alias collision')
        structs = [exp.Struct(expressions=[
            exp.PropertyEQ(this=exp.to_identifier(f'_v{i}'), expression=value.copy())
            for i, value in enumerate(values)]) for values in zip(*(p[2] for p in pairs))]
        unnest = exp.Unnest(expressions=[exp.Array(expressions=structs)],
            alias=exp.TableAlias(columns=[exp.to_identifier('_tl_pair')]))
        select.set('joins', [*(select.args.get('joins') or []), exp.Join(this=unnest, kind='CROSS')])
        projections = list(select.expressions)
        for i, (index, original, _) in enumerate(pairs):
            value = exp.column(f'_v{i}', table='_tl_pair')
            projections[index] = exp.alias_(value, original.alias) if original.alias else value
        select.set('expressions', projections)
    return tree


def _fromless_predicates(tree):
    """Give GoogleSQL the implicit single input row of SELECT ... WHERE.

    Assertion predicates and projections remain unchanged. Fresh identifiers
    keep this neutral relation from shadowing correlated outer references.
    """
    used = {name.name.casefold() for name in tree.find_all(exp.Identifier)}
    def fresh(base):
        name = base
        while name.casefold() in used:
            name += '_'
        used.add(name.casefold())
        return name
    column, relation = fresh('_tl_unit'), fresh('_tl_one_row')
    for select in list(tree.find_all(exp.Select)):
        if select.args.get('where') is not None and select.args.get('from_') is None:
            unit = exp.select(exp.alias_(exp.Literal.number(1), column)).subquery(relation)
            select.set('from_', exp.From(this=unit))
    return tree


def _google(sql):
    tree = _paired_unnest(parse(sql))
    for aggregate in tree.find_all(exp.ArrayAgg):
        order = aggregate.this
        if isinstance(order, exp.Order) and isinstance(order.this, exp.Distinct):
            # GoogleSQL DISTINCT aggregate sort keys must equal the argument.
            # The baseline collects the required, non-null physical activity_id.
            # Its NULL placement is immaterial; do not add an IS NULL sort key.
            values = order.this.expressions
            terms = order.expressions
            if (len(values) != 1 or not isinstance(values[0], exp.Column)
                    or values[0].name != 'activity_id' or len(terms) != 1
                    or terms[0].this != values[0]):
                raise ValidationError('ordered DISTINCT array needs an explicit nullable-key port')
            terms[0].set('nulls_first', not bool(terms[0].args.get('desc')))
    tree = _fromless_predicates(lower(tree))
    for node in list(tree.walk())[::-1]:
        if isinstance(node, exp.Month):
            node.replace(exp.Extract(this=exp.Var(this='MONTH'), expression=node.this.copy()))
        elif isinstance(node, exp.Interval) and isinstance(node.this, exp.Literal) and node.this.this.isdigit():
            node.set('this', exp.Literal.number(node.this.this))
    for table in tree.find_all(exp.Table):
        if table.catalog:
            table.meta['quoted_table'] = True
    output = tree.sql(dialect='bigquery', pretty=True, unsupported_level=sqlglot.ErrorLevel.RAISE)
    parse(output, 'bigquery')
    return output + '\n'


def _render(body, macros, variables, models, sources, project, raw_dataset, model_dataset, *, native=False):
    dependencies = set()
    def relation(dataset, name):
        if native:
            return f'`{project}.{dataset}.{name}`'
        return '.'.join('"' + part + '"' for part in (project, dataset, name))
    def ref(name):
        if name not in models:
            raise ValidationError(f'unregistered baseline ref: {name}')
        dependencies.add(('model', name))
        return relation(model_dataset, name)
    def source(group, name):
        if group != 'raw' or name not in sources:
            raise ValidationError('unregistered baseline source')
        dependencies.add(('source', name))
        return relation(raw_dataset, name)
    def var(name):
        if name not in variables:
            raise ValidationError(f'unfrozen baseline variable: {name}')
        return variables[name]
    env = Environment(undefined=StrictUndefined, keep_trailing_newline=True)
    env.globals.update(ref=ref, source=source, var=var, config=lambda **kwargs: '')
    output = env.from_string(macros + '\n' + body).render()
    if native:
        parse(output, 'bigquery')
        output = output.strip() + '\n'
    else:
        output = _google(output)
    return output, sorted(dependencies)


def _topology(graph):
    remaining = {name: {dep for kind, dep in deps if kind == 'model'} for name, deps in graph.items()}
    order = []
    while remaining:
        ready = sorted(name for name, deps in remaining.items() if not deps)
        if not ready:
            raise ValidationError('baseline model graph is cyclic')
        order.extend(ready)
        for name in ready:
            del remaining[name]
        for deps in remaining.values():
            deps.difference_update(ready)
    return order


def _definition(version, cents):
    if version not in ('v2', 'v3') or type(cents) is not int:
        raise ValidationError('baseline supports released NRR v2/v3 with integer cents')
    path = ROOT / f'definitions/metrics/consumption_nrr/{version}.yaml'
    definition = yaml.safe_load(path.read_text(encoding='utf-8'))
    expected = {'base_t12m': (12, 1, False, cents), 'floor_100k': (12, 1, False, 10000000),
                't3m_annualized': (3, 4, False, 1000000), 'subscription_inclusive': (12, 1, True, 1000000)}
    actual = {v['lens']: (v['months'], v['annualization'], v['include_subscription'],
                         int(Decimal(str(v['floor_usd'])) * 100)) for v in definition['lenses']}
    if definition['version'] != version or actual != expected:
        raise ValidationError('NRR version/floor/lenses do not match the immutable definition')
    return path


def export_baseline(source_binding, output_dir, *, raw_dataset, model_dataset,
                    asof, known_at, watermark, first_month, nrr_version,
                    nrr_floor_cents, input_identity, schema_identity,
                    baseline_directory=None):
    """Export a fresh, immutable, credential-free project. Returns its manifest.

    Identity is the complete source snapshot fingerprint, not the projection
    fingerprint (revenue_recognized assertions are deliberately not a raw input).
    Source and both derived datasets share Binding.project and Binding.location.
    No job, lookup, credential discovery, or IAM assertion occurs here.
    """
    root = Path(output_dir)
    if root.exists():
        raise ValidationError('baseline export requires a new directory')
    runtime = _runtime()
    if not isinstance(source_binding, Binding):
        raise ValidationError('an explicit validated BigQuery Binding is required')
    raw_dataset, model_dataset = _identifier(raw_dataset), _identifier(model_dataset)
    if len({source_binding.dataset.casefold(), raw_dataset.casefold(), model_dataset.casefold()}) != 3:
        raise ValidationError('source, raw and model datasets must be distinct')
    end, first = _day(asof, 'asof'), _day(first_month, 'first_month')
    if (end + timedelta(days=1)).day != 1:
        raise ValidationError('asof must be a calendar month-end')
    if first.day != 1 or first > end:
        raise ValidationError('first_month must start a month at or before asof')
    knowledge = iso(timestamp(known_at))
    if type(watermark) is not int or watermark < 1:
        raise ValidationError('watermark must be a positive explicit integer')
    _sha(schema_identity, 'schema_identity')
    if schema_identity != Catalog(ROOT / 'definitions/activities').digest:
        raise ValidationError('source schema identity does not match the activity catalog')
    if not isinstance(input_identity, dict):
        raise ValidationError('a complete frozen source fingerprint is required')
    input_identity = json.loads(canonical(input_identity))
    _sha(input_identity.get('sha256'), 'input_identity.sha256')
    bounds = input_identity.get('input_row_range')
    count = input_identity.get('activity_count')
    if (input_identity.get('algorithm') != 'cross-engine-logical-json/v1'
            or input_identity.get('columns') != sorted(v[0] for v in FIELDS)
            or type(count) is not int or count < 1 or input_identity.get('rows') != count
            or not isinstance(bounds, list) or len(bounds) != 2
            or any(type(v) is not int for v in bounds)
            or not 1 <= bounds[0] <= bounds[1] <= watermark or count > bounds[1] - bounds[0] + 1
            or any(not isinstance(input_identity.get(k), str) or not input_identity[k]
                   for k in ('activity_id_min', 'activity_id_max'))):
        raise ValidationError('frozen source fingerprint violates the 14-field/watermark contract')
    nrr_path = _definition(nrr_version, nrr_floor_cents)
    baseline = Path(baseline_directory) if baseline_directory is not None else ROOT / 'baseline'
    model_paths = {p.stem: p for p in sorted((baseline / 'models').rglob('*.sql'))}
    if len(model_paths) != len(list((baseline / 'models').rglob('*.sql'))):
        raise ValidationError('duplicate baseline model names')
    tests = sorted((baseline / 'tests').glob('*.sql'))
    source_doc = yaml.safe_load((baseline / 'models/sources.yml').read_text(encoding='utf-8'))
    if len(source_doc['sources']) != 1 or source_doc['sources'][0]['name'] != 'raw':
        raise ValidationError('unsupported baseline source group')
    sources = [v['name'] for v in source_doc['sources'][0]['tables']]
    expected = sorted(p.stem for p in (ROOT / 'definitions/activities').glob('*.yaml') if p.stem != 'revenue_recognized') + ['allocation']
    if len(sources) != len(set(sources)) or set(sources) != set(expected):
        raise ValidationError('baseline source catalog mismatch')
    macro_paths = sorted((baseline / 'macros').glob('*.sql'))
    allocation_paths = sorted((ROOT / 'definitions/allocation').glob('*.yaml'))
    pins = [*model_paths.values(), *tests, *macro_paths, baseline / 'dbt_project.yml', baseline / 'models/sources.yml',
            baseline / 'outputs.yaml', nrr_path, *allocation_paths,
            *(ROOT / 'definitions/activities').glob('*.yaml'), Path(__file__), ROOT / 'tl/bigquery/compiler.py',
            ROOT / 'tl/bigquery/schema.py', ROOT / 'tl/bigquery/config.py', ROOT / 'tl/bigquery/hashing.py',
            ROOT / 'tl/stream/events.py', TEMPLATES / 'requirements-port.txt',
            *(TEMPLATES / f'{name}.sql.j2' for name in PORT_ORIGINS)]
    frozen_hashes = {path: _hash(path.read_bytes()) for path in pins}
    macros = '\n'.join(p.read_text(encoding='utf-8') for p in macro_paths)
    variables = dict(asof=asof, first_month=first_month, nrr_floor_cents=nrr_floor_cents, changed_since=0)
    files, graph, records = {}, {}, {}
    def retain(name, text):
        files[name] = text.encode('utf-8') if isinstance(text, str) else text
    def render(path, category):
        name, body = path.stem, path.read_text(encoding='utf-8')
        native = category == 'models' and name in PORT_ORIGINS
        if native:
            if _hash(path.read_bytes().replace(b'\r\n', b'\n')) != PORT_ORIGINS[name]:
                raise ValidationError(f'{name} changed; its explicit dialect port needs review')
            body = (TEMPLATES / f'{name}.sql.j2').read_text(encoding='utf-8')
        sql, deps = _render(body, macros, variables, model_paths, sources,
                            source_binding.project, raw_dataset, model_dataset, native=native)
        relative = path.relative_to(baseline).as_posix()
        comments = ''.join("-- depends_on: {{ " + (f"ref('{dep}')" if kind == 'model' else f"source('raw', '{dep}')") + " }}\n"
                           for kind, dep in deps)
        retain('project/' + relative, comments + sql)
        retain('sql/' + category + '/' + name + '.sql', sql)
        records[category + '/' + name] = dict(source=relative, source_sha256=_hash(path.read_bytes()),
            sql_path='sql/' + category + '/' + name + '.sql', dependencies=deps,
            dialect_port='bounded-parent-walk' if name == 'dim_customer' else 'date-array' if native else 'generic-ast')
        if category == 'models':
            graph[name] = deps
    for path in model_paths.values():
        render(path, 'models')
    for path in tests:
        render(path, 'tests')
    order = _topology(graph)
    source_sql = visible(source_binding, period_end=(end + timedelta(days=1)).isoformat(), known_at=knowledge, watermark=watermark)
    retain('source/snapshot.sql', source_sql + '\n')
    retain('source/checks.sql', f"""SELECT COUNTIF(_lane != 'sim') AS non_synthetic_rows,
COUNTIF(_schema_hash != '{schema_identity}') AS schema_mismatches,
COUNT(*) - COUNT(DISTINCT activity_id) AS duplicate_activity_ids,
COUNT(*) - COUNT(DISTINCT _stream_position) AS duplicate_positions
FROM ({source_sql})
""")
    columns = ', '.join('`' + v[0] + '`' for v in FIELDS)
    for kind in sorted(set(sources) - {'allocation'}):
        retain(f'projections/{kind}.sql', f"CREATE TABLE `{source_binding.project}.{raw_dataset}.{kind}` AS\nSELECT {columns} FROM ({source_sql}) WHERE activity = '{kind}';\n")
    allocation = []
    for path in allocation_paths:
        spec = yaml.safe_load(path.read_text(encoding='utf-8'))
        if (spec.get('synthetic') is not True or spec.get('version') != 'v1'
                or spec.get('unit') != 'MWh per million tokens' or spec.get('model') != path.stem
                or not re.fullmatch('[a-z0-9-]+', spec['model'])):
            raise ValidationError('invalid synthetic allocation model')
        for kind, coefficient in sorted(spec['coefficients'].items()):
            energy = Decimal(str(coefficient)) * 1000000000
            if not re.fullmatch('[a-z_]+', kind) or energy <= 0 or energy != int(energy):
                raise ValidationError('invalid integer allocation coefficient')
            allocation.append(f"SELECT '{spec['model']}' AS model, '{kind}' AS token_type, {int(energy)} AS coefficient_nano")
    retain('projections/allocation.sql', f'CREATE TABLE `{source_binding.project}.{raw_dataset}.allocation` AS\n' + '\nUNION ALL\n'.join(allocation) + ';\n')
    project = yaml.safe_load((baseline / 'dbt_project.yml').read_text(encoding='utf-8'))
    project['vars'] = variables
    project['on-run-start'] = ['{{ tokenledger_frozen_binding() }}']
    retain('project/dbt_project.yml', yaml.safe_dump(project, sort_keys=False))
    source_doc['sources'][0].update(database=source_binding.project, schema=raw_dataset)
    retain('project/models/sources.yml', yaml.safe_dump(source_doc, sort_keys=False))
    # dbt comments retain real lineage, while rendered SQL cannot drift with vars.
    # The hook rejects a profile/var override that would label frozen SQL falsely.
    checks = {'type': 'bigquery', 'project': source_binding.project, 'dataset': model_dataset, 'location': source_binding.location}
    hook = ['{% macro tokenledger_frozen_binding() %}']
    for key, value in checks.items():
        hook.append("{% if target.get('" + key + "') != " + json.dumps(value) + " %}{{ exceptions.raise_compiler_error('frozen baseline target mismatch: " + key + "') }}{% endif %}")
    for key, value in variables.items():
        hook.append("{% if var('" + key + "') != " + json.dumps(value) + " %}{{ exceptions.raise_compiler_error('frozen baseline variable mismatch: " + key + "') }}{% endif %}")
    hook.extend(["{{ return('SELECT 1 AS frozen_baseline_binding') }}", '{% endmacro %}'])
    retain('project/macros/frozen_binding.sql', '\n'.join(hook) + '\n')
    retain('outputs.yaml', (baseline / 'outputs.yaml').read_bytes())
    source_pins = {}
    for path in pins:
        if _hash(path.read_bytes()) != frozen_hashes[path]:
            raise ValidationError('baseline execution inputs changed during export')
        try:
            label = path.relative_to(ROOT).as_posix()
        except ValueError:
            label = 'baseline/' + path.relative_to(baseline).as_posix()
        source_pins[label] = frozen_hashes[path]
    if Catalog(ROOT / 'definitions/activities').digest != schema_identity:
        raise ValidationError('activity catalog changed during export')
    for name, data in files.items():
        if name.endswith('.sql') and not name.startswith('project/'):
            parse(data.decode('utf-8'), 'bigquery')
    revision = subprocess.run(['git', '-C', str(ROOT), 'rev-parse', 'HEAD'], capture_output=True, text=True)
    source_sha = revision.stdout.strip() if revision.returncode == 0 else None
    manifest = dict(schema='tokenledger-bigquery-baseline-export/v1', status='exported', native_parity='not_run',
        runtime=runtime,
        git_sha=source_sha, source_revision_note='checkout SHA; original_sources binds actual bytes, including uncommitted edits',
        semantic_validation='not_run; SQL parse only', source=dict(project=source_binding.project,
            dataset=source_binding.dataset, table=source_binding.table, location=source_binding.location,
            binding_identity=source_binding.identity, schema_identity=schema_identity,
            physical_schema=list(FIELDS), physical_schema_sha256=_hash(canonical(FIELDS).encode()),
            input_identity=input_identity, input_fingerprint_keys=['activity_id']),
        derived=dict(project=source_binding.project, raw_dataset=raw_dataset, model_dataset=model_dataset,
            location=source_binding.location), cutoffs=dict(asof=asof, known_at=knowledge, watermark=watermark,
            first_month=first_month), definition=dict(name='consumption_nrr', version=nrr_version,
            floor_cents=nrr_floor_cents, sha256=_hash(nrr_path.read_bytes())), variables=variables,
        execution=dict(status='not_run', maximum_bytes_billed=source_binding.maximum_bytes_billed,
            maximum_run_bytes_billed=source_binding.maximum_run_bytes_billed,
            budget_enforcement='not_run; export does not submit or govern jobs', cache_policy='runner must bind and record',
            required_gates=['source physical schema/retention check', 'snapshot fingerprint before and after',
                            'source/checks.sql zero failures', 'all independent dbt tests', 'all 13 output populations']),
        counts=dict(dbt_models=len(model_paths), dbt_singular_tests=len(tests), raw_projection_tables=len(sources),
                    original_business_macros=len(macro_paths), generated_binding_macros=1),
        recursive_parent_limit=498, recursive_overflow_policy='dimension_completeness must fail; never publish incomplete roots',
        model_order=order, graph=graph, artifacts=records, original_sources=source_pins,
        files={name: dict(sha256=_hash(data), bytes=len(data)) for name, data in sorted(files.items())})
    try:
        root.mkdir(parents=True, exist_ok=False)
    except FileExistsError as exc:
        raise ValidationError('baseline export requires a new directory') from exc
    for name, data in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    (root / 'manifest.json').write_text(canonical(manifest) + '\n', encoding='utf-8', newline='\n')
    return manifest


def _no_reparse(path):
    metadata = path.lstat()
    if (stat.S_ISLNK(metadata.st_mode)
            or getattr(metadata, 'st_file_attributes', 0) & getattr(stat, 'FILE_ATTRIBUTE_REPARSE_POINT', 0x400)):
        raise ValidationError('baseline export refuses symlinks, junctions and reparse points')
    return metadata


def _safe_relative(value):
    if (not isinstance(value, str) or not re.fullmatch('[A-Za-z0-9_./-]+', value)
            or any(part in ('', '.', '..') for part in value.split('/'))):
        raise ValidationError('unsafe baseline artifact name')
    return value


def verify_export(root, *, expected_manifest_sha256=None):
    """Verify exact retained bytes offline; this does not verify source or parity.

    The optional externally retained manifest hash prevents a changed manifest
    from redefining the accepted inventory. No ambient credentials are consulted.
    """
    root = Path(root)
    if '..' in root.parts:
        raise ValidationError('unsafe baseline artifact root')
    root = root.absolute()  # Do not resolve through a link before inspecting it.
    for path in (root, *root.parents):
        _no_reparse(path)
    if not root.is_dir():
        raise ValidationError('baseline export root must be a directory')
    _no_reparse(root / 'manifest.json')
    manifest_bytes = (root / 'manifest.json').read_bytes()
    manifest_hash = _hash(manifest_bytes)
    if expected_manifest_sha256 is not None:
        _sha(expected_manifest_sha256, 'expected_manifest_sha256')
        if manifest_hash != expected_manifest_sha256:
            raise ValidationError('baseline manifest does not match its external hash')
    def unique_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValidationError('duplicate baseline manifest key')
            result[key] = value
        return result
    manifest = json.loads(manifest_bytes, object_pairs_hook=unique_pairs)
    if manifest.get('schema') != 'tokenledger-bigquery-baseline-export/v1':
        raise ValidationError('unsupported baseline export manifest')
    files = manifest.get('files')
    if not isinstance(files, dict) or not files:
        raise ValidationError('missing baseline file inventory')
    expected_dirs, expected_files = set(), {'manifest.json'}
    for relative, expected in files.items():
        _safe_relative(relative)
        if relative == 'manifest.json' or not isinstance(expected, dict):
            raise ValidationError('invalid baseline file inventory')
        _sha(expected.get('sha256'), 'artifact sha256')
        if type(expected.get('bytes')) is not int or expected['bytes'] < 0:
            raise ValidationError('invalid baseline artifact size')
        expected_files.add(relative)
        parts = relative.split('/')
        expected_dirs.update('/'.join(parts[:n]) for n in range(1, len(parts)))
    if len({name.casefold() for name in expected_files}) != len(expected_files):
        raise ValidationError('ambiguous baseline artifact names')
    found_files, found_dirs, pending = set(), set(), [root]
    while pending:
        directory = pending.pop()
        with os.scandir(directory) as entries:
            for entry in entries:
                path = Path(entry.path)
                metadata = _no_reparse(path)
                relative = path.relative_to(root).as_posix()
                _safe_relative(relative)
                if stat.S_ISDIR(metadata.st_mode):
                    found_dirs.add(relative)
                    pending.append(path)
                elif stat.S_ISREG(metadata.st_mode):
                    found_files.add(relative)
                else:
                    raise ValidationError('unsupported baseline artifact file type')
    if found_files != expected_files or found_dirs != expected_dirs:
        raise ValidationError('baseline export has missing or unlisted files/directories')
    for relative, expected in manifest['files'].items():
        path = root / relative
        _no_reparse(path)
        data = path.read_bytes()
        if _hash(data) != expected['sha256'] or len(data) != expected['bytes']:
            raise ValidationError(f'baseline export artifact mismatch: {relative}')
    return dict(status='verified', verified=True, recomputed=False, native_parity='not_run',
                files=len(manifest['files']), manifest_sha256=manifest_hash,
                external_manifest_hash_verified=expected_manifest_sha256 is not None)
