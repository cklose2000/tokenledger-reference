"""One native correctness trial using independent dbt and spine execution.

Run in the separately pinned dbt worker environment. This is a fresh full-build
trial, not an incremental benchmark or a completed performance experiment.
"""
from dataclasses import asdict
from contextlib import contextmanager
from collections import Counter
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import shutil
from time import perf_counter
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from tl.bigquery.baseline_sql import verify_export
from tl.bigquery.config import Binding
from tl.bigquery.export import read_export
from tl.bigquery.hashing import fingerprint
from tl.compare.matching import compare_tables
from tl.receipts.metrics import append_receipts, digest, receipt_id
from tl.stream import ValidationError
from tl.stream.events import canonical


def _write(path, value):
    Path(path).write_text(canonical(value) + '\n', encoding='utf-8', newline='\n')


def _sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _retain_export(source, manifest, destination):
    """Copy the verified inventory, including its externally pinned manifest."""
    for name in ['manifest.json', *manifest['files']]:
        target = destination / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source / name, target)


def preflight(binding, baseline_directory, native_directory, *, baseline_sha256, native_sha256):
    base, native = Path(baseline_directory), Path(native_directory)
    verify_export(base, expected_manifest_sha256=baseline_sha256)
    if _sha(native / 'manifest.json') != native_sha256:
        raise ValidationError('exports differ from the externally pinned trial request')
    manifest = json.loads((base / 'manifest.json').read_text(encoding='utf-8'))
    spine = read_export(native, binding)
    if binding.query_principal or binding.writer_principal:
        raise ValidationError('this correctness trial requires explicit operator scratch mode for both architectures')
    if binding.location != 'us-east4':
        raise ValidationError('this registered trial requires explicit us-east4')
    if manifest['source']['binding_identity'] != binding.identity:
        raise ValidationError('baseline export binding differs')
    if manifest['source']['input_identity'] != spine['inputs']:
        raise ValidationError('baseline and spine source populations differ')
    for field in ('asof', 'known_at', 'watermark'):
        if manifest['cutoffs'][field] != spine[field]:
            raise ValidationError('baseline and spine cutoff differs: ' + field)
    if manifest['definition']['version'] != spine['definition_version']:
        raise ValidationError('baseline and spine definition differs')
    registry = yaml.safe_load((base / 'outputs.yaml').read_text(encoding='utf-8'))['outputs']
    if registry != spine['outputs'] or len(registry) != 13:
        raise ValidationError('trial requires all thirteen registered output contracts')
    derived = manifest['derived']
    if derived['project'] != binding.project or derived['location'] != binding.location:
        raise ValidationError('baseline derived project or region differs')
    if any(not value.startswith('tokenledger_baseline_') for value in
           (derived['raw_dataset'], derived['model_dataset'])):
        raise ValidationError('fresh derived datasets must use the tokenledger_baseline_ prefix')
    if len({binding.dataset, derived['raw_dataset'], derived['model_dataset']}) != 3:
        raise ValidationError('source, raw and model datasets must be distinct')
    return manifest, spine


def _profile(binding, model_dataset, threads):
    return {'tokenledger_baseline': {'target': 'benchmark', 'outputs': {'benchmark': {
        'type': 'bigquery', 'method': 'oauth', 'project': binding.project,
        'dataset': model_dataset, 'location': binding.location, 'threads': threads,
        'job_retries': 0, 'job_creation_timeout_seconds': 60,
        'job_execution_timeout_seconds': 900,
        'maximum_bytes_billed': binding.maximum_bytes_billed,
        'priority': 'interactive'}}}}


_CLOCK_SCOPES = {
    'input_admission': 'retain and verify exports, copy project, profile setup, client creation and source metadata validation',
    'source_before_query': 'pair source query: estimate, server wait and REST download combined',
    'source_before_hash': 'pair source logical fingerprint and comparison before both architectures',
    'source_checks_query': 'pair source assertions: estimate, server wait and REST download combined',
    'source_checks_validation': 'validate returned source assertion inventory and zero failures',
    'baseline': 'inclusive baseline dataset creation, raw projection, dbt build/tests and output retrieval',
    'baseline_dataset_setup': 'fresh derived dataset metadata creation',
    'baseline_projections': 'all raw projections: estimate, server wait and REST download combined',
    'baseline_dbt_build_and_tests': 'dbt command wall including compilation, model/test jobs and adapter overhead',
    'baseline_dbt_result_validation': 'validate complete dbt model, test and hook results',
    'baseline_output_queries': 'all baseline output queries: estimate, server wait and REST download combined',
    'native': 'inclusive native execution including its two source checks, domains, outputs, hashes, reports and receipts',
    'native_source_queries': 'existing native source queries: estimate, server wait and REST download combined',
    'native_domain_queries': 'existing native domain queries: estimate, server wait and REST download combined',
    'native_output_queries': 'existing native output queries: estimate, server wait and REST download combined',
    'output_comparison': 'read native Parquets and compare every registered output to independent baseline tables',
    'baseline_population_hashing': 'logical baseline output fingerprinting',
    'baseline_receipt_artifacts': 'construct baseline receipts and write receipt-bearing Parquets',
    'source_after_query': 'pair source query: estimate, server wait and REST download combined',
    'source_after_hash': 'pair source logical fingerprint and comparison after both architectures',
    'export_postflight': 'reverify both retained executable exports after both architectures',
    'job_evidence_refresh': 'refresh and validate detailed job evidence, no query submission',
    'baseline_ledger_append': 'append baseline population receipts to the trial ledger',
}


_ACCOUNTED_COMPONENTS = {
    'baseline': ['source_before_query', 'source_before_hash', 'source_checks_query',
                 'source_checks_validation', 'baseline', 'source_after_query', 'source_after_hash',
                 'baseline_population_hashing', 'baseline_receipt_artifacts', 'baseline_ledger_append'],
    'native': ['native'],
    'shared': ['input_admission', 'output_comparison', 'export_postflight', 'job_evidence_refresh'],
}


class _Clocks:
    """Inclusive stages and child clocks are labeled; their sums are not wall time."""
    def __init__(self, measurements):
        self.stages = measurements['stages'] = {
            name: dict(status='not_run', wall_seconds=None, calls=0, scope=scope)
            for name, scope in _CLOCK_SCOPES.items()}
        self.boundaries = measurements['stage_boundaries'] = []

    @contextmanager
    def measure(self, name):
        item = self.stages[name]
        tick = perf_counter()
        item['calls'] += 1
        prior_failed = item['status'] == 'failed'
        item['status'] = 'running'
        self.boundaries.append(dict(stage=name, event='start', at=datetime.now(timezone.utc).isoformat()))
        try:
            yield
        except BaseException as exc:
            item['status'] = 'failed'
            item['failure'] = dict(type=type(exc).__name__, message=str(exc))
            raise
        else:
            item['status'] = 'failed' if prior_failed else 'complete'
        finally:
            item['wall_seconds'] = (item['wall_seconds'] or 0) + perf_counter() - tick
            self.boundaries.append(dict(stage=name, event=item['status'], at=datetime.now(timezone.utc).isoformat()))

    def require(self, names):
        missing = sorted(name for name in names if self.stages[name]['status'] != 'complete')
        if missing:
            raise ValidationError('required trial stages did not complete: ' + ', '.join(missing))

    def accounted(self):
        result = {}
        for name, components in _ACCOUNTED_COMPONENTS.items():
            complete = all(self.stages[key]['status'] == 'complete' for key in components)
            observed = sum(self.stages[key]['wall_seconds'] or 0 for key in components)
            result[name] = dict(status='complete' if complete else 'incomplete',
                wall_seconds=observed if complete else None,
                observed_component_wall_seconds=observed, components=list(components),
                scope='additive disjoint measured work intervals; not contiguous end-to-end elapsed time')
        return result


def run(binding, baseline_directory, native_directory, directory, *,
        baseline_sha256, native_sha256, threads=4, architecture_order='baseline-first'):
    """Submit one bounded trial; retain partial failures without repair or retry."""
    from google.cloud import bigquery
    from tl.bigquery.baseline_jobs import DbtJobRecorder
    from tl.bigquery.client import Client
    from tl.bigquery.engine import run as native_run
    from tl.bigquery.boundary_evidence import record

    if type(threads) is not int or not 1 <= threads <= 4:
        raise ValidationError('registered trial threads must be between one and four')
    if architecture_order not in ('baseline-first', 'native-first'):
        raise ValidationError('architecture order must be baseline-first or native-first')
    base, native = Path(baseline_directory), Path(native_directory)
    manifest, spine = preflight(binding, base, native, baseline_sha256=baseline_sha256,
                                native_sha256=native_sha256)
    # Keep code and dependency selection reviewable before credential discovery.
    versions = {name: importlib.metadata.version(name) for name in
                ('dbt-core', 'dbt-bigquery', 'google-cloud-bigquery', 'sqlglot')}
    if versions != {'dbt-core': '1.12.3', 'dbt-bigquery': '1.12.0',
                    'google-cloud-bigquery': '3.45.0', 'sqlglot': '30.18.0'}:
        raise ValidationError('native baseline worker differs from pinned versions')
    lock = Path(__file__).resolve().parents[2] / 'baseline/bigquery/requirements-worker-lock.txt'
    installed = {}
    for line in lock.read_text(encoding='utf-8').splitlines():
        if not line.strip() or line.startswith('#'):
            continue
        name, expected = line.split('==', 1)
        installed[name] = importlib.metadata.version(name)
        if installed[name] != expected:
            raise ValidationError('native baseline worker dependency differs: ' + name)
    root = Path(directory).absolute()
    if root.exists():
        raise ValidationError('a native trial requires a fresh local output directory')
    root.mkdir(parents=True)
    started = perf_counter()
    run_id = uuid.uuid4().hex
    result = dict(schema_version='tokenledger-native-baseline-trial/v1', status='running',
        run_id=run_id, started_at=datetime.now(timezone.utc).isoformat(), binding_identity=binding.identity,
        baseline_manifest_sha256=baseline_sha256, native_manifest_sha256=native_sha256,
        inputs=spine['inputs'], cutoffs=manifest['cutoffs'], definition=manifest['definition'],
        versions=versions, worker_dependencies=installed, worker_lock_sha256=_sha(lock),
        threads=threads, architecture_order=architecture_order,
        architecture_sequence=['baseline', 'native'] if architecture_order == 'baseline-first' else ['native', 'baseline'],
        completed_architectures=[], measurements={}, outputs={},
        scope='single fresh full-build correctness trial; performance repetitions not_run',
        matched_performance='not_run', production_acceptance='not_established')
    clocks = _Clocks(result['measurements'])
    result['measurements']['native_subclocks'] = dict(status='not_separately_instrumented',
        fields=['source_hashing', 'population_hashing', 'report_writing', 'receipt_writing'],
        scope='included in native wall and its derived non-query remainder; no engine instrumentation')
    _write(root / 'binding.json', asdict(binding))
    _write(root / 'trial.json', result)
    project = root / 'dbt-project'
    profile_root = root / 'profile'
    raw_dataset, model_dataset = (manifest['derived'][key] for key in ('raw_dataset', 'model_dataset'))
    records = []
    def query(sql, label, *, key=None, with_job=False):
        tick = perf_counter()
        measurement = dict(status='running', wall_seconds=None, job_id=None,
            scope='query call wall: estimate, server wait and REST download combined; not server elapsed')
        result['measurements'][key or label] = measurement
        try:
            table, job = client.query(sql, label=label)
            measurement.update(status='complete', job_id=job['job_id'])
            return (table, job) if with_job else table
        except BaseException:
            measurement['status'] = 'failed'
            raise
        finally:
            measurement['wall_seconds'] = perf_counter() - tick
    def check_source(label):
        with clocks.measure(label + '_query'):
            table = query((base / 'source/snapshot.sql').read_text(encoding='utf-8'), label)
        with clocks.measure(label + '_hash'):
            if fingerprint(table, ['activity_id'], source=True) != spine['inputs']:
                raise ValidationError('source changed or differs from the frozen input')
    def save():
        _write(root / 'trial.json', result)
    def baseline_stage():
        # Dataset creation is explicit and fresh, never exists_ok. Partial
        # attempts remain for review; there is no automatic DROP or source write.
        with clocks.measure('baseline_dataset_setup'):
            for name in (raw_dataset, model_dataset):
                dataset = bigquery.Dataset(binding.project + '.' + name)
                dataset.location = binding.location
                client.sdk.create_dataset(dataset, exists_ok=False)
        with clocks.measure('baseline_projections'):
            projections = sorted(name for name in manifest['files'] if name.startswith('projections/'))
            if not projections:
                raise ValidationError('baseline projection stage is missing')
            for name in projections:
                query((base / name).read_text(encoding='utf-8'), name)
        args = ['--no-use-colors', '--no-partial-parse', 'build',
                '--project-dir', str(project), '--profiles-dir', str(profile_root),
                '--target-path', str(root / 'dbt-artifacts'), '--log-path', str(root / 'dbt-logs')]
        os.environ['DBT_SEND_ANONYMOUS_USAGE_STATS'] = 'false'
        os.environ['DO_NOT_TRACK'] = '1'
        with clocks.measure('baseline_dbt_build_and_tests'):
            invocation = jobs.invoke_dbt(args)
            result['dbt_success'] = bool(invocation.success)
            if not invocation.success:
                raise ValidationError('native dbt build/tests failed; inspect retained artifacts and logs')
        # Preserve the previous diagnostic key while making its scope explicit.
        result['measurements']['dbt_build_and_tests'] = dict(clocks.stages['baseline_dbt_build_and_tests'])
        with clocks.measure('baseline_dbt_result_validation'):
            dbt_results = json.loads((root / 'dbt-artifacts/run_results.json').read_text(encoding='utf-8'))
            expected = {('model', name) for name in manifest['model_order']}
            expected |= {('test', key.split('/', 1)[1]) for key in manifest['artifacts'] if key.startswith('tests/')}
            observed, all_ids = set(), set()
            for row in dbt_results['results']:
                unique_id = row['unique_id']
                if unique_id in all_ids:
                    raise ValidationError('duplicate dbt result identity')
                all_ids.add(unique_id)
                kind, package, name = unique_id.split('.', 2)
                if package != 'tokenledger_baseline' or kind not in ('model', 'test', 'operation'):
                    raise ValidationError('unexpected dbt package or resource type')
                if kind in ('model', 'test'):
                    if row['status'] != ('success' if kind == 'model' else 'pass'):
                        raise ValidationError('baseline test or model did not pass')
                    observed.add((kind, name))
                elif row['status'] != 'success':
                    raise ValidationError('baseline hook did not pass')
            if observed != expected:
                raise ValidationError('dbt did not execute the complete registered model and test inventory')
        tables = {}
        with clocks.measure('baseline_output_queries'):
            for name, spec in spine['outputs'].items():
                table = query(f'SELECT * FROM `{binding.project}.{model_dataset}.{name}`', 'baseline/' + name)
                if set(table.column_names) != set(spec['columns']):
                    raise ValidationError('baseline output columns differ: ' + name)
                tables[name] = table
        return tables

    def native_stage():
        expected = {'source_snapshot': 'native_source_queries', 'source_recheck': 'native_source_queries'}
        expected.update({name: 'native_domain_queries' for name in spine['validation_domains']})
        expected.update({name: 'native_output_queries' for name in spine['outputs']})
        if len(expected) != 2 + len(spine['validation_domains']) + len(spine['outputs']):
            raise ValidationError('native query stage labels collide')
        observed = []
        class ObservedClient:
            def __getattr__(self, name):
                return getattr(client, name)
            def query(self, sql, *, label):
                if label not in expected or label in observed:
                    raise ValidationError('unexpected or repeated native query stage: ' + label)
                observed.append(label)
                with clocks.measure(expected[label]):
                    # Keep the original job record and all transport behavior.
                    return query(sql, label, key='native/' + label, with_job=True)
        value = native_run(binding, native, output_root=root / 'native-runs',
                           ledger=root / 'metrics.jsonl', client=ObservedClient())
        result['native'] = value
        if value.get('status') != 'recorded' or set(observed) != set(expected):
            raise ValidationError('native execution did not complete every registered query stage')
        return value

    def capture_jobs(start):
        # Durable reservations include uncertain submissions whose observation is
        # still null. Read only this stage's new rows, without another cloud call.
        with (root / 'jobs.jsonl').open('rb') as ledger:
            ledger.seek(start)
            return [row['job_id'] for line in ledger if (row := json.loads(line))['event'] == 'reserved'
                    and row['configuration'].get('dryRun') is not True]

    @contextmanager
    def attribute_jobs(architecture):
        offset = (root / 'jobs.jsonl').stat().st_size
        try:
            yield
        finally:
            result['architecture_jobs'][architecture].extend(capture_jobs(offset))

    def verify_job_attribution():
        actual = capture_jobs(0)
        baseline_ids, native_ids = (result['architecture_jobs'][name] for name in ('baseline', 'native'))
        attributed = baseline_ids + native_ids
        duplicates = sorted(key for key, count in Counter(attributed).items() if count != 1)
        missing, extra = sorted(set(actual) - set(attributed)), sorted(set(attributed) - set(actual))
        overlap = sorted(set(baseline_ids) & set(native_ids))
        repeated_reservations = sorted(key for key, count in Counter(actual).items() if count != 1)
        valid = not (duplicates or missing or extra or overlap or repeated_reservations)
        result['job_attribution'] = dict(status='verified' if valid else 'failed',
            actual_reservations=len(actual), attributed_reservations=len(attributed),
            missing_ids=missing, extra_ids=extra, duplicate_ids=duplicates, overlapping_ids=overlap,
            duplicate_reservation_ids=repeated_reservations,
            scope='every non-dry query reservation exactly once, including uncertain submissions; not a success or billing claim')
        if not valid:
            raise ValidationError('architecture job attribution differs from actual query reservations')

    result['architecture_jobs'] = dict(baseline=[], native=[])
    try:
        with clocks.measure('input_admission'):
            # Retain both exports even on failure, then execute only reverified copies.
            profile_root.mkdir()
            (profile_root / 'profiles.yml').write_text(yaml.safe_dump(
                _profile(binding, manifest['derived']['model_dataset'], threads)), encoding='utf-8')
            _retain_export(base, manifest, root / 'baseline-export')
            _retain_export(native, spine, root / 'native-export')
            base, native = root / 'baseline-export', root / 'native-export'
            preflight(binding, base, native, baseline_sha256=baseline_sha256, native_sha256=native_sha256)
            for name in manifest['files']:
                if name.startswith('project/'):
                    target = project / name.removeprefix('project/')
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes((base / name).read_bytes())
            client = Client(binding)
            client.validate_source()
        with DbtJobRecorder(binding, root / 'jobs.jsonl', run_id=run_id,
                source_datasets={binding.dataset}, derived_datasets={raw_dataset, model_dataset}) as jobs:
            try:
                # These source controls span the pair and are attributed to the
                # baseline, matching the native engine's own source work. No
                # additional reads or source writes are introduced.
                with attribute_jobs('baseline'):
                    check_source('source_before')
                with attribute_jobs('baseline'):
                    with clocks.measure('source_checks_query'):
                        checks = query((base / 'source/checks.sql').read_text(encoding='utf-8'), 'source_checks').to_pylist()
                    with clocks.measure('source_checks_validation'):
                        result['source_checks'] = checks
                        expected_checks = {'non_synthetic_rows', 'schema_mismatches',
                                           'duplicate_activity_ids', 'duplicate_positions'}
                        if (len(checks) != 1 or set(checks[0]) != expected_checks
                                or any(type(value) is not int or value != 0 for value in checks[0].values())):
                            raise ValidationError('native baseline source checks failed')
                stages = {'baseline': baseline_stage, 'native': native_stage}
                stage_results = {}
                for name in result['architecture_sequence']:
                    offset = (root / 'jobs.jsonl').stat().st_size
                    try:
                        with clocks.measure(name):
                            stage_results[name] = stages[name]()
                        result['completed_architectures'].append(name)
                    finally:
                        result['architecture_jobs'][name].extend(capture_jobs(offset))
                        if name == 'native':
                            spent = sum(clocks.stages[key]['wall_seconds'] or 0 for key in
                                        ('native_source_queries', 'native_domain_queries', 'native_output_queries'))
                            result['measurements']['native_harness_remainder'] = dict(
                                status='derived', wall_seconds=clocks.stages['native']['wall_seconds'] - spent,
                                scope='native inclusive wall minus nested query-call wall; hashing, serialization, reports and other Python overhead combined')
                        save()
                tables, native_result = stage_results['baseline'], stage_results['native']
                if set(tables) != set(spine['outputs']):
                    raise ValidationError('baseline did not produce all thirteen registered populations')
                for name, spec in spine['outputs'].items():
                    with clocks.measure('output_comparison'):
                        actual = pq.read_table(Path(native_result['run_directory']) / (name + '.parquet')).drop(['receipt_id'])
                        if set(actual.column_names) != set(spec['columns']):
                            raise ValidationError('native retained output columns differ: ' + name)
                        table = tables[name]
                        comparison = compare_tables(actual, table, spec['key'])
                    with clocks.measure('baseline_population_hashing'):
                        population = fingerprint(table, spec['key'])
                    with clocks.measure('baseline_receipt_artifacts'):
                        row = dict(schema_version='tokenledger-bigquery-baseline/v1', run_id=run_id,
                            result=dict(output=name, population=population, comparison=comparison),
                            definition_version='accounting/v1;nrr/' + spine['definition_version'],
                            inputs=spine['inputs'], query_hash=manifest['files']['sql/models/' + name + '.sql']['sha256'],
                            git_sha=spine['git_sha'], execution_hash=spine['execution_hash'],
                            baseline_export_sha256=baseline_sha256, binding_identity=binding.identity,
                            asof=spine['asof'], known_at=spine['known_at'], watermark=spine['watermark'])
                        row['receipt_id'] = receipt_id(row)
                        records.append(row)
                        pq.write_table(table.append_column('receipt_id', pa.array(
                            [row['receipt_id']] * len(table), type=pa.string())), root / (name + '-baseline.parquet'))
                        result['outputs'][name] = dict(receipt_id=row['receipt_id'], comparison=comparison)
                with attribute_jobs('baseline'):
                    check_source('source_after')
                with clocks.measure('export_postflight'):
                    preflight(binding, base, native, baseline_sha256=baseline_sha256, native_sha256=native_sha256)
                clocks.require(set(_CLOCK_SCOPES) - {'job_evidence_refresh', 'baseline_ledger_append'})
                result['status'] = ('equivalent' if all(item['comparison']['status'] == 'equivalent'
                    for item in result['outputs'].values()) else 'different')
            finally:
                try:
                    with clocks.measure('job_evidence_refresh'):
                        result['jobs'] = jobs.refresh(client.sdk)
                        if result['jobs'].get('rejected_observations'):
                            raise ValidationError('returned job identity or configuration failed the evidence boundary')
                finally:
                    verify_job_attribution()
                    save()
    except BaseException as exc:
        result.update(status='failed', failure=dict(type=type(exc).__name__, message=str(exc)))
    if records:
        try:
            with clocks.measure('baseline_ledger_append'):
                append_receipts(root / 'metrics.jsonl', records)
        except BaseException as exc:
            result.update(status='failed', failure=dict(type=type(exc).__name__, message=str(exc)))
    result['finished_at'] = datetime.now(timezone.utc).isoformat()
    result['measurements']['accounted_wall'] = clocks.accounted()
    result['measurements']['accounted_wall_exclusions'] = dict(
        observation_sealing='not_separately_instrumented; outside trial body clock',
        other_harness='stage-ledger attribution, inter-stage report writes and orchestration remain outside accounted components',
        native_delivery_scope='native internal export copy and state/report serialization are inside native wall; baseline admitted export copy and profile setup are shared input admission')
    result['measurements']['end_to_end'] = dict(wall_seconds=perf_counter() - started,
        scope='trial body and population-ledger append; excludes preflight before directory creation and final observation sealing')
    save()
    # Evidence verification is offline and labels itself as an observation.
    # No successful result is inferred from the existence of this receipt.
    proof = {p.relative_to(root).as_posix(): p for p in root.rglob('*') if p.is_file()
             and not p.name.endswith(('.lock', '.oslock'))}
    observation = record(result, root / 'observation', binding=binding, artifacts=proof,
                         ledger=root / 'observation-ledger.jsonl')
    return dict(status=result['status'], directory=str(root), observation=observation,
                outputs=len(result['outputs']), matched_performance='not_run',
                architecture_order=architecture_order, completed_architectures=result['completed_architectures'])
