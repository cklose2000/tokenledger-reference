"""Frozen plain-SQL deployments. Export is not execution or parity evidence."""
from pathlib import Path
import hashlib
import json

from tl.bigquery.compiler import compile_query, parse
from tl.bigquery.schema import ddl, visible
from tl.bigquery.hashing import fingerprint
from tl.compare.engine import artifacts
from tl.compare.outputs import registry
from tl.receipts.metrics import code_revision, digest
from tl.stream import ValidationError
from tl.stream.events import canonical, iso
from tl.views.hashing import input_manifest
from tl.views.session import ViewSession
from tl.views.validation import required


SCAFFOLDS = ('customer_timeline', 'token_ledger', 'revenue_ledger', 'compute_ledger', 'seat_ledger', 'customer_month')


def export(db, binding, directory, *, asof, known_at=None, watermark=None, version='v2', names=()):
    root = Path(directory)
    if root.exists() and any(root.iterdir()): raise ValidationError('export requires a new or empty directory')
    contract = registry(); names = list(names) or list(contract['outputs'])
    if len(names) != len(set(names)) or set(names)-set(contract['outputs']):
        raise ValidationError('export requires distinct registered output names')
    pins = artifacts(); revision = code_revision(pins)
    files = {}; dependencies = {}; descriptions = {}
    with ViewSession(db, asof=asof, known_at=known_at, watermark=watermark) as session:
        inputs = input_manifest(session)
        portable_inputs = fingerprint(session.conn.execute('SELECT * FROM _visible').to_arrow_table(), ['activity_id'], source=True)
        # Local accounting checks are useful admission evidence, not BQ acceptance.
        session.validate_dependencies(names)
        for name in names:
            sql, deps = compile_query(session, session.output_sql(name, version), binding)
            files['queries/'+name+'.sql'] = sql+'\n'; dependencies[name] = deps
            descriptions[name] = contract['outputs'][name]
        for domain in sorted(required(names)):
            files['checks/'+domain+'.sql'] = compile_query(session, 'SELECT * FROM @'+domain, binding)[0]+'\n'
        for name in SCAFFOLDS:
            query, _ = compile_query(session, 'SELECT * FROM @'+name, binding)
            files['scaffolds/'+name+'.sql'] = query+'\n'
        files['stream.sql'] = ddl(binding)
        files['snapshot.sql'] = visible(binding, period_end=session.period_end,
                                       known_at=iso(session.known_at), watermark=session.watermark)+'\n'
        manifest = dict(schema_version='tokenledger-bigquery-export/v1', status='exported',
            native_execution='not_run', asof=asof, known_at=iso(session.known_at), watermark=session.watermark,
            definition_version=version, binding_identity=binding.identity,
            binding=dict(project=binding.project, dataset=binding.dataset, location=binding.location),
            inputs=portable_inputs, local_native_inputs=inputs, execution_artifacts=pins, execution_hash=digest(pins), **revision,
            outputs=descriptions, dependencies=dependencies,
            validation_domains=sorted(required(names)), local_validation=session.validation_results,
            physical_source_columns=14, persisted_reporting_intermediates=0,
            parent_depth_limit=498, files={})
    if artifacts()!=pins: raise ValidationError('execution files changed during export')
    for name, content in files.items():
        parse(content, 'bigquery')
        path = root/name; path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding='utf-8', newline='\n')
        manifest['files'][name] = hashlib.sha256(content.encode()).hexdigest()
    (root/'manifest.json').write_text(canonical(manifest)+'\n', encoding='utf-8', newline='\n')
    return dict(status='exported', directory=str(root.resolve()), outputs=len(names),
                manifest_sha256=digest(manifest), native_execution='not_run', binding_identity=binding.identity)


def read_export(directory, binding):
    root = Path(directory).resolve()
    manifest = json.loads((root/'manifest.json').read_text(encoding='utf-8'))
    if manifest['schema_version']!='tokenledger-bigquery-export/v1' or manifest['binding_identity']!=binding.identity:
        raise ValidationError('export does not match the explicit cloud binding')
    if digest(manifest['execution_artifacts'])!=manifest['execution_hash']:
        raise ValidationError('export execution identity differs')
    if artifacts()!=manifest['execution_artifacts']:
        raise ValidationError('export execution changed; use its original checkout or export again')
    for name, expected in manifest['files'].items():
        path=(root/name).resolve()
        if not path.is_relative_to(root) or hashlib.sha256(path.read_bytes()).hexdigest()!=expected:
            raise ValidationError('export SQL bytes changed: '+name)
    return manifest
