"""Native diagnostic evaluation with portable, explicitly scoped re-performance."""
from pathlib import Path
from datetime import datetime, timezone
import hashlib
import json
import shutil
import uuid

import pyarrow.parquet as pq

from tl.bigquery.client import Client
from tl.bigquery.hashing import fingerprint
from tl.evolution import diagnostic as d
from tl.receipts.metrics import (append_receipts, code_revision, digest, execution_artifacts,
                                read_receipts, receipt_id)
from tl.stream import ValidationError
from tl.stream.events import canonical

SCHEMA = 'tokenledger-reporting-learning-evaluation/v1'
OBSERVATION = 'tokenledger-reporting-learning-observation/v1'


def native_run(*args, **kwargs):
    from tl.bigquery.engine import run
    return run(*args, **kwargs)


def read_export(*args, **kwargs):
    from tl.bigquery.export import read_export as read
    return read(*args, **kwargs)


def artifacts():
    result = execution_artifacts([])
    for path in [*Path('tl/evolution').rglob('*.sql'), *Path('definitions/reporting-learning').rglob('*.yaml')]:
        result[path.as_posix()] = hashlib.sha256(path.read_bytes().replace(b'\r\n', b'\n')).hexdigest()
    return dict(sorted(result.items()))


def write(path, value):
    with Path(path).open('x', encoding='utf-8', newline='\n') as handle:
        handle.write(canonical(value) + '\n')


def seal(root, record, ledger):
    root = Path(root)
    record = {**record, 'files': {p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
                                for p in sorted(root.rglob('*')) if p.is_file()}}
    record['receipt_id'] = receipt_id(record)
    write(root / 'receipt.json', record)
    append_receipts(ledger, [record])
    return record


def verify_files(root, record):
    root = Path(root).resolve()
    for name, expected in record['files'].items():
        path = (root / name).resolve()
        if not path.is_relative_to(root) or not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise ValidationError('learning evidence content differs: ' + name)


def evaluate(binding, export, *, output_root, ledger, client=None, inspection=None):
    manifest = read_export(export, binding)
    if set(manifest['outputs']) != {'consumption_nrr'}:
        raise ValidationError('learning evaluation requires an NRR-only frozen native export')
    spec = d.cases()
    if manifest['asof'] != spec['reporting_date']:
        raise ValidationError('native report date differs from the frozen evaluation cases')
    pins = artifacts(); revision = code_revision(pins)
    if not revision['execution_artifacts_match_git']:
        raise ValidationError('freeze the exact execution and candidate in Git before native evaluation')
    client = client or Client(binding)
    requests = spec['cases'] if inspection is None else [dict(case_id=inspection['case_id'],
        population='nrr', requested_date=inspection['requested_date'], scenario='original')]
    job_start = len(client.jobs)
    root = Path(output_root) / uuid.uuid4().hex
    root.mkdir(parents=True, exist_ok=False)
    write(root / 'started.json', dict(status='running', recorded_at=datetime.now(timezone.utc).isoformat()))
    try:
        # Existing native reporting engine performs domain checks, source checks,
        # produces its own report receipt, and preserves its exact SQL export.
        native = native_run(binding, export, output_root=output_root, ledger=ledger, client=client)
        original_root = Path(native['run_directory'])
        original_records = read_receipts(original_root / 'receipts.jsonl')
        business_receipt = next(r for r in original_records.values() if r['schema_version'] == 'tokenledger-bigquery/v1')
        original = pq.read_table(original_root / 'consumption_nrr.parquet').drop(['receipt_id'])
        report_sql = (Path(export) / 'queries/consumption_nrr.sql').read_text(encoding='utf-8')
        sql = d.query(report_sql, requests)
        (root / 'candidate.sql').write_text(sql, encoding='utf-8', newline='\n')
        actual, _ = client.query(sql, label='learning_candidate_evaluation')
        source, _ = client.query((Path(export) / 'snapshot.sql').read_text(encoding='utf-8'), label='learning_source_recheck')
        if fingerprint(source, ['activity_id'], source=True) != manifest['inputs']:
            raise ValidationError('learning source changed during evaluation')
        actual_rows = sorted(actual.to_pylist(), key=lambda row: row['case_id'])
        reference = []
        for case in requests:
            answer = d.oracle(d.scenario(original.to_pylist(), case['scenario']), case['requested_date'], case.get('population','nrr'))
            if 'expected' in case and answer['status'] != case['expected']:
                raise ValidationError('native population does not exercise frozen case: ' + case['case_id'])
            reference.append(dict(case_id=case['case_id'], **answer))
        reference.sort(key=lambda row: row['case_id'])
        offline = d.offline(original.to_pylist(), requests).to_pylist()
        matched = actual_rows == reference == offline
        result = dict(status='passed' if matched else 'failed', cases=len(reference),
                      equivalent=matched, diagnostic_rows=actual_rows, oracle_rows=reference,
                      report_receipt_id=business_receipt['receipt_id'],
                      baseline_behavior='An unmatched date selects zero rows without an executable explanation.',
                      report_population_changed=False, reporting_controls_changed=False,
                      native_execution=True, held_out_accuracy='not_measured', ongoing_activation=False,
                      measurement='frozen_evaluation' if inspection is None else 'inspection_diagnostic')
        pq.write_table(original, root / 'report.parquet')
        pq.write_table(source, root / 'source.parquet')
        pq.write_table(actual, root / 'diagnostics.parquet')
        write(root / 'requests.json', requests)
        write(root / 'business-receipt.json', business_receipt)
        write(root / 'jobs.json', client.jobs[job_start:])
        write(root / 'oracle.json', reference)
        shutil.copyfile(d.CASES, root / 'frozen-cases.yaml')
        if artifacts() != pins:
            raise ValidationError('learning execution changed during evaluation')
        record = seal(root, dict(schema_version=SCHEMA, run_id=root.name, result=result,
            definition_version='nrr-date-diagnostic/v1', inputs=manifest['inputs'],
            query_hash=hashlib.sha256(sql.encode()).hexdigest(), execution_artifacts=pins, execution_hash=digest(pins),
            git_sha=revision['git_sha'], asof=manifest['asof'], known_at=manifest['known_at'], watermark=manifest['watermark'],
            binding_identity=binding.identity, candidate_sha256=d.pins()[d.SQL.as_posix()],
            cases_sha256=d.pins()[d.CASES.as_posix()]), ledger)
        return dict(status=result['status'], receipt_id=record['receipt_id'], run_directory=str(root.resolve()),
                    native_report_directory=str(original_root.resolve()), result=result)
    except BaseException as exc:
        write(root / 'failure.json', dict(status='failed', error_type=type(exc).__name__,
                                         error=str(exc), jobs=client.jobs[job_start:]))
        if isinstance(exc, Exception) and not (root/'receipt.json').exists():
            record = seal(root, dict(schema_version=OBSERVATION, run_id=root.name,
                result=dict(status='failed', error_type=type(exc).__name__, jobs=client.jobs[job_start:],
                            native_evaluation='incomplete', ongoing_activation=False),
                definition_version='nrr-date-diagnostic/v1', inputs=manifest['inputs'],
                query_hash=digest(manifest['files']) if 'files' in manifest else digest(manifest),
                execution_hash=digest(pins), git_sha=revision['git_sha'], binding_identity=binding.identity), ledger)
            raise ValidationError('native learning evaluation failed; retained observation '+record['receipt_id']) from exc
        raise


def replay(ids, *, output_root, ledger):
    records = read_receipts(ledger)
    results = []
    for key in ids:
        record = records.get(key)
        if not record or record['schema_version'] not in (SCHEMA, OBSERVATION):
            raise ValidationError('unknown reporting learning receipt')
        root = Path(output_root) / record['run_id']
        verify_files(root, record)
        if record['schema_version'] == OBSERVATION:
            results.append(dict(receipt_id=key, verified=True, recomputed=False, result=record['result']))
            continue
        if record['execution_artifacts'] != artifacts():
            raise ValidationError('replay requires the retained learning execution revision')
        original = pq.read_table(root / 'report.parquet')
        source = pq.read_table(root / 'source.parquet')
        if fingerprint(source, ['activity_id'], source=True) != record['inputs']:
            raise ValidationError('learning source fingerprint differs')
        native_receipt = json.loads((root / 'business-receipt.json').read_bytes())
        if (receipt_id(native_receipt) != native_receipt['receipt_id']
                or fingerprint(original, ['month','lens']) != native_receipt['result']['population']):
            raise ValidationError('learning original report differs from its receipt')
        requests = json.loads((root / 'requests.json').read_bytes())
        actual = d.offline(original.to_pylist(), requests).to_pylist()
        expected = record['result']['diagnostic_rows']
        oracle = sorted([dict(case_id=case['case_id'], **d.oracle(d.scenario(original.to_pylist(), case['scenario']),
            case['requested_date'], case.get('population','nrr'))) for case in requests], key=lambda row: row['case_id'])
        if actual != expected or oracle != record['result']['oracle_rows']:
            raise ValidationError('learning diagnostic reproduction failed')
        results.append(dict(receipt_id=key, verified=True, recomputed='diagnostic_from_retained_population',
                            native_sql_reexecuted=False, business_recognition_recomputed=False, result=record['result']))
    return results
