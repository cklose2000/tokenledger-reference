"""Four serial, externally pinned paired trials under one remaining allowance.

This module generates no exports, retries no trials, and refunds no reservation.
The operator allocates a remaining byte allowance after considering earlier
authorized work; a new output directory is not a new spending authorization.
"""
from contextlib import chdir
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import statistics
import subprocess

from tl.bigquery.baseline_runner import preflight
from tl.bigquery.baseline_sql import ROOT, _no_reparse
from tl.bigquery.boundary_evidence import record, verify
from tl.bigquery.config import Binding
from tl.receipts.journal import ledger_lock
from tl.receipts.metrics import digest, read_receipts
from tl.stream import ValidationError
from tl.stream.events import canonical


SCHEMA = 'tokenledger-bigquery-matched-plan/v2'
TRANSPORT = 'REST-Arrow-no-Storage-Read-API'
PIN_FIELDS = ('git_sha', 'worker_lock_sha256', 'worker_python_sha256')
SCOPE_NAMES = {'comparison', 'boundary_observation'}
PLAN_FIELDS = {'schema_version', 'suite_id', 'output_directory', 'binding',
    'aggregate_maximum_bytes_billed', 'authorization_basis', 'threads', 'transport', 'trials',
    'execution_scopes', *PIN_FIELDS}
TRIAL_FIELDS = {'case_id', 'role', 'repetition', 'architecture_order', 'baseline_directory',
    'baseline_sha256', 'native_directory', 'native_sha256', 'directory'}


def _sha(path):
    result = hashlib.sha256()
    with Path(path).open('rb') as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()


def _path(value):
    path = Path(value)
    if not path.is_absolute() or '..' in path.parts:
        raise ValidationError('suite paths must be explicit absolute paths without parent traversal')
    for ancestor in (path, *path.parents):
        if ancestor.exists() or ancestor.is_symlink():
            _no_reparse(ancestor)
    return path


def _write(path, value):
    with Path(path).open('x', encoding='utf-8', newline='\n') as out:
        out.write(canonical(value) + '\n')
        out.flush()
        os.fsync(out.fileno())


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValidationError('duplicate suite plan key')
        result[key] = value
    return result


def execution_scopes():
    """The two real producers intentionally hash different artifact populations."""
    from tl.compare.engine import artifacts
    from tl.receipts.metrics import execution_artifacts
    with chdir(ROOT):
        populations = {'comparison': artifacts(), 'boundary_observation': execution_artifacts([])}
    return {name: dict(execution_hash=digest(files), artifacts=files)
            for name, files in populations.items()}


def _validate_scopes(scopes):
    if not isinstance(scopes, dict) or set(scopes) != SCOPE_NAMES:
        raise ValidationError('both named execution artifact scopes must be independently pinned')
    for scope in scopes.values():
        if not isinstance(scope, dict) or set(scope) != {'execution_hash', 'artifacts'}:
            raise ValidationError('execution scope requires its complete artifact map and hash')
        files = scope['artifacts']
        if (not isinstance(files, dict) or not files
                or any(not isinstance(name, str) or not re.fullmatch(r'[A-Za-z0-9_./-]+', name)
                    or any(part in ('', '.', '..') for part in name.split('/'))
                    or not isinstance(value, str) or not re.fullmatch(r'[a-f0-9]{64}', value)
                    for name, value in files.items())
                or scope['execution_hash'] != digest(files)):
            raise ValidationError('execution scope artifact map differs from its hash')


def _runtime_matches(runtime, plan):
    return (all(runtime.get(field) == plan[field] for field in PIN_FIELDS)
            and runtime.get('execution_scopes') == plan['execution_scopes'])


def _run_id(value):
    if not isinstance(value, str) or not re.fullmatch(r'[a-f0-9]{32}', value):
        raise ValidationError('invalid retained run identity')
    return value


def load_plan(path, expected_sha256):
    path = _path(path)
    if not re.fullmatch(r'[a-f0-9]{64}', expected_sha256 or '') or _sha(path) != expected_sha256:
        raise ValidationError('suite plan differs from its external SHA-256 pin')
    plan = json.loads(path.read_bytes(), object_pairs_hook=_unique)
    if set(plan) != PLAN_FIELDS or plan['schema_version'] != SCHEMA:
        raise ValidationError('unsupported suite plan schema or fields')
    _validate_scopes(plan['execution_scopes'])
    if not isinstance(plan['suite_id'], str) or not re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,63}', plan['suite_id']):
        raise ValidationError('suite_id must be a safe explicit identifier')
    binding = Binding(**plan['binding'])
    if binding.location != 'us-east4' or binding.query_principal or binding.writer_principal:
        raise ValidationError('matched suite requires explicit us-east4 operator scratch binding')
    if (type(plan['aggregate_maximum_bytes_billed']) is not int
            or plan['aggregate_maximum_bytes_billed'] < 4 * binding.maximum_run_bytes_billed):
        raise ValidationError('remaining aggregate allowance cannot reserve all four whole-trial caps')
    if not isinstance(plan['authorization_basis'], str) or not plan['authorization_basis'].strip():
        raise ValidationError('record the operator allocation of remaining authorization')
    if type(plan['threads']) is not int or not 1 <= plan['threads'] <= 4 or plan['transport'] != TRANSPORT:
        raise ValidationError('fixed threads and registered REST/Arrow transport are required')
    for field in PIN_FIELDS:
        if not isinstance(plan[field], str) or not re.fullmatch(r'[a-f0-9]{40}' if field == 'git_sha' else r'[a-f0-9]{64}', plan[field]):
            raise ValidationError('invalid immutable suite pin: ' + field)
    root = _path(plan['output_directory'])
    trials = plan['trials']
    if not isinstance(trials, list) or len(trials) != 4:
        raise ValidationError('suite requires one warmup pair and three measured pairs')
    names, prior_order = set(), None
    for index, trial in enumerate(trials):
        if not isinstance(trial, dict) or set(trial) != TRIAL_FIELDS:
            raise ValidationError('unsupported trial plan fields')
        name = trial['case_id']
        if not isinstance(name, str) or not re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,63}', name) or name in names:
            raise ValidationError('trial case IDs must be distinct safe identifiers')
        names.add(name)
        if trial['role'] != ('warmup' if index == 0 else 'measured') or type(trial['repetition']) is not int or trial['repetition'] != index:
            raise ValidationError('trial order must be warmup0, measured1, measured2, measured3')
        order = trial['architecture_order']
        if order not in ('baseline-first', 'native-first') or order == prior_order:
            raise ValidationError('architecture order must alternate across serial paired trials')
        prior_order = order
        if _path(trial['directory']) != root / 'trials' / name:
            raise ValidationError('each trial output must be its unique planned suite/trials/case_id path')
        for prefix in ('baseline', 'native'):
            export = _path(trial[prefix + '_directory'])
            if export == root or export.is_relative_to(root) or root.is_relative_to(export):
                raise ValidationError('exports and fresh suite output must not overlap')
            if not re.fullmatch(r'[a-f0-9]{64}', trial[prefix + '_sha256'] or ''):
                raise ValidationError('every export needs an external manifest hash')
    return plan


def _runtime_identity(worker_python):
    """Inspect the exact local worker and complete dependency lock, without credentials."""
    script = '''import hashlib, importlib.metadata, json, platform
from pathlib import Path
from tl.bigquery.baseline_suite import execution_scopes
from tl.receipts.metrics import code_revision
lock=Path('baseline/bigquery/requirements-worker-lock.txt')
versions={}
for line in lock.read_text().splitlines():
 if not line.strip() or line.startswith('#'): continue
 name, expected=line.split('==',1)
 actual=importlib.metadata.version(name)
 if actual != expected: raise RuntimeError('worker lock mismatch: '+name)
 versions[name]=actual
scopes=execution_scopes()
revisions=[code_revision(scope['artifacts']) for scope in scopes.values()]
if any(not item['execution_artifacts_match_git'] for item in revisions) or len({item['git_sha'] for item in revisions}) != 1:
 raise RuntimeError('independently scoped execution artifacts differ from frozen revision')
result=revisions[0]
result.update(execution_scopes=scopes,worker_lock_sha256=hashlib.sha256(lock.read_bytes()).hexdigest(),versions=versions,python=platform.python_version())
print(json.dumps(result))
'''
    env = {**os.environ, 'PYTHONPATH': str(ROOT), 'PYTHONNOUSERSITE': '1',
           'DO_NOT_TRACK': '1', 'DBT_SEND_ANONYMOUS_USAGE_STATS': 'false'}
    checked = subprocess.run([str(worker_python), '-c', script], cwd=ROOT, env=env,
                             capture_output=True, text=True, check=False)
    if checked.returncode:
        raise ValidationError('pinned worker/code/dependency inspection failed before launch')
    result = json.loads(checked.stdout)
    result['worker_python_sha256'] = _sha(worker_python)
    if result.get('execution_artifacts_match_git') is not True:
        raise ValidationError('suite execution artifacts must match the frozen revision')
    return result


def preflight_plan(plan, worker_python, *, fresh=True):
    worker_python = _path(worker_python)
    if _sha(worker_python) != plan['worker_python_sha256']:
        raise ValidationError('worker interpreter differs from the plan')
    runtime = _runtime_identity(worker_python)
    if not _runtime_matches(runtime, plan):
        raise ValidationError('code, revision, or worker runtime differs from the suite plan')
    binding = Binding(**plan['binding'])
    if fresh and _path(plan['output_directory']).exists():
        raise ValidationError('existing, partial, or completed suites cannot resume or retry')
    identity, datasets, outputs = None, set(), None
    with chdir(ROOT):
        for trial in plan['trials']:
            base, native = preflight(binding, trial['baseline_directory'], trial['native_directory'],
                baseline_sha256=trial['baseline_sha256'], native_sha256=trial['native_sha256'])
            # Native exports also get an exact inventory check, including links.
            native_root = _path(trial['native_directory'])
            expected = {'manifest.json', *native['files']}
            expected_dirs = {str(Path(name).parent).replace('\\', '/') for name in expected}
            expected_dirs.discard('.')
            actual = set()
            for path in native_root.rglob('*'):
                _no_reparse(path)
                name = path.relative_to(native_root).as_posix()
                if path.is_file(): actual.add(name)
                elif name not in expected_dirs: raise ValidationError('unlisted native export directory')
            if actual != expected:
                raise ValidationError('native export has missing or unlisted files')
            comparison = plan['execution_scopes']['comparison']
            if (native['git_sha'] != plan['git_sha'] or base['git_sha'] != plan['git_sha']
                    or native['execution_hash'] != comparison['execution_hash']
                    or native['execution_artifacts'] != comparison['artifacts']):
                raise ValidationError('exports do not use the suite execution revision')
            common = dict(inputs=native['inputs'], cutoffs=base['cutoffs'], definition=base['definition'],
                          source=base['source'], outputs=native['outputs'])
            if identity is None: identity, outputs = common, native['outputs']
            elif common != identity: raise ValidationError('paired repetitions must share identical frozen source, cutoffs, definitions and outputs')
            for key in ('raw_dataset', 'model_dataset'):
                name = base['derived'][key]
                if name in datasets: raise ValidationError('each trial needs fresh, unique derived datasets')
                datasets.add(name)
            if fresh and _path(trial['directory']).exists():
                raise ValidationError('existing trial output cannot be retried')
    return dict(runtime=runtime, identity=identity, outputs=outputs)


def _append(path, event):
    with path.open('a', encoding='utf-8', newline='\n') as out:
        out.write(canonical(event) + '\n'); out.flush(); os.fsync(out.fileno())


def _launch(plan, trial, root, worker_python):
    command = [str(worker_python), '-m', 'tl', 'cloud', 'bigquery', '--config', str(root / 'binding.json'),
        'baseline-run', '--baseline-export', trial['baseline_directory'], '--native-export', trial['native_directory'],
        '--baseline-sha256', trial['baseline_sha256'], '--native-sha256', trial['native_sha256'],
        '--worker-python', str(worker_python), '--threads', str(plan['threads']),
        '--architecture-order', trial['architecture_order'], '--output', trial['directory'], '--json']
    env = {**os.environ, 'PYTHONPATH': str(ROOT), 'PYTHONNOUSERSITE': '1',
           'DO_NOT_TRACK': '1', 'DBT_SEND_ANONYMOUS_USAGE_STATS': 'false'}
    with (root / (trial['case_id'] + '.log')).open('x', encoding='utf-8') as log:
        return subprocess.run(command, cwd=ROOT, env=env, stdout=log,
                              stderr=subprocess.STDOUT, check=False).returncode


def _trial_observation(root, plan, trial):
    """Read sealed successes and failures alike; no cloud job is re-performed."""
    directory = root / 'trials' / trial['case_id'] / 'observation'
    receipt = json.loads((directory / 'receipt.json').read_bytes())
    validation = verify(receipt['receipt_id'], directory)
    if validation['evidence_status'] != 'sealed': raise ValidationError('trial evidence is incomplete')
    report = json.loads((directory / 'result.json').read_bytes())
    execution = json.loads((directory / 'execution.json').read_bytes())
    if not isinstance(report, dict) or not isinstance(report.get('jobs', {}), dict):
        raise ValidationError('malformed sealed trial result')
    observed = dict(case_id=trial['case_id'], role=trial['role'], repetition=trial['repetition'],
        receipt_id=receipt['receipt_id'], observation_run_id=receipt['run_id'], report=report,
        plan_admission='rejected', admission_error=None)
    try:
        _admit_observation(report, receipt, execution, plan, trial)
        observed['plan_admission'] = 'admitted'
    except (ValidationError, KeyError, TypeError) as exc:
        # A seal establishes retained bytes. A failed plan check must keep those
        # job/cost observations visible without admitting a timing population.
        observed['admission_error'] = str(exc)
    return observed


def _admit_observation(report, receipt, execution, plan, trial):
    scope = plan['execution_scopes']['boundary_observation']
    if (report['binding_identity'] != Binding(**plan['binding']).identity
            or receipt['binding_identity'] != report['binding_identity'] or receipt['inputs'] != report['inputs']
            or report['baseline_manifest_sha256'] != trial['baseline_sha256']
            or report['native_manifest_sha256'] != trial['native_sha256']
            or report.get('architecture_order') != trial['architecture_order']
            or report['threads'] != plan['threads']
            or report['worker_lock_sha256'] != plan['worker_lock_sha256']
            or execution['git_sha'] != plan['git_sha'] or execution['execution_hash'] != scope['execution_hash']
            or execution['artifacts'] != scope['artifacts']):
        raise ValidationError('trial result differs from the frozen paired plan or did not complete')
    _run_id(report['run_id']); _run_id(receipt['run_id'])
    if report['run_id'] == receipt['run_id']:
        raise ValidationError('trial and boundary observation must have distinct run identities')


def _retained_trial(root, plan, trial, observed=None):
    """Admit only complete equivalent trials to the measured population."""
    observed = observed or _trial_observation(root, plan, trial)
    if observed['plan_admission'] != 'admitted':
        raise ValidationError('sealed trial rejected by immutable plan: ' + observed['admission_error'])
    report = observed['report']
    if (report['status'] != 'equivalent' or report.get('dbt_success') is not True
            or report.get('completed_architectures') != (['baseline', 'native']
                if trial['architecture_order'] == 'baseline-first' else ['native', 'baseline'])):
        raise ValidationError('trial did not complete both registered architectures')
    jobs = report['jobs']
    if (jobs.get('run_id') != report['run_id'] or jobs.get('project') != plan['binding']['project']
            or jobs.get('location') != plan['binding']['location']):
        raise ValidationError('job evidence belongs to a different run or binding')
    if jobs.get('rejected_observations') or jobs.get('rejected_job_count', 0):
        raise ValidationError('trial contains rejected job identity/configuration evidence')
    proof = root / 'trials' / trial['case_id'] / 'observation/proof'
    if json.loads((proof / 'trial.json').read_bytes()) != report:
        raise ValidationError('sealed trial result differs from its retained runner report')
    reserved_ids = set()
    job_rows = [json.loads(line) for line in (proof / 'jobs.jsonl').read_text().splitlines()]
    header = job_rows[0] if job_rows else {}
    if (header.get('event') != 'boundary' or header.get('run_id') != report['run_id']
            or header.get('binding_identity') != report['binding_identity']):
        raise ValidationError('job reservation ledger differs from its trial run or binding')
    for row in job_rows:
        if row.get('event') == 'reserved' and row['configuration'].get('dryRun') is not True:
            if row['job_id'] in reserved_ids: raise ValidationError('duplicate actual job reservation')
            reserved_ids.add(row['job_id'])
    architecture_jobs = report.get('architecture_jobs', {})
    if set(architecture_jobs) != {'baseline', 'native'} or any(
        not isinstance(ids, list) or not ids or len(set(ids)) != len(ids)
        for ids in architecture_jobs.values()):
        raise ValidationError('architecture job populations must be explicit and distinct')
    baseline_ids, native_ids = (set(architecture_jobs[key]) for key in ('baseline', 'native'))
    if baseline_ids & native_ids or baseline_ids | native_ids != reserved_ids:
        raise ValidationError('architecture job sets do not partition every actual reservation')
    if (_sha(proof / 'baseline-export/manifest.json') != trial['baseline_sha256']
            or _sha(proof / 'native-export/manifest.json') != trial['native_sha256']):
        raise ValidationError('retained export bytes differ from external suite pins')
    baseline = json.loads((proof / 'baseline-export/manifest.json').read_bytes())
    native = json.loads((proof / 'native-export/manifest.json').read_bytes())
    scope = plan['execution_scopes']['comparison']
    if (report['inputs'] != native['inputs'] or report['cutoffs'] != baseline['cutoffs']
            or report['definition'] != baseline['definition']
            or native['git_sha'] != plan['git_sha'] or baseline['git_sha'] != plan['git_sha']
            or native['execution_hash'] != scope['execution_hash']
            or native['execution_artifacts'] != scope['artifacts']):
        raise ValidationError('retained result source, cutoffs or definition differs from its export')
    if len(report['outputs']) != 13 or set(report['outputs']) != set(native['outputs']):
        raise ValidationError('trial must retain all thirteen registered outputs')
    records = read_receipts(proof / 'metrics.jsonl')
    native_run_id = _run_id(report['native']['run_id'])
    run_directory = report['native']['run_directory']
    if not isinstance(run_directory, str) or Path(run_directory.replace('\\', '/')).name != native_run_id:
        raise ValidationError('native run path differs from its explicit run identity')
    native_root = proof / 'native-runs' / native_run_id
    native_run = json.loads((native_root / 'run.json').read_bytes())
    if (native_run['schema_version'] != 'tokenledger-bigquery-run/v1'
            or native_run['run_id'] != native_run_id or native_run['binding_identity'] != report['binding_identity']
            or native_run['export_sha256'] != digest(native) or native_run['status'] != 'recorded'
            or report['native']['status'] != 'recorded'
            or _sha(native_root / 'export/manifest.json') != trial['native_sha256']):
        raise ValidationError('native run binding, export or status differs')
    native_results = native_run['outputs']
    if set(native_results) != set(native['outputs']):
        raise ValidationError('native run is missing registered output receipts')
    common = dict(inputs=report['inputs'], git_sha=plan['git_sha'], execution_hash=scope['execution_hash'],
        binding_identity=report['binding_identity'], asof=native['asof'], known_at=native['known_at'],
        watermark=native['watermark'])
    definition_version = 'accounting/v1;nrr/' + native['definition_version']
    native_observations = [row for row in records.values()
        if row.get('schema_version') == 'tokenledger-bigquery-observation/v1']
    if len(native_observations) != 1:
        raise ValidationError('native run requires exactly one manifest observation receipt')
    native_observation = native_observations[0]
    if (any(native_observation.get(key) != value for key, value in common.items())
            or native_observation['run_id'] != native_run_id
            or native_observation['result']['manifest_sha256'] != digest(native_run)
            or native_observation['result']['jobs'] != native_run['jobs']
            or native_observation['query_hash'] != digest(native['files'])
            or native_observation['definition_version'] != 'bigquery/v1'):
        raise ValidationError('native run observation differs from its run, export or execution scope')
    selected_ids = [observed['receipt_id'], native_observation['receipt_id']]
    native_ids = [native_observation['receipt_id']]
    for name, output in report['outputs'].items():
        metric = records.get(output['receipt_id'])
        if (metric is None or metric['schema_version'] != 'tokenledger-bigquery-baseline/v1'
                or metric['result']['output'] != name or metric['result']['comparison'] != output['comparison']
                or output['comparison']['status'] != 'equivalent'
                or any(metric.get(key) != value for key, value in common.items())
                or metric['run_id'] != report['run_id'] or metric['definition_version'] != definition_version
                or metric['baseline_export_sha256'] != trial['baseline_sha256']
                or metric['query_hash'] != baseline['files']['sql/models/' + name + '.sql']['sha256']
                or not (proof / (name + '-baseline.parquet')).is_file()):
            raise ValidationError('missing or inconsistent receipt-backed equivalent output')
        native_metric = records.get(native_results[name]['receipt_id'])
        if (native_metric is None or native_metric['schema_version'] != 'tokenledger-bigquery/v1'
                or native_metric['result']['output'] != name
                or any(native_metric.get(key) != value for key, value in common.items())
                or native_metric['run_id'] != native_run_id or native_metric['definition_version'] != definition_version
                or native_metric['query_hash'] != native['files']['queries/' + name + '.sql']
                or _sha(native_root / ('export/queries/' + name + '.sql')) != native_metric['query_hash']
                or not (native_root / (name + '.parquet')).is_file()):
            raise ValidationError('missing or inconsistent native output receipt')
        selected_ids.extend([metric['receipt_id'], native_metric['receipt_id']])
        native_ids.append(native_metric['receipt_id'])
    run_ids = [report['run_id'], native_run_id, observed['observation_run_id']]
    if len(set(run_ids)) != 3 or len(set(selected_ids)) != 28:
        raise ValidationError('run and population receipt identities must be distinct')
    if set(records) != set(selected_ids) - {observed['receipt_id']}:
        raise ValidationError('trial metric ledger differs from its registered receipt population')
    native_ledger = read_receipts(native_root / 'receipts.jsonl')
    if (native_ledger != {key: records[key] for key in native_ids}
            or len(report['native']['receipt_ids']) != 14 or set(report['native']['receipt_ids']) != set(native_ids)):
        raise ValidationError('native run receipt ledger or returned IDs differ from retained population')
    observed['identities'] = dict(run_ids=run_ids, receipt_ids=selected_ids)
    return observed


def _distinct_trial_identities(retained, prior):
    for kind, values in retained['identities'].items():
        if any(set(values) & set(item['identities'][kind]) for item in prior):
            raise ValidationError('paired repetitions must have globally distinct run and population receipt identities')


def _control_evidence(root, plan, plan_sha256, observations):
    """Validate the durable suite allowance and driver outcomes, including interruption."""
    result = dict(status='incomplete', reserved_bytes=None, unknown_reservations=[], errors=[],
                  allowance_bytes=plan['aggregate_maximum_bytes_billed'], outcomes=[])
    try:
        admission = json.loads((root / 'admission.json').read_bytes())
        if not _runtime_matches(admission['runtime'], plan):
            raise ValidationError('admission pins differ from immutable plan')
        for observed in observations:
            if observed['plan_admission'] != 'admitted':
                raise ValidationError('sealed trial was not admitted by the frozen plan')
            report = observed['report']
            if (report['inputs'] != admission['identity']['inputs']
                    or report['cutoffs'] != admission['identity']['cutoffs']
                    or report['definition'] != admission['identity']['definition']):
                raise ValidationError('trial source/definition differs from suite admission')
    except (OSError, ValueError, KeyError, TypeError, ValidationError) as exc:
        result['errors'].append('admission: ' + str(exc))
    try:
        raw = (root / 'reservations.jsonl').read_bytes()
        if not raw or not raw.endswith(b'\n'): raise ValidationError('reservation ledger is incomplete')
        rows = [json.loads(line, object_pairs_hook=_unique) for line in raw.splitlines()]
        expected_header = dict(event='suite_bound', plan_sha256=plan_sha256, suite_id=plan['suite_id'],
            aggregate_maximum_bytes_billed=plan['aggregate_maximum_bytes_billed'],
            authorization_basis=plan['authorization_basis'])
        if rows[0] != expected_header: raise ValidationError('reservation header differs from plan')
        total, index, pending, stopped = 0, 0, None, False
        outcomes, reservations = [], []
        for row in rows[1:]:
            if stopped or index >= len(plan['trials']): raise ValidationError('ledger continued after a stopped suite')
            trial = plan['trials'][index]
            if row.get('case_id') != trial['case_id']: raise ValidationError('reservation trial order differs')
            if row.get('event') == 'trial_reserved':
                cap = plan['binding']['maximum_run_bytes_billed']
                if (pending is not None or row.get('plan_sha256') != plan_sha256
                        or row.get('maximum_run_bytes_billed') != cap
                        or row.get('aggregate_reserved_bytes') != total + cap
                        or total + cap > plan['aggregate_maximum_bytes_billed']):
                    raise ValidationError('invalid whole-trial reservation')
                total += cap; pending = trial['case_id']; reservations.append(pending)
            elif row.get('event') == 'trial_finished':
                cap = plan['binding']['maximum_run_bytes_billed'] if pending else 0
                if row.get('reserved_bytes') != cap or row.get('reservation_retained') is not True:
                    raise ValidationError('finished trial refunded or changed its reservation')
                status = row.get('status')
                if status not in ('equivalent', 'failed_or_unknown') or (status == 'equivalent' and pending is None):
                    raise ValidationError('invalid driver outcome')
                outcome = {key: value for key, value in row.items()
                           if key not in ('event', 'reserved_bytes', 'reservation_retained')}
                outcomes.append(outcome)
                if status == 'equivalent':
                    matching = [item for item in observations if item['case_id'] == trial['case_id']]
                    if len(matching) != 1 or matching[0]['receipt_id'] != row.get('receipt_id'):
                        raise ValidationError('driver outcome differs from retained trial receipt')
                stopped = status != 'equivalent'; pending = None; index += 1
            else: raise ValidationError('unexpected suite reservation event')
        result.update(reserved_bytes=total, reserved_cases=reservations, outcomes=outcomes,
                      unknown_reservations=[pending] if pending else [])
        if pending: result['errors'].append('interrupted launch: whole reservation remains unknown and held')
        if index != 4: result['errors'].append('suite did not complete four driver outcomes')
        execution = json.loads((root / 'execution.json').read_bytes())
        if execution != dict(outcomes=outcomes, reserved_bytes=total,
                allowance_bytes=plan['aggregate_maximum_bytes_billed'], refunds=0, automatic_retries=0):
            raise ValidationError('execution summary differs from durable reservation outcomes')
    except (OSError, ValueError, KeyError, TypeError, ValidationError) as exc:
        result['errors'].append('reservations/execution: ' + str(exc))
    if not result['errors']: result['status'] = 'verified'
    return result


def _sum_known(values):
    return sum(values) if values and all(value is not None for value in values) else None


def _numbers(trial):
    report = trial['report']
    values = {}
    for architecture in ('baseline', 'native', 'shared'):
        item = report['measurements'].get('accounted_wall', {}).get(architecture, {})
        values['accounted_wall_seconds/' + architecture] = item.get('wall_seconds') if item.get('status') == 'complete' else None
    for name, stage in report['measurements'].get('stages', {}).items():
        value = stage.get('wall_seconds')
        values['wall_seconds/' + name] = value if stage.get('status') == 'complete' else None
    for name in ('native_harness_remainder', 'end_to_end'):
        item = report['measurements'].get(name, {})
        values['wall_seconds/' + name] = item.get('wall_seconds')
    observations = {row['job_id']: row for row in report['jobs']['observations'] if row is not None and not row.get('dry_run')}
    for architecture in ('baseline', 'native'):
        identifiers = report.get('architecture_jobs', {}).get(architecture, [])
        rows = [observations.get(key) for key in identifiers]
        for field in ('total_bytes_billed', 'slot_millis'):
            values[architecture + '/' + field] = _sum_known([row.get(field) if row else None for row in rows])
        elapsed = []
        for row in rows:
            stats = row.get('statistics', {}) if row else {}
            start, end = stats.get('startTime'), stats.get('endTime')
            elapsed.append(None if start is None or end is None else (float(end) - float(start)) / 1000)
        values[architecture + '/server_elapsed_seconds_sum'] = _sum_known(elapsed)
        values[architecture + '/unknown_job_statistics'] = sum(
            row is None or row.get('total_bytes_billed') is None or row.get('slot_millis') is None
            or duration is None for row, duration in zip(rows, elapsed)) if identifiers else None
    if any(value is not None and (type(value) not in (int, float) or not math.isfinite(value) or value < 0)
           for value in values.values()):
        raise ValidationError('invalid retained timing/statistics value')
    return values


def report(directory, *, plan_sha256, output=None):
    """Create an offline observation from complete retained trial receipts only."""
    root = _path(directory)
    plan = load_plan(root / 'plan.json', plan_sha256)
    trials, observations, failures = [], [], []
    for trial in plan['trials']:
        try:
            observed = _trial_observation(root, plan, trial)
            observations.append(observed)
            retained = _retained_trial(root, plan, trial, observed)
            _distinct_trial_identities(retained, trials)
            trials.append(retained)
        except (ValidationError, OSError, KeyError, ValueError, TypeError) as exc:
            failures.append(dict(case_id=trial['case_id'], status='missing_failed_or_invalid', reason=str(exc)))
    controls = _control_evidence(root, plan, plan_sha256, observations)
    measured = [trial for trial in trials if trial['role'] == 'measured']
    complete = not failures and controls['status'] == 'verified' and len(measured) == 3 and len(trials) == 4
    statistics_report = {}
    if complete:
        values = [_numbers(trial) for trial in measured]
        for key in sorted(set.union(*(set(row) for row in values))):
            population = [row.get(key) for row in values]
            statistics_report[key] = (dict(status='measured', median=statistics.median(population),
                minimum=min(population), maximum=max(population), values=population)
                if all(value is not None for value in population) else dict(status='unknown', values=population))
    result = dict(schema_version='tokenledger-bigquery-matched-report/v2',
        status='measured' if complete else 'incomplete', suite_id=plan['suite_id'], plan_sha256=plan_sha256,
        binding_identity=Binding(**plan['binding']).identity, matched_performance='measured' if complete else 'not_run',
        scope='one warmup and three serial paired fresh full builds; incremental performance not_run',
        cloud_recomputed=False, recomputed=False, no_cloud_jobs_submitted=True,
        server_elapsed_scope='sum of individual job end-start intervals; concurrent jobs overlap, not wall time',
        query_wall_scope='includes dry-run estimate, server wait and REST/Arrow transfer; not server-only',
        stage_wall_scope='inclusive stages and their nested child clocks overlap; their rows must not be added together',
        stage_scopes={name: value.get('scope') for name, value in
                      (trials[0]['report']['measurements'].get('stages', {}) if trials else {}).items()},
        native_harness_scope='inclusive native wall minus nested query-call wall; hashing, serialization and other Python overhead combined',
        accounted_wall_scope='sums of disjoint work intervals; not contiguous end-to-end elapsed time',
        inclusive_wall_scope='raw baseline/native stage clocks are diagnostics with different work scopes',
        suite_controls=controls,
        warmup=[dict(case_id=x['case_id'], receipt_id=x['receipt_id'], observations=_numbers(x))
                for x in trials if x['role'] == 'warmup'],
        measured_receipts=[x['receipt_id'] for x in measured], statistics=statistics_report, failures=failures,
        trial_job_status=[dict(case_id=x['case_id'], role=x['role'], receipt_id=x['receipt_id'],
            observed_status=x['report'].get('status'), plan_admission=x['plan_admission'],
            admission_error=x['admission_error'],
            actual_job_count=x['report'].get('jobs', {}).get('actual_job_count'),
            observed_billed_bytes=x['report'].get('jobs', {}).get('observed_billed_bytes'),
            reserved_bytes=x['report'].get('jobs', {}).get('reserved_bytes'),
            unknown_or_incomplete_jobs=x['report'].get('jobs', {}).get('unknown_or_incomplete_jobs'),
            rejected_job_count=x['report'].get('jobs', {}).get('rejected_job_count'),
            complete_billed_total=x['report'].get('jobs', {}).get('complete_billed_total')) for x in observations],
        production_acceptance='not_established')
    if output is None: return result
    target = _path(output)
    if target.exists(): raise ValidationError('suite report output must be fresh')
    target.mkdir(parents=True)
    _write(target / 'report.json', result)
    proof = {name: root / name for name in ('plan.json', 'reservations.jsonl', 'execution.json', 'admission.json')}
    proof['report.json'] = target / 'report.json'
    for trial in observations:
        proof[trial['case_id'] + '-receipt.json'] = root / 'trials' / trial['case_id'] / 'observation/receipt.json'
        proof[trial['case_id'] + '-result.json'] = root / 'trials' / trial['case_id'] / 'observation/result.json'
    for trial in plan['trials']:
        # Explicit names only; never enumerate a private credential/source folder.
        log = _path(root / (trial['case_id'] + '.log'))
        if log.is_file(): proof['logs/' + log.name] = log
    observation = record(result, target / 'observation', binding=Binding(**plan['binding']),
                         artifacts=proof, ledger=target / 'receipts.jsonl')
    lines = ['# Matched native BigQuery full-build observations', '',
             'Status: ' + result['status'] + '. Receipt: `' + observation['receipt_id'] + '`.', '',
             'One warmup pair is excluded from the three measured pairs. Individual server intervals overlap under concurrency.', '',
             'Accounted wall sums disjoint measured work intervals; it is not contiguous end-to-end elapsed time. '
             'Raw inclusive stage clocks below are diagnostics with different work scopes.', '',
             '| Observation | Median | Minimum | Maximum |', '|---|---:|---:|---:|']
    for name in sorted(statistics_report, key=lambda key: (not key.startswith('accounted_wall_seconds/'), key)):
        value = statistics_report[name]
        lines.append('| ' + name + ' | ' + ' | '.join(str(value.get(k, 'unknown')) for k in ('median', 'minimum', 'maximum')) + ' |')
    lines += ['', 'Trial receipts: ' + ', '.join('`' + key + '`' for key in result['measured_receipts']), '',
              'Offline evidence verification only. Incremental performance and production acceptance are not established.', '']
    (target / 'report.md').write_text('\n'.join(lines), encoding='utf-8')
    return dict(**result, observation=observation, directory=str(target))


def run(plan_path, *, plan_sha256, output, worker_python):
    plan = load_plan(plan_path, plan_sha256)
    root = _path(output)
    if root != _path(plan['output_directory']): raise ValidationError('output differs from the immutable plan')
    admission = preflight_plan(plan, worker_python)
    root.mkdir(parents=True, exist_ok=False)
    (root / 'plan.json').write_bytes(Path(plan_path).read_bytes())
    _write(root / 'binding.json', plan['binding'])
    _write(root / 'admission.json', admission)
    ledger, reserved, outcomes, retained_trials = root / 'reservations.jsonl', 0, [], []
    with ledger_lock(ledger):
        _append(ledger, dict(event='suite_bound', plan_sha256=plan_sha256, suite_id=plan['suite_id'],
            aggregate_maximum_bytes_billed=plan['aggregate_maximum_bytes_billed'],
            authorization_basis=plan['authorization_basis']))
        for trial in plan['trials']:
            # Recheck all immutable exports/runtime between serial trials; a
            # changed candidate never consumes another worker's cloud allowance.
            cap = plan['binding']['maximum_run_bytes_billed']
            trial_reservation = 0
            try:
                preflight_plan(plan, worker_python, fresh=False)
                if _path(trial['directory']).exists():
                    raise ValidationError('existing or partial trial cannot be relaunched')
                if reserved + cap > plan['aggregate_maximum_bytes_billed']:
                    raise ValidationError('suite remaining allowance exhausted before launch')
                reserved += cap
                trial_reservation = cap
                _append(ledger, dict(event='trial_reserved', case_id=trial['case_id'],
                    maximum_run_bytes_billed=cap, aggregate_reserved_bytes=reserved,
                    plan_sha256=plan_sha256, recorded_at=datetime.now(timezone.utc).isoformat()))
                code = _launch(plan, trial, root, Path(worker_python))
                if code != 0: raise ValidationError('trial worker exited unsuccessfully')
                retained = _retained_trial(root, plan, trial)
                _distinct_trial_identities(retained, retained_trials)
                retained_trials.append(retained)
                outcome = dict(case_id=trial['case_id'], status='equivalent', receipt_id=retained['receipt_id'])
            except BaseException as exc:
                outcome = dict(case_id=trial['case_id'], status='failed_or_unknown',
                               error_type=type(exc).__name__, reason=str(exc))
            outcomes.append(outcome)
            _append(ledger, dict(event='trial_finished', reserved_bytes=trial_reservation,
                                reservation_retained=True, **outcome))
            if outcome['status'] != 'equivalent': break
    _write(root / 'execution.json', dict(outcomes=outcomes, reserved_bytes=reserved,
        allowance_bytes=plan['aggregate_maximum_bytes_billed'], refunds=0, automatic_retries=0))
    return report(root, plan_sha256=plan_sha256, output=root / 'report')
