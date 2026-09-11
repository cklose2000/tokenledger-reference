"""One signed, bounded diagnostic trial. Never collects or changes usage data."""
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
import json
import re
import subprocess
import uuid

from tl.learning import evaluation
from tl.learning.candidate import diagnose, VERSION
from tl.metrics.engine import read_run, replay_receipts, runtime
from tl.receipts.journal import ledger_lock
from tl.receipts.metrics import append_receipts, code_revision, digest, input_manifest, read_receipts, receipt_id
from tl.reporting.application import private_path
from tl.stream import ValidationError
from tl.stream.events import canonical, iso, timestamp
from tl.usage.importer import events_for

SCHEMA = 'tokenledger-learning-trial/v1'
OBSERVATION = 'tokenledger-learning-trial-observation/v1'
AUTHORIZATION = 'tokenledger-learning-authorization/v1'
NAMESPACE = 'tokenledger-learning-trial'
PRINCIPAL = 'chandler'


def _write(path, value):
    with Path(path).open('x', encoding='utf-8', newline='\n') as handle:
        handle.write(canonical(value) + '\n')


def _json(path):
    try:
        return json.loads(Path(path).read_bytes())
    except (OSError, ValueError):
        raise ValidationError('learning evidence unavailable or invalid') from None


def _directory(app, directory):
    directory = private_path(directory)
    if not directory.is_relative_to(app.path('evidence/learning-trials')):
        raise ValidationError('trial must belong to the bound private application')
    return directory


def _trust(app):
    return app.path('evidence/learning-authority/allowed_signers')


def _signature(plan, signature, trust):
    from tl.learning.signatures import verify
    verify(plan, signature, trust)


def _report(app, run_id, *, reperform=True):
    original = read_run(run_id, output_root=app.outputs, ledger=app.ledger, application=app)
    if reperform:
        answers = replay_receipts([row['receipt_id'] for row in original['rows']],
            db=app.db, output_root=app.outputs, ledger=app.ledger, application=app)
        if not all(a.get('verified') and a.get('recomputed', True) for a in answers):
            raise ValidationError('original report was not freshly reproduced')
    return original


def _evaluation(app, key, directory, study_hash):
    records = read_receipts(app.ledger)
    record = records.get(key, {})
    if record.get('schema_version') != evaluation.SCHEMA or record['result']['status'] != 'passed':
        raise ValidationError('a passed original evaluation receipt is required')
    root = app.outputs / record['run_id']
    manifest = _json(root / 'run.json')
    if (manifest['study_sha256'] != study_hash
            or Path(manifest['study_directory']).resolve() != directory.resolve()
            or manifest['candidate_sha256'] != evaluation.candidate_identity()):
        raise ValidationError('evaluation, study and diagnostic identities differ')
    answers = replay_receipts([key], db=app.db, output_root=app.outputs,
        ledger=app.ledger, application=app)
    if not all(a['verified'] and a['recomputed'] for a in answers):
        raise ValidationError('original evaluation did not reproduce')
    return record


def prepare(app, directory, *, evaluation_receipt, expires_at):
    """Create exact bytes for human review; this does not authorize a trial."""
    directory, frozen = evaluation.study(app, directory)
    now = datetime.now(timezone.utc)
    expires = timestamp(expires_at)
    if not now < expires or (expires - now).days > 30:
        raise ValidationError('trial expiry must be in the next 30 days')
    pins = evaluation.artifacts(app)
    study_hash = evaluation.file_hash(directory / 'study.json')
    record = _evaluation(app, evaluation_receipt, directory, study_hash)
    prior = _report(app, frozen['prior_run_id'])
    packet = _json(directory / 'packet.json')
    with app.session(asof=now.date().isoformat(), known_at=iso(now)) as session:
        session.verify()
        baseline = dict(asof=session.asof.isoformat(), known_at=iso(session.known_at),
            watermark=session.watermark, inputs=input_manifest(session.snapshot))
    trust = _trust(app)
    plan = dict(schema_version='tokenledger-learning-plan/v1', trial_id=uuid.uuid4().hex,
        application=app.name, binding_id=app.binding.identity,
        prepared_at=iso(now), expires_at=iso(expires), study_directory=str(directory),
        study_sha256=study_hash, evaluation_receipt_id=evaluation_receipt,
        evaluation_receipt_sha256=digest(record), candidate_sha256=evaluation.candidate_identity(),
        execution_artifacts=pins, execution_hash=digest(pins), runtime=runtime(),
        original_report_run_id=prior['run_id'], original_report_sha256=evaluation.file_hash(
            app.outputs / prior['run_id'] / 'run.json'), baseline=baseline,
        source_scope={k:packet[k] for k in ('account_alias', 'source', 'client_version')},
        reviewer_principal=PRINCIPAL, signature_namespace=NAMESPACE,
        trusted_signers_sha256=evaluation.file_hash(trust) if trust.is_file() else None,
        limits=dict(max_packets=1, collection=False, stream_writes=False, policy_changes=False,
            privacy_changes=False, acceptance_changes=False, automatic_followup=False),
        collection_boundary='first eligible admitted packet collected after explicit signed-plan authorization',
        acceptance=dict(false_positives=0, missed_findings=0, incorrect_fields=0,
            unlocated_findings=0, no_new_discrepancy='inconclusive', regression='rolled_back'),
        **code_revision(pins))
    if evaluation.artifacts(app) != pins:
        raise ValidationError('implementation changed during trial preparation')
    root = app.path('evidence/learning-trials/' + plan['trial_id'])
    root.mkdir(parents=True)
    _write(root / 'plan.json', plan)
    return dict(status='prepared_not_approved', trial_directory=str(root),
        plan_sha256=evaluation.file_hash(root / 'plan.json'),
        candidate_sha256=plan['candidate_sha256'], wrapper_sha256=plan['execution_hash'],
        approval='signature_required' if trust.is_file() else 'reviewer_trust_unconfigured',
        promotion=False)


def _plan(app, root, *, live=True):
    app.validate()
    root = _directory(app, root)
    plan = _json(root / 'plan.json')
    if (plan.get('schema_version') != 'tokenledger-learning-plan/v1'
            or plan.get('binding_id') != app.binding.identity or plan.get('application') != app.name
            or plan.get('reviewer_principal') != PRINCIPAL or plan.get('signature_namespace') != NAMESPACE):
        raise ValidationError('trial plan application or approval scope differs')
    if plan['candidate_sha256'] != evaluation.candidate_identity():
        raise ValidationError('reviewed diagnostic changed; new approval required')
    if plan['execution_artifacts'] != evaluation.artifacts(app) or plan['runtime'] != runtime():
        raise ValidationError('reviewed wrapper or runtime changed; new approval required')
    trust = _trust(app) if live else root / 'approved_signers'
    if not plan['trusted_signers_sha256'] or not trust.is_file():
        raise ValidationError('Promotion unavailable: reviewer trust was not configured for this plan')
    if evaluation.file_hash(trust) != plan['trusted_signers_sha256']:
        raise ValidationError('reviewer trust changed; new approval required')
    _signature(root / 'plan.json', root / 'plan.json.sig', trust)
    if live and ((root / 'revoked.json').exists() or datetime.now(timezone.utc) >= timestamp(plan['expires_at'])):
        raise ValidationError('trial approval expired or revoked')
    directory, _ = evaluation.study(app, plan['study_directory'], plan['study_sha256'])
    record = _evaluation(app, plan['evaluation_receipt_id'], directory, plan['study_sha256'])
    if digest(record) != plan['evaluation_receipt_sha256']:
        raise ValidationError('original evaluation changed')
    if evaluation.file_hash(app.outputs / plan['original_report_run_id'] / 'run.json') != plan['original_report_sha256']:
        raise ValidationError('original report changed')
    return root, plan


def _authorization(app, root):
    """Resolve the observed activation through the shared receipt ledger."""
    if not (root/'authorization-receipt.json').is_file():
        raise ValidationError('Promotion unavailable: signed plan must be explicitly authorized before collection')
    pointer = _json(root / 'authorization-receipt.json')
    record = read_receipts(app.ledger).get(pointer.get('receipt_id'), {})
    if (record.get('schema_version') != AUTHORIZATION
            or record['result']['authorization_sha256'] != evaluation.file_hash(root / 'authorization.json')):
        raise ValidationError('signed plan has no valid observed authorization receipt')
    return _json(root / 'authorization.json'), record


def authorize(app, directory):
    """Observe approved activation before collection. Signature mtime is never authority."""
    root, plan = _plan(app, directory)
    with ledger_lock(root / 'execution'):
        if (root / 'authorization-receipt.json').exists():
            _, record = _authorization(app, root)
            replay_receipts([record['receipt_id']], db=app.db, output_root=app.outputs,
                ledger=app.ledger, application=app)
            return dict(status='authorized_one_run', receipt_id=record['receipt_id'],
                trial_directory=str(root), retry='verified_existing_authorization',
                availability='exhausted' if (root/'request.json').exists() else 'one_run_available')
        if (root / 'authorization.json').exists():
            raise ValidationError('interrupted authorization requires review and a new signed plan')
        now = datetime.now(timezone.utc)
        with app.session(asof=now.date().isoformat(), known_at=iso(now)) as session:
            session.verify()
            baseline = dict(asof=session.asof.isoformat(), known_at=iso(session.known_at),
                watermark=session.watermark, inputs=input_manifest(session.snapshot))
        if evaluation.artifacts(app) != plan['execution_artifacts']:
            raise ValidationError('reviewed wrapper changed during authorization')
        authorization = dict(schema_version=AUTHORIZATION, trial_id=plan['trial_id'],
            binding_id=app.binding.identity, authorized_at=iso(now), baseline=baseline,
            plan_sha256=evaluation.file_hash(root / 'plan.json'),
            signature_sha256=evaluation.file_hash(root / 'plan.json.sig'),
            authority='reviewer signature verified now; human signing timestamp is not inferred',
            state='one_run_only; collect and admit the first eligible packet after this observation')
        (root / 'approved_signers').write_bytes(_trust(app).read_bytes())
        _write(root / 'authorization.json', authorization)
        run_id=uuid.uuid4().hex; output=app.outputs/run_id; output.mkdir()
        manifest=dict(schema_version=AUTHORIZATION,run_id=run_id,application=app.name,
            binding_id=app.binding.identity,trial_directory=str(root),
            files={n:evaluation.file_hash(root/n) for n in ('plan.json','plan.json.sig','approved_signers','authorization.json')},
            execution_artifacts=plan['execution_artifacts'],execution_hash=plan['execution_hash'],
            runtime=runtime(),definition_version=VERSION,query_hash=evaluation.candidate_identity(),
            inputs=baseline['inputs'],asof=baseline['asof'],known_at=baseline['known_at'],watermark=baseline['watermark'],
            **code_revision(plan['execution_artifacts']))
        _write(output/'run.json',manifest)
        record=dict(schema_version=AUTHORIZATION,
            result=dict(status='authorized_one_run',authorization=authorization,
                authorization_sha256=evaluation.file_hash(root/'authorization.json')),
            manifest_sha256=evaluation.file_hash(output/'run.json'),
            **{k:manifest[k] for k in ('run_id','inputs','definition_version','query_hash','git_sha',
                'execution_hash','asof','known_at','watermark')})
        record['receipt_id']=receipt_id(record)
        append_receipts(app.ledger,[record])
        _write(root/'authorization-receipt.json',dict(receipt_id=record['receipt_id']))
        return dict(status='authorized_one_run',receipt_id=record['receipt_id'],
            trial_directory=str(root),collection='not_started_by_runner')


def _oracle(packet):
    """Scalar arithmetic independent of the frozen candidate's SQL."""
    truth = {}
    for row in packet['observations']:
        a = row['counters'].get('last_token_usage', {})
        total, incoming, outgoing = (a.get(k) for k in ('total_tokens', 'input_tokens', 'output_tokens'))
        if None in (total, incoming, outgoing) or total == incoming + outgoing:
            continue
        c = row['counters'].get('total_token_usage', {})
        values = [c.get(k) for k in ('total_tokens', 'input_tokens', 'output_tokens')]
        event = next(e for e in events_for(dict(packet, observations=[row]), 'oracle')
            if e.feature_json['temporality'] == 'interval_total' and e.feature_json['token_kind'] == 'total')
        truth[(row['session_id'], row['source_line'])] = dict(session_id=row['session_id'],
            source_line=row['source_line'], observed_at=row['ts'], reported_total=total,
            expected_total=incoming + outgoing, residual=total - incoming - outgoing,
            cumulative_status='incomplete' if None in values else 'reconciled' if values[0] == values[1] + values[2] else 'mismatch',
            activity_id=event.activity_id)
    return truth


def _compute(app, plan, request, authorization):
    if not re.fullmatch('[0-9a-f]{64}', request['packet_sha256']):
        raise ValidationError('invalid trial packet identity')
    path = app.path('evidence/imports/' + request['packet_sha256'] + '/packet.json')
    if not path.is_file() or evaluation.file_hash(path) != request['packet_sha256']:
        raise ValidationError('trial packet is not an unchanged retained import')
    packet = _json(path)
    if any(packet.get(k) != v for k, v in plan['source_scope'].items()):
        raise ValidationError('trial packet changed the approved source scope')
    if timestamp(packet['collected_at']) <= timestamp(authorization['authorized_at']):
        raise ValidationError('trial requires a genuinely new collection after signed-plan authorization')
    original = _report(app, plan['original_report_run_id'])
    following = _report(app, request['source_run_id'])
    reporting_window = following['manifest'].get('report_window')
    if not reporting_window or any(not (
            timestamp(reporting_window['start_utc']) <= timestamp(row['ts'])
            < timestamp(reporting_window['end_utc'])) for row in packet['observations']):
        raise ValidationError('following report must cover the complete approved packet window')
    baseline = authorization['baseline']
    with app.session(asof=baseline['asof'], known_at=baseline['known_at'], watermark=baseline['watermark']) as session:
        session.verify()
        if input_manifest(session.snapshot) != baseline['inputs']:
            raise ValidationError('pretrial source population changed')
        before = set(session.snapshot['activity_id'].to_pylist())
    m = following['manifest']
    with app.session(asof=m['asof'], known_at=m['known_at'], watermark=m['watermark'], **m['session_options']) as session:
        session.verify()
        inputs = input_manifest(session.snapshot)
        if inputs != m['inputs']:
            raise ValidationError('next source report snapshot changed')
        admitted = {r['activity_id']:r for r in session.snapshot.select(
            ['activity_id', 'activity', 'customer', 'ts', 'feature_json', '_stream_position', '_recorded_at']).to_pylist()}
        # Admission order is provenance, independent of business-event time.
        # A narrow report window cannot hide an earlier admitted packet with
        # later economic timestamps and select a more favorable subsequent one.
        # Diagnostic values still come exclusively from the shared snapshot.
        with app.reader().connect() as source:
            first=source.execute("""SELECT feature_json::VARCHAR FROM stream.activity
                WHERE activity='usage_observed' AND customer=?
                  AND _stream_position>? AND _stream_position<=?
                  AND _recorded_at<CAST(? AS TIMESTAMPTZ)
                  AND json_extract_string(feature_json,'$.source')=?
                ORDER BY _stream_position LIMIT 1""",[plan['source_scope']['account_alias'],
                    baseline['watermark'],m['watermark'],m['known_at'],plan['source_scope']['source']]).fetchone()
        if first and json.loads(first[0])['evidence_sha256']!=request['packet_sha256']:
            raise ValidationError('trial must use the first eligible postauthorization import, not a later selected packet')
        populations = {'overlap':set(), 'new':set()}
        for row in packet['observations']:
            keys = []
            for event in events_for(dict(packet, observations=[row]), request['packet_sha256']):
                observed = admitted.get(event.activity_id)
                semantic = lambda f:{k:v for k,v in f.items() if k != 'evidence_sha256'}
                if (observed is None or observed['customer'] != event.customer or iso(observed['ts']) != event.ts
                        or semantic(json.loads(observed['feature_json'])) != semantic(event.feature_json)):
                    raise ValidationError('trial packet differs from admitted report observations')
                keys.append(event.activity_id in before)
                if not keys[-1] and (observed['_stream_position'] <= baseline['watermark']
                        or observed['_recorded_at'] <= timestamp(authorization['authorized_at'])):
                    raise ValidationError('new trial observation predates the approved boundary')
            if len(set(keys)) != 1:
                raise ValidationError('partially overlapping source observation is outside this trial')
            populations['overlap' if keys[0] else 'new'].add((row['session_id'], row['source_line']))
    truth = _oracle(packet)
    findings = diagnose(packet)
    found = {(r['session_id'], r['source_line']):r for r in findings}
    if len(found) != len(findings):
        raise ValidationError('diagnostic returned duplicate findings')
    scores = {}
    for label, population in populations.items():
        wanted = population & truth.keys(); actual = population & found.keys()
        scores[label] = dict(observations=len(population), expected_findings=len(wanted),
            located_findings=len(actual), false_positives=len(actual - wanted),
            missed_findings=len(wanted - actual), incorrect_fields=sum(
                any(found[k].get(f) != v for f,v in truth[k].items()) for k in wanted & actual),
            unlocated_findings=sum(found[k].get('activity_id') not in admitted for k in actual))
    outside = len(found.keys() - (populations['new'] | populations['overlap']))
    bad = outside or any(s[k] for s in scores.values()
        for k in ('false_positives', 'missed_findings', 'incorrect_fields', 'unlocated_findings'))
    failures=[]
    for failure in sorted(app.path('evidence/failures').glob('*.json')):
        item=_json(failure)
        if item.get('ts') and timestamp(authorization['authorized_at'])<=timestamp(item['ts'])<min(
                timestamp(m['known_at']),timestamp(request['evidence_cutoff'])):
            failures.append(dict(failure_id=failure.stem,sha256=evaluation.file_hash(failure),
                scope='application failure; source attribution may be unavailable',status=item.get('status'),gate=item.get('gate')))
    status = 'rolled_back' if bad else 'retained' if scores['new']['expected_findings'] and not failures else 'inconclusive'
    return dict(status=status, candidate=VERSION, candidate_sha256=evaluation.candidate_identity(),
        scores=scores, findings=findings, outside_source_findings=outside,
        baseline=dict(method='Existing session-level flag', observation_locators=0,
            new_unlocated_discrepancies=scores['new']['expected_findings']),
        original_report_reproduced=True, next_report_reproduced=True,
        usage_policy='unchanged cumulative-only usage; last counters remain diagnostic',
        source_report_window=reporting_window,
        authorization_receipt_scope='first eligible postauthorization import; preparation-to-authorization evidence is overlap',
        intervening_import_failures=failures,
        coverage='approved account/source only; no broader completeness claim',
        manual_minutes=None, ongoing_activation=False,
        next_action='one-run authorization exhausted; any subsequent trial needs a new reviewed plan',
        reason='diagnostic regression' if bad else 'new discrepancy opportunity graded exactly' if status == 'retained'
            else 'intervening import failures require review; no improvement claim' if failures
            else 'no new discrepancy opportunity; no improvement claim'), inputs, m


def run(app, directory, *, packet_sha256, source_run_id):
    root, plan = _plan(app, directory)
    authorization, authority_record = _authorization(app, root)
    replay_receipts([authority_record['receipt_id']], db=app.db, output_root=app.outputs,
        ledger=app.ledger, application=app)
    supplied = dict(packet_sha256=packet_sha256, source_run_id=source_run_id)
    with ledger_lock(root / 'execution'):
        if (root / 'request.json').exists():
            saved_request=_json(root / 'request.json')
            if {k:saved_request.get(k) for k in supplied} != supplied:
                raise ValidationError('one-run authorization already bound to different input')
            if (root / 'completion.json').exists():
                saved = _json(root / 'completion.json')
                replay_receipts([saved['receipt_id']], db=app.db, output_root=app.outputs,
                    ledger=app.ledger, application=app)
                return dict(saved, retry='reproduced_existing_result')
            raise ValidationError('interrupted or failed trial requires review and a new signed plan')
        # Exclusive claim consumes this exact authorization even if a gate fails.
        request=dict(supplied,evidence_cutoff=iso(datetime.now(timezone.utc)))
        _write(root / 'request.json', request)
        started = perf_counter()
        try:
            result, inputs, source = _compute(app, plan, request, authorization)
            _plan(app, root)  # Fail if authority, input or code changed during trial.
        except Exception:
            _write(root / 'failure.json', dict(status='held', reason='trial gate failed; original reports preserved',
                retry='new reviewed plan required', candidate_activated=False))
            raise
        run_id = uuid.uuid4().hex
        output = app.outputs / run_id; output.mkdir()
        manifest = dict(schema_version=SCHEMA, run_id=run_id, application=app.name,
            binding_id=app.binding.identity, trial_directory=str(root),
            files={n:evaluation.file_hash(root / n) for n in ('plan.json','plan.json.sig','approved_signers',
                'authorization.json','authorization-receipt.json','request.json')},
            execution_artifacts=plan['execution_artifacts'], execution_hash=plan['execution_hash'],
            runtime=runtime(), definition_version=VERSION, query_hash=evaluation.candidate_identity(),
            inputs=inputs, asof=source['asof'], known_at=source['known_at'], watermark=source['watermark'],
            **code_revision(plan['execution_artifacts']))
        _write(output / 'run.json', manifest)
        shared = {k:manifest[k] for k in ('run_id','inputs','definition_version','query_hash','git_sha',
            'execution_hash','asof','known_at','watermark')}
        shared['manifest_sha256'] = evaluation.file_hash(output / 'run.json')
        record = dict(schema_version=SCHEMA, result=result, **shared)
        record['receipt_id'] = receipt_id(record)
        timing = dict(schema_version=OBSERVATION, result=dict(seconds=perf_counter()-started,
            manual_minutes=None, trial_receipt_id=record['receipt_id']), **shared)
        timing['receipt_id'] = receipt_id(timing)
        _write(output / 'trial.json', dict(receipt_id=record['receipt_id'], **result))
        append_receipts(app.ledger, [record, timing])
        summary = dict(status=result['status'], run_id=run_id, receipt_id=record['receipt_id'],
            timing_receipt_id=timing['receipt_id'], trial_directory=str(root), promotion='one_run_finished_not_enabled')
        _write(root / 'completion.json', summary)
        return summary


def revoke(app, directory, *, actor):
    app.validate(); root = _directory(app, directory)
    if not isinstance(actor, str) or not actor.strip() or actor != actor.strip():
        raise ValidationError('an explicit nonempty actor is required')
    with ledger_lock(root / 'execution'):
        if not (root / 'revoked.json').exists():
            _write(root / 'revoked.json', dict(actor=actor, actor_basis='caller claim; stop permission only',
                revoked_at=iso(datetime.now(timezone.utc))))
    return dict(status='revoked', promotion=False)


def replay(ids, *, application, output_root, ledger, python=None):
    if application is None:
        raise ValidationError('learning trial receipt requires its bound private application')
    app = application; records = read_receipts(ledger); answers = []
    for key in ids:
        record = records[key]; output = Path(output_root) / record['run_id']
        if evaluation.file_hash(output / 'run.json') != record['manifest_sha256']:
            raise ValidationError('trial run manifest changed')
        m = _json(output / 'run.json')
        if m['binding_id'] != app.binding.identity or m['application'] != app.name:
            raise ValidationError('trial receipt application differs')
        if any(record[k] != m[k] for k in ('run_id','inputs','definition_version','query_hash','git_sha',
                'execution_hash','asof','known_at','watermark')):
            raise ValidationError('trial receipt differs from its run contract')
        root = _directory(app, m['trial_directory'])
        for name, expected in m['files'].items():
            if name not in ('plan.json','plan.json.sig','approved_signers','authorization.json',
                    'authorization-receipt.json','request.json') or evaluation.file_hash(root / name) != expected:
                raise ValidationError('retained approved trial evidence changed')
        if python or m['execution_artifacts'] != evaluation.artifacts(app) or m['runtime'] != runtime():
            from tl.receipts.archive import historical_replay
            answers.extend(historical_replay([key], manifest=m, db=app.db, output_root=output_root,
                ledger=ledger, python=python, application_config=app.config)); continue
        _, plan = _plan(app, root, live=False)
        if record['schema_version']==AUTHORIZATION:
            authorization=_json(root/'authorization.json');baseline=authorization['baseline']
            if (authorization['plan_sha256']!=m['files']['plan.json']
                    or authorization['signature_sha256']!=m['files']['plan.json.sig']
                    or record['result']['authorization']!=authorization
                    or record['result']['authorization_sha256']!=m['files']['authorization.json']):
                raise ValidationError('observed authorization differs from its signed plan')
            with app.session(asof=baseline['asof'],known_at=baseline['known_at'],watermark=baseline['watermark']) as session:
                session.verify()
                if input_manifest(session.snapshot)!=m['inputs'] or m['inputs']!=baseline['inputs']:
                    raise ValidationError('authorization source boundary changed')
            answers.append(dict(receipt_id=key,verified=True,recomputed=False,result=record['result']))
            continue
        observed = record['schema_version'] == OBSERVATION
        if not observed:
            authorization, authority_record=_authorization(app,root)
            replay_receipts([authority_record['receipt_id']],db=app.db,output_root=app.outputs,ledger=app.ledger,application=app)
            actual, inputs, _ = _compute(app, plan, _json(root / 'request.json'), authorization)
            if actual != record['result'] or inputs != m['inputs'] or _json(output / 'trial.json') != dict(receipt_id=key, **actual):
                raise ValidationError('trial re-performance differs from receipt')
        answers.append(dict(receipt_id=key, verified=True, recomputed=not observed, result=record['result']))
    return answers
