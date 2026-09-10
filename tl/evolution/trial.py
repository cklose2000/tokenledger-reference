"""Prepare and measure one signed next inspection, with no promotion operation."""
from datetime import datetime, timezone, timedelta
from pathlib import Path
import hashlib
import json
import uuid

import pyarrow.parquet as pq

from tl.evolution import admission as a, diagnostic as d, evidence as e, github, transport
from tl.receipts.metrics import code_revision, digest, read_receipts
from tl.stream import ValidationError
from tl.stream.events import canonical, iso, timestamp


def state(config, output_root, ledger):
    observed = transport.snapshot(config, output_root=output_root, ledger=ledger)
    rows = pq.read_table(Path(observed['run_directory']) / 'source.parquet').to_pylist()
    return a.validate(rows, [], config), observed


def current_execution():
    pins = e.artifacts()
    revision = code_revision(pins)
    if not revision['execution_artifacts_match_git']:
        raise ValidationError('freeze the exact learning execution before preparing or running a trial')
    return digest(pins), revision['git_sha']


def plan_document(config, case_id, *, proposal_id, evaluation_id, proposal, execution_hash, pr_head,
                  prepared_watermark, expires_at, now=None):
    """The exact one-run plan a reviewer signs. Limits are fixed, never caller-supplied."""
    now = now or datetime.now(timezone.utc); expires = timestamp(expires_at)
    if not now < expires <= now + timedelta(days=30):
        raise ValidationError('trial expiry must be future and no more than 30 days away')
    return dict(schema_version=a.PLAN, case_id=case_id, gateway_identity=config.identity,
        proposal_id=proposal_id, evaluation_id=evaluation_id,
        candidate=a.CANDIDATE, candidate_sha256=proposal['candidate_sha256'],
        execution_sha256=execution_hash, cases_sha256=proposal['cases_sha256'], pr_head=pr_head,
        prepared_watermark=prepared_watermark, nonce=uuid.uuid4().hex,
        created_at=iso(now), expires_at=iso(expires), reviewer_principal=a.PRINCIPAL,
        signature_namespace=a.NAMESPACE, trust_sha256=hashlib.sha256(config.learning_signers.encode()).hexdigest(),
        limits=dict(runs=1, diagnostic_only=True, source_writes=False, policy_changes=False,
                    reporting_gate_changes=False, automatic_followup=False, ongoing_activation=False))


def prepare(config, case_id, *, output_root, ledger, expires_at):
    records, observed = state(config, output_root, ledger)
    own = [item for item in records.values() if item[0] == case_id]
    evaluation = next((item for item in own if item[1]['activity'] == 'candidate_evaluated'), None)
    if evaluation is None or evaluation[2]['verdict'] != 'passed':
        raise ValidationError('case needs a passed native candidate evaluation')
    if any(item[1]['activity'] == 'trial_authorized' for item in own):
        raise ValidationError('case already has one-run authority; preserve it and open a new case')
    proposal = records[evaluation[2]['proposal_id']]
    number = int(proposal[2]['pr_url'].rsplit('/',1)[1])
    pull = github.inspect(number)
    if not pull['ci_passed'] or any(pull[k] != proposal[2][k] for k in ('pr_head','changed_files')):
        raise ValidationError('private PR head, scope or required CI differs from the proposal')
    receipt = read_receipts(ledger).get(evaluation[2]['evaluation_receipt_id'])
    if receipt is None:
        raise ValidationError('restore the retained native evaluation receipt before trial preparation')
    e.replay([receipt['receipt_id']], output_root=output_root, ledger=ledger)
    execution_hash, git_sha = current_execution()
    if pull['pr_head'] != git_sha:
        raise ValidationError('trial preparation must run from the exact private PR head')
    if (receipt['schema_version'] != e.SCHEMA or receipt['result']['status'] != 'passed'
            or receipt['result'].get('measurement') != 'frozen_evaluation'
            or receipt['execution_hash'] != execution_hash
            or receipt['binding_identity'] != evaluation[2]['evaluation_binding_identity']
            or receipt['candidate_sha256'] != proposal[2]['candidate_sha256']
            or receipt['cases_sha256'] != proposal[2]['cases_sha256']
            or proposal[2]['execution_sha256'] != execution_hash):
        raise ValidationError('candidate and evaluation no longer match the execution')
    plan = plan_document(config, case_id, proposal_id=proposal[1]['activity_id'], evaluation_id=evaluation[1]['activity_id'],
        proposal=proposal[2], execution_hash=execution_hash, pr_head=pull['pr_head'],
        prepared_watermark=observed['result']['watermark'], expires_at=expires_at)
    root = Path(output_root) / uuid.uuid4().hex
    root.mkdir(parents=True, exist_ok=False)
    e.write(root / 'plan.json', plan)
    e.write(root / 'pr.json', pull)
    e.write(root / 'evaluation.json', receipt)
    e.write(root / 'snapshot-observation.json', observed)
    e.write(root / 'review.json', dict(status='prepared_not_approved', git_sha=git_sha,
        plan_sha256=hashlib.sha256((root/'plan.json').read_bytes()).hexdigest(),
        candidate=a.CANDIDATE, requested_authority=plan['limits'], native_evaluation_receipt=receipt['receipt_id'],
        next_run='First newly admitted NRR inspection; no opportunity means inconclusive.',
        user_action='Review this exact plan and native evidence, then sign plan.json outside this application.'))
    return dict(status='prepared_not_approved', directory=str(root.resolve()), plan=str((root/'plan.json').resolve()),
                evaluation_receipt_id=receipt['receipt_id'], ongoing_activation=False)


def authorize(config, plan_path, signature_path, producer, url, *, output_root, ledger):
    value = dict(plan_json=Path(plan_path).read_bytes().decode('utf-8'),
                 signature=Path(signature_path).read_bytes().decode('utf-8'))
    plan = a._signed_plan(value, config)
    execution_hash, _ = current_execution()
    if execution_hash != plan['execution_sha256']:
        raise ValidationError('signed execution changed')
    event = transport.event('trial_authorized', plan['case_id'], value, 'approval-' + plan['nonce'])
    return transport.submit(config, event, producer, url, output_root=output_root, ledger=ledger)


def run(config, authorization_id, export, binding, producer, url, *, output_root, ledger):
    records, observed = state(config, output_root, ledger)
    authorization = records.get(authorization_id)
    if authorization is None or authorization[1]['activity'] != 'trial_authorized':
        raise ValidationError('an admitted signed authorization is required before the next inspection')
    case_id, auth_row, auth_value = authorization
    plan = auth_value['_plan']
    own = [item for item in records.values() if item[0] == case_id]
    if any(item[1]['activity'] in ('trial_started','trial_completed','trial_revoked') for item in own):
        raise ValidationError('one-run authority is consumed or revoked; replay retained evidence, never collect again')
    requests = [item for item in own if item[1]['activity'] == 'inspection_requested'
                and item[1]['_stream_position'] > auth_row['_stream_position']]
    if not requests:
        raise ValidationError('waiting for the first newly admitted reporting inspection')
    inspection = requests[0]
    execution_hash, git_sha = current_execution()
    if execution_hash != plan['execution_sha256'] or datetime.now(timezone.utc) > timestamp(plan['expires_at']):
        raise ValidationError('signed candidate changed or expired')
    proposal = records[plan['proposal_id']]
    pull = github.inspect(int(proposal[2]['pr_url'].rsplit('/',1)[1]))
    if pull['pr_head'] != plan['pr_head'] or git_sha != plan['pr_head'] or not pull['ci_passed']:
        raise ValidationError('PR changed or required checks no longer pass')
    evaluation = records[plan['evaluation_id']]
    if binding.identity != evaluation[2]['evaluation_binding_identity']:
        raise ValidationError('trial must use the evaluated native reporting binding')
    # The requested receipt must describe this exact verified export. A caller
    # cannot attach an unrelated report and then run a more favorable population.
    from tl.bigquery.export import read_export
    manifest = read_export(export, binding)
    original = read_receipts(ledger).get(inspection[2]['report_receipt_id'], {})
    if (original.get('schema_version') != 'tokenledger-bigquery/v1'
            or original['result']['output'] != 'consumption_nrr'
            or original.get('query_hash') != manifest['files']['queries/consumption_nrr.sql']
            or original.get('definition_version') != 'accounting/v1;nrr/'+manifest['definition_version']
            or any(original[k] != manifest[k] for k in ('binding_identity','asof','known_at','watermark','inputs'))):
        raise ValidationError('inspection report differs from the retained native export')
    # Each process competes with a distinct claim. A deterministic shared ID
    # would let two simultaneous runners both accept an idempotent retry and
    # execute. The gateway's once-per-case transition elects exactly one claim.
    claim_id = 'trial-' + plan['nonce'] + '-' + uuid.uuid4().hex
    claim = transport.event('trial_started', case_id, dict(authorization_id=authorization_id,
        inspection_id=inspection[1]['activity_id'], candidate_sha256=plan['candidate_sha256'],
        execution_sha256=execution_hash), claim_id)
    admitted = transport.submit(config, claim, producer, url, output_root=output_root, ledger=ledger)
    if admitted['status'] != 'accepted':
        raise ValidationError('trial claim is unverified; no diagnostic execution occurred')
    root = Path(output_root) / uuid.uuid4().hex
    root.mkdir(parents=True, exist_ok=False)
    e.write(root / 'claim.json', admitted)
    e.write(root / 'authorization.json', auth_value)
    e.write(root / 'before.json', observed)
    try:
        measured = e.evaluate(binding, export, output_root=output_root, ledger=ledger,
            inspection=dict(case_id=inspection[1]['activity_id'], requested_date=inspection[2]['requested_date']))
        scored = measured['result']
        status = ('failed' if not scored['equivalent'] else 'improved' if
                  scored['diagnostic_rows'][0]['status'] == 'date_grain_mismatch' else 'inconclusive')
        result = dict(status=status, measurement_receipt_id=measured['receipt_id'],
                      finding=scored['diagnostic_rows'][0], report_population_changed=False,
                      investigation_minutes=None, ongoing_activation=False,
                      interpretation='One diagnostic opportunity, not general agent efficiency or ongoing self-improvement.')
    except Exception as exc:
        result = dict(status='failed', error_type=type(exc).__name__, ongoing_activation=False,
                      retry='No new collection under this exhausted one-run authorization.')
    record = e.seal(root, dict(schema_version=e.OBSERVATION, run_id=root.name, result=result,
        definition_version=a.CANDIDATE, inputs=observed['result']['source'],
        query_hash=digest(dict(plan=plan, inspection_id=inspection[1]['activity_id'])),
        git_sha=git_sha, execution_hash=execution_hash, binding_identity=config.identity), ledger)
    completion = transport.event('trial_completed', case_id, dict(trial_id=claim_id,
        outcome_receipt_id=record['receipt_id'], status=result['status'], ongoing_activation=False),
        'outcome-' + plan['nonce'])
    e.write(root / 'completion-event.json', completion)
    completed = transport.submit(config, completion, producer, url, output_root=output_root, ledger=ledger)
    return dict(status=result['status'], receipt_id=record['receipt_id'], result=result,
                completion_status=completed['status'], completion_event=str((root/'completion-event.json').resolve()))
