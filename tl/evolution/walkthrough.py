"""Credential-free synthetic rehearsal of the governed reporting-learning loop.

One new synthetic business, one real July NRR report with its receipt, one wrong
date inspection, and then the closed workflow: finding, verification (with a
refuted finding kept visible), bounded candidate, frozen evaluation, the signed
approval boundary, the first next inspection, its measured outcome, consumed
authority and original-report replay.

Everything runs through the same Gateway, admission validation, diagnostic
candidate, scalar oracle and receipt engine as the native application. Only the
warehouse is replaced: an in-memory append-only stream stands in for the pinned
BigQuery write stream, and DuckDB re-performs the candidate SQL.

The signer is a disposable key generated inside the walkthrough directory. It
is not Chandler's enrolled key, the gateway configuration that trusts it is
marked synthetic, cannot be served, and is refused by every cloud learning
command. A walkthrough outcome is a rehearsal observation, never a trial result.
"""
from copy import deepcopy
from datetime import datetime, timezone, timedelta
from pathlib import Path
from time import perf_counter
import hashlib
import json
import subprocess
import threading
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

from tl.bigquery.config import Binding
from tl.bigquery.hashing import encode
from tl.bigquery.schema import FIELDS
from tl.evolution import admission as a, diagnostic as d, evidence as e
from tl.generate import Config, SyntheticWorld
from tl.learning.signatures import NAMESPACE
from tl.receipts.metrics import code_revision, digest, read_receipts
from tl.stream import Activity, Stream, ValidationError
from tl.stream.events import Catalog, canonical, iso, timestamp
from tl.stream.store import prepare_row

SCHEMA = 'tokenledger-reporting-learning-walkthrough/v1'
PROJECT = 'tokenledger-walkthrough'
PRODUCER_DOMAIN = PROJECT + '.iam.gserviceaccount.com'
ORIGIN_SUFFIX = '.invalid'
AUDIENCE = 'https://walkthrough.tokenledger.invalid'
ROLES = {
    'proposer': ('finding_submitted', 'candidate_proposed', 'inspection_requested'),
    'verifier': ('finding_verified', 'candidate_evaluated'),
    'reviewer': ('trial_authorized', 'trial_revoked'),
    'runner': ('trial_started', 'trial_completed'),
}
ACTORS = {role: f'walkthrough-{role}@{PRODUCER_DOMAIN}' for role in ROLES}
ASOF = '2026-07-31'
KNOWN_AT = '2026-10-01T00:00:00Z'
CASE = 'walkthrough-nrr-date'
REFUTED_CASE = 'walkthrough-refuted-claim'
# The historical bounded engine PR that introduced this candidate. Recorded as
# the pinned proposal scope; the walkthrough performs no GitHub inspection.
PR_URL = 'https://github.com/cklose2000/tokenledger/pull/22'
PR_HEAD = '03016001d2aa56cab6344f0a9737d9bd18643a12'
CANDIDATE_FILES = ['tl/evolution/sql/nrr_date_diagnostic_v1.sql', 'definitions/reporting-learning/nrr-date-cases.v1.yaml']
AUTHORITY = ('Disposable synthetic signer generated in this directory. Not Chandler, not enrolled trust, '
             'not a real trial authorization.')
OUTCOME_MEANING = {
    'improved': 'The candidate explained a builder-chosen synthetic wrong-date inspection. Not a measured operating gain.',
    'inconclusive': 'The synthetic inspection selected the report directly, so the candidate had no opportunity.',
    'failed': 'The candidate and oracle disagreed on the synthetic inspection.',
}
ACTUAL_TRIAL = dict(status='inconclusive', signed_revision='fabd91acc87f8eabe0d91a84a6aabc4221e05ce7',
                    inspection_date='2026-07-31', selected_rows=4, ongoing_activation=False, promotion=False,
                    note='The one real signed trial requested the report date and supplied no mismatch opportunity. '
                         'Its private receipts are not in Git; this rehearsal does not replay or replace it.')


class LocalStream:
    """In-memory append-only stream with the exact offset contract the Gateway expects."""

    def __init__(self, config):
        self.config = config
        self.rows = []
        self.jobs = []
        self.sdk = self
        self.lock = threading.Lock()

    def get_table(self, name):
        if name != self.config.binding.table:
            raise ValidationError('walkthrough stream bound to a different table')
        fields = []
        for field, kind, nullable in FIELDS:
            raw = dict(name=field, type='NUMERIC' if kind.startswith('NUMERIC') else kind,
                       mode='NULLABLE' if nullable else 'REQUIRED')
            if kind.startswith('NUMERIC'):
                raw.update(precision=18, scale=2)
            fields.append(raw)
        raw = dict(creationTime=self.config.table_creation_time, schema=dict(fields=fields),
                   timePartitioning=dict(field='ts', type='DAY'), clustering=dict(fields=['customer', 'activity']))
        return type('Table', (), dict(to_api_repr=lambda self: raw))()

    def metadata(self, name):
        if name != self.config.write_stream:
            raise ValidationError('walkthrough stream identity differs')
        return type('WriteStream', (), dict(name=name, type_=1, location=self.config.binding.location))()

    def query(self, sql, *, label):
        with self.lock:
            rows = deepcopy(self.rows)
        self.jobs.append(dict(job_id='local-' + str(len(self.jobs) + 1), label=label, status='local_synthetic_not_native'))
        if not rows:
            return pa.table({name: [] for name, _, _ in FIELDS}), self.jobs[-1]
        return table(rows), self.jobs[-1]

    def append(self, name, offset, rows):
        if name != self.config.write_stream:
            raise ValidationError('walkthrough stream identity differs')
        with self.lock:
            if offset != len(self.rows):
                raise RuntimeError('ALREADY_EXISTS')
            self.rows.extend(deepcopy(rows))


def table(rows):
    return pa.Table.from_pylist(rows).select([f[0] for f in FIELDS])


def configuration(signers):
    binding = Binding(PROJECT, 'synthetic', 'us-east4', 10**8, 10**9,
                      query_principal=f'walkthrough-reader@{PRODUCER_DOMAIN}',
                      writer_principal=f'walkthrough-writer@{PRODUCER_DOMAIN}')
    from tl.bigquery.gateway import GatewayConfig
    return GatewayConfig(binding, AUDIENCE, '1788793200000',
                         f'projects/{PROJECT}/datasets/synthetic/tables/activity/streams/WALKTHROUGH',
                         Catalog(a.CATALOG).digest, {ACTORS[role]: 'walkthrough/' + role for role in ROLES},
                         allowed_activities={ACTORS[role]: names for role, names in ROLES.items()},
                         admission_policy=a.POLICY, learning_signers=signers, synthetic_rehearsal=True)


def keygen(directory):
    """Disposable ed25519 key. Renamed in place so OpenSSH keeps its own permissions."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=False)
    created = directory / 'generated'
    try:
        subprocess.run(['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-C', 'SYNTHETIC-WALKTHROUGH-SIGNER not-chandler',
                        '-f', str(created)], capture_output=True, check=True, timeout=60)
    except (OSError, subprocess.SubprocessError) as exc:
        raise ValidationError('the walkthrough needs OpenSSH ssh-keygen on PATH to create its disposable signer') from exc
    key = directory / 'synthetic-signer'
    created.rename(key)
    created.with_suffix('.pub').rename(key.with_suffix('.pub'))
    public = ' '.join(key.with_suffix('.pub').read_text(encoding='utf-8').split()[:2])
    # The admission contract fixes the principal label; the key comment and the
    # synthetic configuration keep the disposable trust visibly distinct.
    signers = f'{a.PRINCIPAL} {public} synthetic-walkthrough-signer\n'
    (directory / 'allowed_signers').write_text(signers, encoding='utf-8', newline='\n')
    return key, signers


def sign(key, plan, path):
    path = Path(path)
    with path.open('x', encoding='utf-8', newline='\n') as handle:
        handle.write(canonical(plan) + '\n')
    subprocess.run(['ssh-keygen', '-q', '-Y', 'sign', '-f', str(key), '-n', NAMESPACE, str(path)],
                   capture_output=True, check=True, timeout=60)
    return dict(plan_json=path.read_text(encoding='utf-8'), signature=path.with_name(path.name + '.sig').read_text(encoding='utf-8'))


def population_rows(run_directory):
    raw = pq.read_table(Path(run_directory) / 'consumption_nrr.parquet').select(['month', 'lens', 'status', 'nrr'])
    return [dict(row, month=row['month'].isoformat()) for row in raw.to_pylist()]


def population_sha256(rows):
    return hashlib.sha256(canonical(encode(rows)).encode()).hexdigest()


def evaluate(rows, requests, *, output_root, ledger, config, report_receipt, measurement):
    """DuckDB re-performance of the frozen candidate against a receipted local report."""
    pins = e.artifacts(); revision = code_revision(pins)
    root = Path(output_root) / uuid.uuid4().hex
    root.mkdir(parents=True, exist_ok=False)
    sql = d.query('SELECT * FROM retained', requests, dialect='duckdb')
    actual = sorted(d.offline(rows, requests).to_pylist(), key=lambda row: row['case_id'])
    reference = []
    for case in requests:
        answer = d.oracle(d.scenario(rows, case.get('scenario', 'original')), case['requested_date'], case.get('population', 'nrr'))
        if 'expected' in case and answer['status'] != case['expected']:
            raise ValidationError('synthetic population does not exercise frozen case: ' + case['case_id'])
        reference.append(dict(case_id=case['case_id'], **answer))
    reference.sort(key=lambda row: row['case_id'])
    matched = actual == reference
    result = dict(status='passed' if matched else 'failed', cases=len(reference), equivalent=matched,
                  diagnostic_rows=actual, oracle_rows=reference, report_receipt_id=report_receipt['receipt_id'],
                  target='duckdb', native_execution=False, synthetic=True, measurement=measurement,
                  baseline_behavior='An unmatched date selects zero rows without an executable explanation.',
                  report_population_changed=False, reporting_controls_changed=False,
                  held_out_accuracy='not_measured', ongoing_activation=False)
    (root / 'candidate.sql').write_text(sql, encoding='utf-8', newline='\n')
    pq.write_table(pa.Table.from_pylist(rows), root / 'report.parquet')
    e.write(root / 'requests.json', requests)
    e.write(root / 'oracle.json', reference)
    e.write(root / 'diagnostics.json', actual)
    e.write(root / 'report-receipt.json', report_receipt)
    (root / 'frozen-cases.yaml').write_bytes(d.CASES.read_bytes())
    record = e.seal(root, dict(schema_version=SCHEMA, run_id=root.name, result=result,
        definition_version=a.CANDIDATE, inputs=report_receipt['inputs'],
        query_hash=hashlib.sha256(sql.encode()).hexdigest(), execution_artifacts=pins, execution_hash=digest(pins),
        git_sha=revision['git_sha'], execution_artifacts_match_git=revision['execution_artifacts_match_git'],
        asof=report_receipt['asof'], known_at=report_receipt['known_at'], watermark=report_receipt['watermark'],
        binding_identity=config.identity, candidate_sha256=d.pins()[d.SQL.as_posix()],
        cases_sha256=d.pins()[d.CASES.as_posix()]), ledger)
    return dict(status=result['status'], receipt_id=record['receipt_id'], run_directory=str(root.resolve()), result=result)


def run(destination=None, *, customers=28, next_inspection_date='2026-07-01', progress=None):
    started = perf_counter()
    from tl.bigquery.gateway import Gateway, AuthenticationError
    from tl.views.engine import profile, replay as replay_views
    from datetime import date
    try:
        if date.fromisoformat(next_inspection_date).isoformat() != next_inspection_date: raise ValueError
    except (TypeError, ValueError):
        raise ValidationError('next inspection date must be an ISO date') from None
    root = Path(destination or Path('data/learning-walkthrough') / uuid.uuid4().hex).resolve()
    if root.exists() and any(root.iterdir()):
        raise ValidationError('walkthrough requires a new or empty directory; existing evidence is never overwritten')
    root.mkdir(parents=True, exist_ok=True)
    db = root / 'world.duckdb'; outputs = root / 'runs'; ledger = root / 'metrics.jsonl'
    def step(message):
        if progress: progress(message)

    step('Generate an isolated synthetic business and report July NRR with a receipt.')
    stream = Stream(db); stream.init()
    generated = stream.generate(SyntheticWorld(Config(customers=customers)), workers=1)
    original = profile(db, asof=ASOF, known_at=KNOWN_AT, names=['consumption_nrr'], output_root=outputs, ledger=ledger)
    records = read_receipts(ledger)
    report_receipt = next(records[k] for k in original['receipt_ids']
                          if records[k]['schema_version'] == 'tokenledger-views/v1' and records[k]['result']['output'] == 'consumption_nrr')
    rows = population_rows(original['run_directory'])
    evidence_sha = population_sha256(rows)

    step('Inspect that report with the wrong date grain: the month start against a month-end report.')
    wrong_date = '2026-07-01'
    baseline = dict(requested_date=wrong_date, report_date=ASOF, report_rows=len(rows),
                    selected_rows=sum(row['month'] == wrong_date for row in rows), explanation='none')

    step('Create a disposable synthetic signer and a synthetic gateway with the same admission rules.')
    key, signers = keygen(root / 'signer')
    config = configuration(signers)
    config.save(root / 'gateway.json')
    backend = LocalStream(config)
    gateway = Gateway(config, a.CATALOG, client=backend, storage=backend, sleep=lambda _: None)
    events = []
    refusals = []

    def event(kind, case, record, event_id):
        now = datetime.now(timezone.utc)
        return dict(activity_id=event_id, ts=iso(now), customer='learning:' + case, anonymous_customer_id=None,
                    activity=kind, feature_json=a.envelope(case, record), revenue_impact=None, link=None)

    def admit(kind, case, record, event_id, *, role=None):
        role = role or next(name for name, names in ROLES.items() if kind in names)
        value = event(kind, case, record, event_id)
        result = gateway.append(canonical(dict(events=[value])).encode(), ACTORS[role])
        if result['status'] != 'accepted':
            raise ValidationError('walkthrough gateway did not accept ' + kind)
        events.append(dict(activity_id=event_id, activity=kind, case_id=case, role=role, actor=ACTORS[role],
                           stream_position=result['watermark']))
        return event_id

    def refuse(label, expected, action):
        before = deepcopy(backend.rows)
        try:
            action()
        except (ValidationError, AuthenticationError) as exc:
            message = str(exc)
            if expected not in message or backend.rows != before:
                raise ValidationError(f'walkthrough refusal {label} did not behave as specified: {message}')
            refusals.append(dict(label=label, refused=True, error_type=type(exc).__name__, message=message,
                                 stream_rows_before=len(before), stream_rows_after=len(backend.rows)))
            return
        raise ValidationError('walkthrough expected the gateway to refuse: ' + label)

    step('Record the finding. The proposer cannot verify its own claim; a tampered envelope never writes.')
    finding = admit('finding_submitted', CASE, dict(report_receipt_id=report_receipt['receipt_id'], requested_date=wrong_date,
        description='Requesting the July NRR report at 2026-07-01 returned no rows; the report is dated 2026-07-31.',
        evidence_sha256=evidence_sha), 'finding-' + uuid.uuid4().hex)
    verification_record = dict(finding_id=finding, verdict='confirmed', evidence_receipt_id=report_receipt['receipt_id'],
                               rationale='Re-read the receipted population: four defined lenses dated 2026-07-31, zero rows at 2026-07-01.')
    refuse('finding author verifying its own finding', 'principal cannot emit this activity',
           lambda: admit('finding_verified', CASE, verification_record, 'self-verification', role='proposer'))
    def tampered():
        value = event('finding_verified', CASE, verification_record, 'tampered-verification')
        value['feature_json']['record_sha256'] = '0' * 64
        gateway.append(canonical(dict(events=[value])).encode(), ACTORS['verifier'])
    refuse('verification with a tampered content hash', 'content hash', tampered)

    step('Verify both findings against the retained population. A refuted claim stays on the record.')
    verifier_view = d.oracle(rows, wrong_date)
    if verifier_view['selected_rows'] != 0 or verifier_view['status'] != 'date_grain_mismatch':
        raise ValidationError('walkthrough population did not reproduce the wrong-date finding')
    verification = admit('finding_verified', CASE, verification_record, 'verification-' + uuid.uuid4().hex)
    bad_finding = admit('finding_submitted', REFUTED_CASE, dict(report_receipt_id=report_receipt['receipt_id'], requested_date=ASOF,
        description='Claim: the July NRR report is empty at its own reporting date.', evidence_sha256=evidence_sha),
        'finding-' + uuid.uuid4().hex)
    refuted_view = d.oracle(rows, ASOF)
    bad_verification = admit('finding_verified', REFUTED_CASE, dict(finding_id=bad_finding, verdict='refuted',
        evidence_receipt_id=report_receipt['receipt_id'],
        rationale=f"Re-read the receipted population: {refuted_view['selected_rows']} rows selected at {ASOF}; the claim is false."),
        'verification-' + uuid.uuid4().hex)
    execution_hash = digest(e.artifacts())
    proposal_record = dict(verification_id=bad_verification, candidate=a.CANDIDATE, candidate_sha256=d.pins()[d.SQL.as_posix()],
        execution_sha256=execution_hash, cases_sha256=d.pins()[d.CASES.as_posix()], pr_url=PR_URL, pr_head=PR_HEAD,
        changed_files=CANDIDATE_FILES)
    refuse('candidate proposed from a refuted finding', 'confirmed failure',
           lambda: admit('candidate_proposed', REFUTED_CASE, proposal_record, 'proposal-from-refuted'))

    step('Propose the bounded candidate and evaluate its eleven frozen cases in DuckDB against the scalar oracle.')
    proposal_record = dict(proposal_record, verification_id=verification)
    proposal = admit('candidate_proposed', CASE, proposal_record, 'proposal-' + uuid.uuid4().hex)
    evaluation = evaluate(rows, d.cases()['cases'], output_root=outputs, ledger=ledger, config=config,
                          report_receipt=report_receipt, measurement='synthetic_frozen_evaluation')
    if evaluation['status'] != 'passed':
        raise ValidationError('walkthrough candidate evaluation failed; retained ' + evaluation['receipt_id'])
    refuse('evaluation claiming native BigQuery execution on the synthetic gateway', 'native candidate',
           lambda: admit('candidate_evaluated', CASE, dict(proposal_id=proposal, verdict='passed',
               evaluation_receipt_id=evaluation['receipt_id'], candidate_sha256=proposal_record['candidate_sha256'],
               cases_sha256=proposal_record['cases_sha256'], target='bigquery', evaluation_binding_identity=config.identity),
               'native-claim'))
    evaluated = admit('candidate_evaluated', CASE, dict(proposal_id=proposal, verdict='passed',
        evaluation_receipt_id=evaluation['receipt_id'], candidate_sha256=proposal_record['candidate_sha256'],
        cases_sha256=proposal_record['cases_sha256'], target='duckdb', evaluation_binding_identity=config.identity),
        'evaluation-' + uuid.uuid4().hex)

    step('Approval boundary: prepare the exact one-run plan; expanded, tampered or unsigned approvals fail closed.')
    from tl.evolution.trial import plan_document
    now = datetime.now(timezone.utc)
    plan = plan_document(config, CASE, proposal_id=proposal, evaluation_id=evaluated, proposal=proposal_record,
                         execution_hash=execution_hash, pr_head=PR_HEAD, prepared_watermark=len(backend.rows),
                         expires_at=iso(now + timedelta(days=1)), now=now)
    approval = sign(key, plan, root / 'plan.json')
    expanded = deepcopy(plan); expanded['limits']['ongoing_activation'] = True
    refuse('signed plan that expands authority to ongoing activation', 'limits differ',
           lambda: admit('trial_authorized', CASE, sign(key, expanded, root / 'rejected-expanded-plan.json'), 'approval-expanded'))
    altered = dict(approval, plan_json=approval['plan_json'].replace(plan['nonce'], uuid.uuid4().hex))
    refuse('signed bytes altered after signing', 'signature did not verify',
           lambda: admit('trial_authorized', CASE, altered, 'approval-altered'))
    refuse('proposer submitting the approval', 'principal cannot emit this activity',
           lambda: admit('trial_authorized', CASE, approval, 'approval-by-proposer', role='proposer'))
    authorization = admit('trial_authorized', CASE, approval, 'approval-' + plan['nonce'])

    step('Admit the next inspections. Only the first one after authorization may be claimed, exactly once.')
    first = admit('inspection_requested', CASE, dict(report_receipt_id=report_receipt['receipt_id'], population='nrr',
        requested_date=next_inspection_date), 'inspection-' + uuid.uuid4().hex)
    second = admit('inspection_requested', CASE, dict(report_receipt_id=report_receipt['receipt_id'], population='nrr',
        requested_date=ASOF), 'inspection-' + uuid.uuid4().hex)
    claim_record = lambda inspection: dict(authorization_id=authorization, inspection_id=inspection,
                                           candidate_sha256=plan['candidate_sha256'], execution_sha256=execution_hash)
    refuse('claiming a later, more favorable inspection', 'first next inspection',
           lambda: admit('trial_started', CASE, claim_record(second), 'trial-' + plan['nonce'] + '-' + uuid.uuid4().hex))
    claim = admit('trial_started', CASE, claim_record(first), 'trial-' + plan['nonce'] + '-' + uuid.uuid4().hex)

    step('Measure the claimed inspection with the candidate, record the outcome and close the one-run authority.')
    measured = evaluate(rows, [dict(case_id=first, population='nrr', requested_date=next_inspection_date, scenario='original')],
                        output_root=outputs, ledger=ledger, config=config, report_receipt=report_receipt,
                        measurement='synthetic_inspection_diagnostic')
    finding_row = measured['result']['diagnostic_rows'][0]
    status = ('failed' if not measured['result']['equivalent'] else 'improved'
              if finding_row['status'] == 'date_grain_mismatch' else 'inconclusive')
    outcome_root = outputs / uuid.uuid4().hex
    outcome_root.mkdir(parents=True, exist_ok=False)
    outcome = dict(status=status, measurement_receipt_id=measured['receipt_id'], finding=finding_row,
                   report_population_changed=False, investigation_minutes=None, ongoing_activation=False,
                   synthetic=True, native_execution=False, interpretation=OUTCOME_MEANING[status], actual_native_trial=ACTUAL_TRIAL)
    e.write(outcome_root / 'claim.json', dict(claim_id=claim, inspection_id=first, requested_date=next_inspection_date))
    e.write(outcome_root / 'authorization.json', dict(authorization_id=authorization, authority=AUTHORITY, plan=plan))
    pins = e.artifacts(); revision = code_revision(pins)
    outcome_receipt = e.seal(outcome_root, dict(schema_version=e.OBSERVATION, run_id=outcome_root.name, result=outcome,
        definition_version=a.CANDIDATE, inputs=report_receipt['inputs'],
        query_hash=digest(dict(plan=plan, inspection_id=first)), git_sha=revision['git_sha'],
        execution_hash=digest(pins), binding_identity=config.identity), ledger)
    admit('trial_completed', CASE, dict(trial_id=claim, outcome_receipt_id=outcome_receipt['receipt_id'], status=status,
                                        ongoing_activation=False), 'outcome-' + plan['nonce'])
    refuse('second trial under the consumed authority', 'phase already recorded',
           lambda: admit('trial_started', CASE, claim_record(second), 'trial-' + plan['nonce'] + '-' + uuid.uuid4().hex))
    refuse('revoking authority after completion', 'cannot be revoked retroactively',
           lambda: admit('trial_revoked', CASE, dict(authorization_id=authorization, reason='Too late.'), 'revoke-late'))
    refuse('unregistered promotion activity', 'principal cannot emit this activity',
           lambda: admit('candidate_promoted', CASE, dict(status='approved'), 'promotion', role='runner'))

    step('Reproduce the original July report and confirm the business stream holds no learning rows.')
    reproduced = replay_views([report_receipt['receipt_id']], db=db, output_root=outputs, ledger=ledger)
    if not (reproduced and reproduced[0]['verified'] and reproduced[0]['recomputed'] == 'spine_population'):
        raise ValidationError('original report did not reproduce after the learning workflow')
    separation = business_separation(db, generated['watermark'])
    learning_rows = table(backend.rows)
    stream_path = root / 'learning-stream.parquet'
    pq.write_table(learning_rows, stream_path)
    a.validate(pq.read_table(stream_path).to_pylist(), [], config)

    seal_root = outputs / uuid.uuid4().hex
    seal_root.mkdir(parents=True, exist_ok=False)
    for name in ('learning-stream.parquet', 'gateway.json', 'plan.json', 'plan.json.sig'):
        (seal_root / name).write_bytes((root / name).read_bytes())
    e.write(seal_root / 'events.json', events)
    e.write(seal_root / 'refusals.json', refusals)
    e.write(seal_root / 'baseline.json', baseline)
    summary = dict(status='rehearsed', synthetic=True, native_execution=False, case_id=CASE, refuted_case_id=REFUTED_CASE,
        report_receipt_id=report_receipt['receipt_id'], evaluation_receipt_id=evaluation['receipt_id'],
        measurement_receipt_id=measured['receipt_id'], outcome_receipt_id=outcome_receipt['receipt_id'],
        outcome_status=status, next_inspection_date=next_inspection_date, workflow_events=len(events),
        refused_attempts=len(refusals), learning_stream_sha256=hashlib.sha256(stream_path.read_bytes()).hexdigest(),
        gateway_identity=config.identity, approval_authority=AUTHORITY, original_report_reproduced=True,
        business_separation=separation, ongoing_activation=False, promotion=False)
    walkthrough_receipt = e.seal(seal_root, dict(schema_version=e.OBSERVATION, run_id=seal_root.name, result=summary,
        definition_version='reporting-learning-walkthrough/v1', inputs=report_receipt['inputs'],
        query_hash=digest(dict(gateway=config.identity, events=[row['activity_id'] for row in events])),
        git_sha=revision['git_sha'], execution_hash=digest(pins), binding_identity=config.identity), ledger)
    seconds = perf_counter() - started
    manifest = dict(schema_version=SCHEMA, artifact_root=str(root), database=str(db), asof=ASOF, known_at=KNOWN_AT,
        customers=customers, generation=generated, baseline_inspection=baseline, events=events, refusals=refusals,
        outcome=outcome, evaluation=evaluation['result'], walkthrough_receipt_id=walkthrough_receipt['receipt_id'],
        walkthrough_run_id=seal_root.name, execution_artifacts_match_git=revision['execution_artifacts_match_git'],
        git_sha=revision['git_sha'], observed_workflow_seconds=seconds,
        timing_scope='Generation, report, workflow admission, evaluation, signing, measurement and original replay; excludes process startup',
        **summary)
    (root / 'walkthrough.json').write_text(canonical(manifest) + '\n', encoding='utf-8', newline='\n')
    (root / 'README.md').write_text(readme(manifest, root), encoding='utf-8', newline='\n')
    return manifest


def business_separation(db, expected_watermark):
    import duckdb
    with duckdb.connect(str(db), read_only=True) as conn:
        total, learning = conn.execute("SELECT count(*), count(*) FILTER (WHERE customer LIKE 'learning:%') FROM stream.activity").fetchone()
    if total != expected_watermark or learning:
        raise ValidationError('business stream changed or received learning rows during the walkthrough')
    return dict(business_rows=total, learning_rows_in_business_stream=learning, business_watermark_unchanged=True)


def readme(m, root):
    lines = ['# Reporting learning walkthrough (synthetic rehearsal)', '',
        'One synthetic business, one receipted July NRR report, and the complete governed feedback loop run through the same gateway, admission rules, candidate SQL, oracle and receipt engine as the native application. The warehouse is an in-memory stream and DuckDB. Nothing here is a real trial, a cloud execution or Chandler\'s approval.', '',
        '| Step | What happened | Receipt or record |', '|---|---|---|',
        f"| Report | July NRR, {m['baseline_inspection']['report_rows']} lenses dated {m['asof']} | `{m['report_receipt_id']}` |",
        f"| Wrong-date inspection | requested {m['baseline_inspection']['requested_date']}: {m['baseline_inspection']['selected_rows']} rows, no explanation | baseline.json |",
        f"| Finding and verification | one confirmed finding, one refuted claim kept visible | events.json |",
        f"| Candidate evaluation | {m['evaluation']['cases']} frozen cases, DuckDB candidate equals scalar oracle | `{m['evaluation_receipt_id']}` |",
        f"| Approval boundary | disposable synthetic signer; expanded, altered and misrouted approvals refused | plan.json, plan.json.sig |",
        f"| Next inspection | first inspection after authorization, requested {m['next_inspection_date']} | `{m['measurement_receipt_id']}` |",
        f"| Outcome | `{m['outcome_status']}`: {m['outcome']['interpretation']} | `{m['outcome_receipt_id']}` |",
        f"| Refusals | {m['refused_attempts']} refused attempts; stream unchanged after each | refusals.json |",
        f"| Preservation | original report reproduced; {m['business_separation']['learning_rows_in_business_stream']} learning rows in the business stream | walkthrough.json |",
        '',
        f"Workflow events admitted: {m['workflow_events']}. Observed wall time: {m['observed_workflow_seconds']:.3f} seconds (an observation, not a benchmark).", '',
        '## What the labels mean', '',
        f"- Approval authority: {m['approval_authority']}",
        f"- Outcome `improved` on a builder-chosen synthetic inspection means the candidate produced an explanation. It is not an efficiency measurement.",
        f"- The one real signed native trial ({m['outcome']['actual_native_trial']['signed_revision'][:7]}) completed **{m['outcome']['actual_native_trial']['status']}**: the operator requested {m['outcome']['actual_native_trial']['inspection_date']} and all four rows were selected. No promotion or ongoing activation occurred. Its private receipts are not reproduced here.",
        '', '## Replay', '', 'From the repository checkout that produced this directory:', '', '```sh',
        f"tl learning replay-walkthrough \"{root}\" --json",
        f"tl --db \"{root / 'world.duckdb'}\" --artifact-root \"{root}\" receipt {m['report_receipt_id']} --json",
        f"tl --artifact-root \"{root}\" receipt {m['evaluation_receipt_id']} --json",
        '```', '',
        'The first command rebuilds workflow state from the retained event stream, re-verifies the detached signature against the synthetic trust, re-performs the candidate and oracle, reproduces the original report and fails on any changed byte.']
    return '\n'.join(lines) + '\n'


def replay(root):
    """Offline re-validation of a walkthrough directory. Any changed byte fails."""
    from tl.bigquery.gateway import GatewayConfig
    from tl.views.engine import replay as replay_views
    root = Path(root).resolve()
    manifest = a.strict_json((root / 'walkthrough.json').read_bytes())
    if manifest.get('schema_version') != SCHEMA:
        raise ValidationError('not a reporting-learning walkthrough directory')
    ledger = root / 'metrics.jsonl'; outputs = root / 'runs'
    records = read_receipts(ledger)
    seal = records.get(manifest['walkthrough_receipt_id'])
    if seal is None or seal['run_id'] != manifest['walkthrough_run_id']:
        raise ValidationError('walkthrough receipt is missing from its ledger')
    seal_root = outputs / seal['run_id']
    e.verify_files(seal_root, seal)
    stream_path = root / 'learning-stream.parquet'
    if hashlib.sha256(stream_path.read_bytes()).hexdigest() != seal['result']['learning_stream_sha256']:
        raise ValidationError('learning event stream differs from its sealed digest')
    for name in ('gateway.json', 'plan.json', 'plan.json.sig'):
        if (root / name).read_bytes() != (seal_root / name).read_bytes():
            raise ValidationError('reader copy differs from its sealed original: ' + name)
    config = GatewayConfig.read(root / 'gateway.json')
    if not config.synthetic_rehearsal or config.identity != seal['result']['gateway_identity']:
        raise ValidationError('walkthrough gateway configuration differs or is not synthetic')
    rows = pq.read_table(stream_path).to_pylist()
    for row in rows:
        row['_recorded_at'] = timestamp(row['_recorded_at']); row['ts'] = timestamp(row['ts'])
    state = a.validate(rows, [], config)
    events = a.strict_json((seal_root / 'events.json').read_bytes())
    if [row['activity_id'] for row in rows] != [item['activity_id'] for item in events] or \
            any(row['_actor'] != item['actor'] or row['activity'] != item['activity'] for row, item in zip(rows, events)):
        raise ValidationError('retained workflow events differ from the sealed event list')
    completion = next(value for case, row, value in state.values() if row['activity'] == 'trial_completed')
    if completion['outcome_receipt_id'] != seal['result']['outcome_receipt_id'] or completion['status'] != seal['result']['outcome_status']:
        raise ValidationError('recorded outcome differs from the sealed summary')
    verified = [dict(receipt_id=seal['receipt_id'], verified=True, recomputed=False, kind='walkthrough_seal')]
    for key in (seal['result']['evaluation_receipt_id'], seal['result']['measurement_receipt_id']):
        verified.append(replay_evaluation(key, output_root=outputs, ledger=ledger, records=records))
    verified.extend(e.replay([seal['result']['outcome_receipt_id']], output_root=outputs, ledger=ledger))
    reproduced = replay_views([seal['result']['report_receipt_id']], db=root / 'world.duckdb', output_root=outputs, ledger=ledger)
    verified.extend(reproduced)
    separation = business_separation(root / 'world.duckdb', manifest['generation']['watermark'])
    return dict(status='verified', synthetic=True, native_execution=False, workflow_events=len(rows),
                signature_reverified=True, state_rebuilt=True, original_report_reproduced=True,
                business_separation=separation, results=verified)


def replay_evaluation(key, *, output_root, ledger, records=None):
    records = records or read_receipts(ledger)
    record = records.get(key)
    if record is None or record['schema_version'] != SCHEMA:
        raise ValidationError('unknown walkthrough evaluation receipt')
    root = Path(output_root) / record['run_id']
    e.verify_files(root, record)
    if record['execution_artifacts'] != e.artifacts():
        raise ValidationError('replay requires the retained learning execution revision')
    rows = [dict(row, month=str(row['month'])) for row in pq.read_table(root / 'report.parquet').to_pylist()]
    report_receipt = a.strict_json((root / 'report-receipt.json').read_bytes())
    if (records.get(report_receipt['receipt_id']) != report_receipt or report_receipt['inputs'] != record['inputs']
            or record['result']['report_receipt_id'] != report_receipt['receipt_id']):
        raise ValidationError('walkthrough evaluation is not bound to its retained report receipt')
    requests = a.strict_json((root / 'requests.json').read_bytes())
    actual = sorted(d.offline(rows, requests).to_pylist(), key=lambda row: row['case_id'])
    oracle = sorted([dict(case_id=case['case_id'], **d.oracle(d.scenario(rows, case.get('scenario', 'original')),
        case['requested_date'], case.get('population', 'nrr'))) for case in requests], key=lambda row: row['case_id'])
    if actual != record['result']['diagnostic_rows'] or oracle != record['result']['oracle_rows']:
        raise ValidationError('walkthrough diagnostic reproduction failed')
    return dict(receipt_id=key, verified=True, recomputed='diagnostic_from_retained_population', native_sql_reexecuted=False,
                business_recognition_recomputed=False, result=record['result'])
