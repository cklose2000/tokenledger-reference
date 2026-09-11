"""Bounded workflow transitions validated at the canonical append offset.

Worker attestations are authenticated claims, not independent approval. Only a
detached signature from the configured reviewer authorizes a one-run trial.
There is deliberately no activation or promotion transition.
"""
from datetime import timedelta
import hashlib
import json
import re
from pathlib import Path
import tempfile

from tl.learning.signatures import NAMESPACE, PRINCIPAL, verify
from tl.receipts.metrics import digest
from tl.stream import ValidationError
from tl.stream.events import canonical, timestamp

POLICY = 'reporting-learning/v1'
PLAN = 'tokenledger-reporting-trial/v1'
CANDIDATE = 'nrr-date-diagnostic/v1'
CATALOG = Path('definitions/reporting-learning/activities')
# First increment includes the fixed admission/evaluation harness. This is an
# explicit enabling scope, not a claim that the complete PR edits one SQL file.
# Subsequent candidates must receive a newly reviewed scope/version.
ENABLING_FILES = frozenset((
    'AGENTS.md', 'tl/cli.py', 'tl/metrics/engine.py', 'tl/learning/runner.py', 'tl/learning/signatures.py',
    'tl/bigquery/gateway.py', 'tl/bigquery/gateway_package.py', 'tl/bigquery/cli.py', 'tl/bigquery/iam.py',
    'tl/evolution/__init__.py', 'tl/evolution/admission.py', 'tl/evolution/cli.py',
    'tl/evolution/diagnostic.py', 'tl/evolution/evidence.py', 'tl/evolution/transport.py',
    'tl/evolution/trial.py', 'tl/evolution/github.py', 'tl/evolution/native_acceptance.py',
    'tl/evolution/sql/nrr_date_diagnostic_v1.sql',
    'definitions/reporting-learning/nrr-date-cases.v1.yaml',
    'docs/plans/bigquery-reporting-learning.md', 'docs/decisions/ADR-026-reporting-learning-on-bigquery.md',
    'docs/handoffs/bigquery-reporting-learning.md', 'ledger/process.jsonl',
    'tests/test_bigquery_gateway.py', 'tests/test_bigquery_gateway_package.py', 'tests/test_bigquery_iam.py',
    'tests/test_reporting_learning_admission.py', 'tests/test_reporting_learning_diagnostic.py',
    'tests/test_reporting_learning_transport.py', 'tests/test_reporting_learning_evidence.py',
    'tests/test_reporting_learning_trial.py', 'tests/test_reporting_learning_github.py',
))
ACTIVITIES = (
    'finding_submitted', 'finding_verified', 'candidate_proposed', 'candidate_evaluated',
    'trial_authorized', 'inspection_requested', 'trial_started', 'trial_completed', 'trial_revoked',
)
KEYS = {
    'finding_submitted': {'report_receipt_id', 'requested_date', 'description', 'evidence_sha256'},
    'finding_verified': {'finding_id', 'verdict', 'evidence_receipt_id', 'rationale'},
    'candidate_proposed': {'verification_id', 'candidate', 'candidate_sha256', 'execution_sha256',
                          'cases_sha256', 'pr_url', 'pr_head', 'changed_files'},
    'candidate_evaluated': {'proposal_id', 'verdict', 'evaluation_receipt_id', 'candidate_sha256',
                           'cases_sha256', 'target', 'evaluation_binding_identity'},
    'trial_authorized': {'plan_json', 'signature'},
    'inspection_requested': {'report_receipt_id', 'population', 'requested_date'},
    'trial_started': {'authorization_id', 'inspection_id', 'candidate_sha256', 'execution_sha256'},
    'trial_completed': {'trial_id', 'outcome_receipt_id', 'status', 'ongoing_activation'},
    'trial_revoked': {'authorization_id', 'reason'},
}


def _require(condition, message):
    if not condition:
        raise ValidationError('reporting learning admission: ' + message)


def strict_json(text):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            _require(key not in result, 'duplicate JSON key')
            result[key] = value
        return result
    try:
        return json.loads(text, object_pairs_hook=unique,
                          parse_constant=lambda _: _require(False, 'nonfinite JSON'))
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        raise ValidationError('reporting learning admission: invalid JSON') from exc


def envelope(case_id, record):
    return dict(case_id=case_id, record_json=canonical(record), record_sha256=digest(record))


def body(row):
    features = strict_json(row['feature_json']) if isinstance(row['feature_json'], str) else row['feature_json']
    _require(set(features) == {'case_id', 'record_json', 'record_sha256'}, 'closed event envelope required')
    value = strict_json(features['record_json'])
    _require(isinstance(value, dict) and canonical(value) == features['record_json']
             and digest(value) == features['record_sha256'], 'record content hash or canonical encoding differs')
    case_id = features['case_id']
    _require(isinstance(case_id, str) and re.fullmatch(r'[a-z][a-z0-9-]{2,79}', case_id), 'invalid case ID')
    _require(row['customer'] == 'learning:' + case_id and row['anonymous_customer_id'] is None
             and row['link'] is None and row['revenue_impact'] is None, 'learning identity is not financial activity')
    _require(row['activity'] in KEYS and set(value) == KEYS[row['activity']], 'closed workflow record required')
    # Make downstream checks total. Containers occur only in these two explicit places.
    for key, item in value.items():
        if key == 'changed_files':
            _require(isinstance(item, list) and item and all(isinstance(p, str) for p in item), 'changed files required')
        elif key == 'ongoing_activation':
            _require(item is False, 'ongoing activation is forbidden')
        else:
            _require(isinstance(item, str) and 0 < len(item) <= 32768, 'bounded nonempty strings required')
        if key.endswith('_sha256') or key.endswith('_identity'):
            _require(isinstance(item, str) and re.fullmatch(r'[a-f0-9]{64}', item), 'invalid content digest')
        if key.endswith('_receipt_id'):
            _require(re.fullmatch(r'mr-[a-f0-9]{32}', item), 'invalid receipt ID')
        if key == 'requested_date':
            from datetime import date
            try: parsed = date.fromisoformat(item)
            except ValueError: raise ValidationError('invalid reporting date') from None
            _require(parsed.isoformat() == item, 'use an ISO reporting date')
    return case_id, value


def _signed_plan(value, config):
    raw = value['plan_json']
    plan = strict_json(raw)
    expected = {'schema_version', 'case_id', 'gateway_identity', 'proposal_id', 'evaluation_id',
                'candidate', 'candidate_sha256', 'execution_sha256', 'cases_sha256', 'pr_head',
                'prepared_watermark', 'nonce', 'created_at', 'expires_at', 'reviewer_principal',
                'signature_namespace', 'trust_sha256', 'limits'}
    _require(isinstance(plan, dict) and set(plan) == expected and raw == canonical(plan) + '\n',
             'exact canonical plan bytes required')
    _require(plan['schema_version'] == PLAN and plan['gateway_identity'] == config.identity
             and plan['candidate'] == CANDIDATE and plan['reviewer_principal'] == PRINCIPAL
             and plan['signature_namespace'] == NAMESPACE
             and plan['trust_sha256'] == hashlib.sha256(config.learning_signers.encode()).hexdigest(),
             'plan does not match this deployment and reviewer')
    _require(canonical(plan['limits']) == canonical(dict(runs=1, diagnostic_only=True, source_writes=False,
                                  policy_changes=False, reporting_gate_changes=False,
                                  automatic_followup=False, ongoing_activation=False)), 'trial limits differ')
    _require(type(plan['prepared_watermark']) is int and plan['prepared_watermark'] >= 0
             and isinstance(plan['nonce'], str) and re.fullmatch(r'[a-f0-9]{32}', plan['nonce']), 'invalid trial anchor')
    created, expires = timestamp(plan['created_at']), timestamp(plan['expires_at'])
    _require(created < expires <= created + timedelta(days=30), 'trial validity exceeds one month')
    _require(value['signature'].startswith('-----BEGIN SSH SIGNATURE-----\n')
             and len(value['signature']) <= 8192, 'detached SSH signature required')
    with tempfile.TemporaryDirectory(prefix='tl-public-signature-') as directory:
        directory = Path(directory)
        # Public trust, exact plan, detached signature only. Never read a private key.
        (directory / 'plan').write_bytes(raw.encode())
        (directory / 'signature').write_bytes(value['signature'].encode())
        (directory / 'signers').write_bytes(config.learning_signers.encode())
        verify(directory / 'plan', directory / 'signature', directory / 'signers')
    return plan


def validate(prefix, pending, config):
    """Re-derive state from the prefix on every compare-and-append attempt.

    Historical events are checked at their own admission timestamps, so an
    expired approval remains replayable. New events are stamped by Gateway.
    """
    records, stages, nonces = {}, {}, set()
    previous_time = None
    for row in [*prefix, *pending]:
        at = timestamp(row['_recorded_at'])
        _require(previous_time is None or at >= previous_time, 'arrival clock moved behind canonical prefix')
        previous_time = at
        case_id, value = body(row)
        stage = stages.setdefault(case_id, {})
        kind = row['activity']
        key = row['activity_id']

        def ref(name, expected):
            record = records.get(value[name])
            _require(record is not None and record[0] == case_id and record[1]['activity'] == expected,
                     'missing, future or cross-case reference: ' + name)
            return record[1], record[2]

        def once():
            _require(kind not in stage, 'phase already recorded; retry identical event or open a new case')

        if kind == 'finding_submitted':
            once()
        elif kind == 'finding_verified':
            once(); source, _ = ref('finding_id', 'finding_submitted')
            _require(source['_actor'] != row['_actor'], 'finding author cannot attest its verification')
            _require(value['verdict'] in ('confirmed', 'refuted', 'unresolved'), 'invalid verification verdict')
        elif kind == 'candidate_proposed':
            once(); _, verification = ref('verification_id', 'finding_verified')
            _require(verification['verdict'] == 'confirmed', 'a confirmed failure is required')
            _require(value['candidate'] == CANDIDATE and re.fullmatch(r'[a-f0-9]{40}', value['pr_head'])
                     and re.fullmatch(r'https://github.com/cklose2000/tokenledger/pull/[1-9][0-9]*', value['pr_url']),
                     'pin the bounded candidate and private engine PR')
            _require(len(set(value['changed_files'])) == len(value['changed_files'])
                     and 'tl/evolution/sql/nrr_date_diagnostic_v1.sql' in value['changed_files']
                     and set(value['changed_files']) <= ENABLING_FILES | {
                         f'definitions/reporting-learning/activities/{name}.yaml' for name in ACTIVITIES},
                     'candidate diagnostic SQL and fixed enabling scope required')
        elif kind == 'candidate_evaluated':
            once(); _, proposal = ref('proposal_id', 'candidate_proposed')
            # A synthetic rehearsal gateway records DuckDB evaluations only; every
            # other gateway requires native BigQuery evaluation.
            target = 'duckdb' if config.synthetic_rehearsal else 'bigquery'
            _require(value['verdict'] in ('passed', 'failed') and value['target'] == target
                     and all(value[f] == proposal[f] for f in ('candidate_sha256', 'cases_sha256')),
                     'evaluation must name the native candidate and frozen cases')
        elif kind == 'trial_authorized':
            once(); plan = _signed_plan(value, config)
            proposal = records.get(plan['proposal_id'])
            evaluation = records.get(plan['evaluation_id'])
            _require(plan['case_id'] == case_id and proposal is not None and evaluation is not None
                     and proposal[0] == evaluation[0] == case_id
                     and proposal[1]['activity'] == 'candidate_proposed'
                     and evaluation[1]['activity'] == 'candidate_evaluated'
                     and evaluation[2]['proposal_id'] == plan['proposal_id']
                     and evaluation[2]['verdict'] == 'passed', 'passed native evaluation required')
            _require(all(plan[f] == proposal[2][f] for f in ('candidate', 'candidate_sha256', 'execution_sha256',
                                                           'cases_sha256', 'pr_head')), 'candidate pin differs')
            _require(plan['prepared_watermark'] < row['_stream_position'] and plan['nonce'] not in nonces
                     and timestamp(plan['created_at']) <= at <= timestamp(plan['expires_at']), 'expired or reused approval')
            nonces.add(plan['nonce'])
            value = {**value, '_plan': plan}
        elif kind == 'inspection_requested':
            _require('finding_submitted' in stage and value['population'] == 'nrr', 'unsupported inspection population')
        elif kind == 'trial_started':
            once(); authorization, authorization_value = ref('authorization_id', 'trial_authorized')
            inspection, _ = ref('inspection_id', 'inspection_requested')
            plan = authorization_value['_plan']
            eligible = [r for c, r, _ in records.values() if c == case_id and r['activity'] == 'inspection_requested'
                        and r['_stream_position'] > authorization['_stream_position']]
            _require(eligible and eligible[0]['activity_id'] == inspection['activity_id'], 'first next inspection must be used')
            _require('trial_revoked' not in stage and at <= timestamp(plan['expires_at'])
                     and value['candidate_sha256'] == plan['candidate_sha256']
                     and value['execution_sha256'] == plan['execution_sha256'], 'trial is revoked, expired or changed')
        elif kind == 'trial_completed':
            once(); trial, _ = ref('trial_id', 'trial_started')
            _require(trial['_actor'] == row['_actor'] and value['status'] in ('improved', 'inconclusive', 'failed')
                     and 'trial_revoked' not in stage, 'only the claimed runner can complete an unrevoked trial')
        elif kind == 'trial_revoked':
            once(); ref('authorization_id', 'trial_authorized')
            _require('trial_completed' not in stage, 'completed one-run evidence cannot be revoked retroactively')
        _require(key not in records, 'duplicate event ID')
        records[key] = (case_id, row, value)
        stage[kind] = key
    return records
