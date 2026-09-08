"""Explicitly close one disposable stream, then observe the stale gateway.

FinalizeWriteStream is irreversible. This harness requires a concrete opt-in,
retains intent before the one admin RPC, and never creates/replaces a stream.
An uncertain finalization is not retried and cannot pass the lifecycle gate.
"""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import os
import re
import time
import uuid

from tl.bigquery import boundary_evidence
from tl.bigquery.client import Client
from tl.bigquery.contract import validate_source_table
from tl.bigquery.denials import fixture_fingerprint, _error
from tl.bigquery.gateway_probe import _deployment_pin, identity_token, https_request
from tl.stream import ValidationError
from tl.stream.events import canonical


SCHEMA = 'tokenledger-bigquery-gateway-lifecycle/v1'
ADMIN_SCOPE = 'https://www.googleapis.com/auth/cloud-platform'


def _now(): return datetime.now(timezone.utc).isoformat()
def _sha(value): return hashlib.sha256(value).hexdigest()


class AdminFinalizer:
    """Admin authority is used only for one exact, explicitly authorized close."""
    def __init__(self, binding, *, sdk=None):
        if sdk is None:
            import google.auth
            from google.cloud import bigquery_storage_v1
            credential, _ = google.auth.default(scopes=[ADMIN_SCOPE], quota_project_id=binding.project)
            sdk = bigquery_storage_v1.BigQueryWriteClient(credentials=credential)
        self.sdk = sdk

    def metadata(self, name):
        return self.sdk.get_write_stream(name=name, retry=None, timeout=30)

    def finalize(self, name):
        return self.sdk.finalize_write_stream(name=name, retry=None, timeout=60).row_count


def _verified_deployment(config, url, deployment):
    pin = _deployment_pin(config, url, None, deployment)
    status = deployment.get('status') or {}
    if status:
        ready = any(row.get('type') == 'Ready' and row.get('status') in (True, 'True')
                    for row in status.get('conditions', []))
        revision = status.get('latestReadyRevisionName')
        traffic = status.get('traffic', [])
    else:
        ready = (deployment.get('terminalCondition') or {}).get('state') == 'CONDITION_SUCCEEDED'
        revision = deployment.get('latestReadyRevision')
        traffic = deployment.get('trafficStatuses', [])
    if not ready or not isinstance(revision, str) or not revision:
        raise ValidationError('lifecycle requires the independently observed Ready deployment revision')
    # Pin the observed revision served by the ordinary service URL. No tag,
    # redirect or alternate revision is silently created for the experiment.
    active = [row for row in traffic if row.get('percent', 0)]
    if (len(active) != 1 or active[0].get('percent') != 100
            or (active[0].get('revisionName') or active[0].get('revision', '')).split('/')[-1] != revision.split('/')[-1]):
        raise ValidationError('lifecycle requires observed 100 percent traffic to the pinned Ready revision')
    return dict(**pin, ready_revision=revision, deployment_observation_sha256=_sha(canonical(deployment).encode()))


def run(config, url, output, *, expected_source_sha256, authorize_finalize=False, deployment,
        producer_principal=None, observer=None, finalizer=None, token_factory=None, transport=None,
        ledger=None, artifacts=None, provenance=None, progress=None, run_id=None):
    """Seal one admin closure and one authenticated stale-revision refusal.

    Call only after append/concurrency and denial probes are complete. This
    command cannot restore append availability. A later rollover is a separately
    reviewed lifecycle change; it is never an automatic recovery path here.
    """
    if authorize_finalize is not True:
        raise ValidationError('explicit authorize_finalize=True is required for the irreversible pinned-stream close')
    if not re.fullmatch(r'tokenledger_iam_[a-z0-9_]{1,80}', config.binding.dataset):
        raise ValidationError('lifecycle requires an explicitly disposable tokenledger_iam_ fixture')
    if not isinstance(expected_source_sha256, str) or not re.fullmatch(r'[a-f0-9]{64}', expected_source_sha256):
        raise ValidationError('pin the exact original source SHA-256 before finalization')
    principal = producer_principal
    if principal is None and len(config.allowed_producers) == 1: principal = next(iter(config.allowed_producers))
    if principal not in config.allowed_producers:
        raise ValidationError('select one configured producer for the authenticated stale-revision request')
    pin = _verified_deployment(config, url, deployment)
    maximum_queries = 3 + config.max_attempts + 1
    if maximum_queries * config.binding.maximum_bytes_billed > config.binding.maximum_run_bytes_billed:
        raise ValidationError('lifecycle observations and gateway retries exceed the configured byte ceiling')
    run_id = run_id or uuid.uuid4().hex
    if not isinstance(run_id, str) or not re.fullmatch(r'[a-f0-9]{32}', run_id):
        raise ValidationError('lifecycle run ID must be a fresh UUID hex')
    output = Path(output).absolute()
    pending = output.with_name(output.name + '.pending')
    boundary_evidence._reject_links(output); boundary_evidence._reject_links(pending)
    if output.exists() or pending.exists(): raise ValidationError('lifecycle requires fresh output and pending paths')
    reserved = {'lifecycle/start.json', 'lifecycle/progress.jsonl', 'lifecycle/finalize-intent.json',
                'lifecycle/collected.json', 'lifecycle/execution-after.json'}
    if reserved.intersection(artifacts or {}): raise ValidationError('proof artifact uses a reserved lifecycle name')
    pending.mkdir(parents=True)
    frozen = provenance or dict(**boundary_evidence._execution(), pin_scope='lifecycle_before_execute_checked_after')
    result = dict(schema_version=SCHEMA, run_id=run_id, started_at=_now(), status='running',
        deployment=pin, gateway_identity=config.identity, binding_identity=config.binding.identity,
        expected_source_sha256=expected_source_sha256, original={}, snapshots=[], reader_jobs=[],
        finalization=dict(status='not_run', stream=config.write_stream, explicit_authorization=True,
                          admin_requested_oauth_scope=ADMIN_SCOPE, independently_attested_admin_identity=False),
        stale_request=dict(status='not_run', producer_principal=principal, audience=config.audience),
        source_unchanged=None, finalize_calls=0, http_requests=0, reader_queries=0,
        stream_replacement='not_attempted', automatic_cleanup=False, automatic_retries=False,
        production_acceptance='not_established', append_availability_after_run='not_established',
        scope='explicit disposable-stream finalization and one stale-revision request; not an expiry or rollover test')
    boundary_evidence._write(pending/'start.json', result, exclusive=True)
    events = pending/'progress.jsonl'; events.touch(exist_ok=False)
    def retain(value):
        with events.open('a', encoding='utf-8', newline='\n') as handle:
            handle.write(canonical(value) + '\n'); handle.flush(); os.fsync(handle.fileno())
        if progress: progress(value)

    def snapshot(stage):
        nonlocal observer
        if result['reader_queries'] >= 3: raise ValidationError('lifecycle reader query bound exhausted')
        observer = observer or Client(config.binding)
        source = observer.sdk.get_table(config.binding.table)
        validate_source_table(source)
        if str(source.to_api_repr().get('creationTime')) != config.table_creation_time:
            raise ValidationError('source incarnation differs from the immutable gateway pin')
        result['reader_queries'] += 1
        table, job = observer.query(f'SELECT * FROM `{config.binding.table}` ORDER BY _stream_position LIMIT 1001',
                                    label='gateway_lifecycle_source')
        result['reader_jobs'].append(job)
        identity = fixture_fingerprint(config.binding, table)
        for row in table.to_pylist():
            if (not row['activity_id'].startswith('synthetic-iam-') or row['_lane'] != 'dev'
                    or row['_schema_hash'] != config.catalog_hash
                    or row['_actor'] not in config.allowed_producers
                    or row['_source'] != config.allowed_producers[row['_actor']]):
                raise ValidationError('lifecycle source is not the configured synthetic live-ingestion fixture')
        observation = dict(stage=stage, inputs=identity, job=job, observed_at=_now(),
                           matches_original=identity['sha256'] == expected_source_sha256)
        result['snapshots'].append(observation)
        retain(dict(kind='source_observation', **observation))
        if stage == 'before_finalize': result['original'] = identity
        result['source_unchanged'] = all(row['matches_original'] for row in result['snapshots'])
        if not observation['matches_original']:
            raise ValidationError('source changed or differs from the exact independently supplied hash')
        return identity

    try:
        initial = snapshot('before_finalize')
        # Obtain the actual producer token before the irreversible admin call.
        # Token mint failure therefore leaves the stream open and unmodified.
        token = (token_factory or identity_token)(principal, config.audience)
        if not isinstance(token, str) or len(token) > 16_384 or len(token.split('.')) != 3:
            raise ValidationError('a complete in-memory producer ID token is required')
        finalizer = finalizer or AdminFinalizer(config.binding)
        metadata = finalizer.metadata(config.write_stream)
        if (metadata.name != config.write_stream or int(metadata.type_) != 1
                or metadata.location.lower() != config.binding.location.lower()):
            raise ValidationError('pinned stream metadata changed before explicit finalization')
        intent = dict(stream=config.write_stream, gateway_identity=config.identity,
                      source=initial, requested_at=_now(), action='FinalizeWriteStream', retry=False)
        boundary_evidence._write(pending/'finalize-intent.json', intent, exclusive=True)
        retain(dict(kind='finalization_intent', **intent))
        result['finalize_calls'] = 1
        result['finalization']['status'] = 'submitted_unknown'
        started = time.perf_counter()
        try:
            count = finalizer.finalize(config.write_stream)
        except Exception as exc:
            detail = _error(exc, 'finalize_write_stream')
            result['finalization'].update(status='failed_or_unknown', error=detail,
                                           wall_seconds=time.perf_counter() - started)
            retain(dict(kind='finalization_observation', **result['finalization']))
            snapshot('after_uncertain_finalize')
            result['status'] = 'failed' if detail['status'] in ('iam_denied', 'oauth_scope_denied') else 'uncertain'
            result['stale_request'].update(status='not_run', reason='finalization_not_acknowledged; no request or retry attempted')
        else:
            result['finalization'].update(status='acknowledged', row_count=count,
                                           wall_seconds=time.perf_counter() - started)
            result['append_availability_after_run'] = 'pinned_stream_finalized; no replacement attempted'
            retain(dict(kind='finalization_observation', **result['finalization']))
            snapshot('after_finalize')
            if type(count) is not int or count != initial['activity_count']:
                raise ValidationError('finalized stream row count differs from the pinned complete source prefix')
            event = dict(activity_id='synthetic-iam-' + run_id + '-closed-stream',
                ts='2026-01-01T00:00:00Z', customer='synthetic-iam-customer', anonymous_customer_id=None,
                activity='customer_created', revenue_impact=None, link=None,
                feature_json=dict(segment='enterprise', channel='direct', country='US', parent_customer=None))
            raw = (canonical(dict(events=[event])) + '\n').encode()
            result['http_requests'] = 1
            request = result['stale_request']
            request.update(status='submitted_unknown', request=dict(events=[event]),
                           request_sha256=_sha(raw), started_at=_now(), http_status=None)
            retain(dict(kind='stale_request_intent', **request))
            started = time.perf_counter()
            try:
                status, body = (transport or https_request)(pin['url'], raw, token)
                if type(status) is not int or not isinstance(body, bytes) or len(body) > 1_000_000:
                    raise ValidationError('invalid bounded HTTP response')
                text = body.decode('utf-8', errors='replace')
                redacted = token in text
                text = text.replace(token, '[REDACTED_ID_TOKEN]')
                try: response = json.loads(text)
                except json.JSONDecodeError: response = None
                request.update(http_status=status, response_sha256=_sha(body), response_text=text,
                               response=response, token_echo_redacted=redacted,
                               status='redirect_refused' if 300 <= status < 400 else 'response_received')
            except Exception as exc:
                request.update(status='failed_or_unknown', error_type=type(exc).__name__,
                               error_sha256=_sha(str(exc).encode()))
            request.update(finished_at=_now(), wall_seconds=time.perf_counter() - started)
            retain(dict(kind='stale_request_observation', **request))
            snapshot('after_stale_request')
            body = request.get('response')
            observation = body.get('observation') if isinstance(body, dict) else None
            refused = (request['status'] == 'response_received' and request['http_status'] == 503
                and not request.get('token_echo_redacted') and isinstance(body, dict)
                and body.get('status') == 'append_unverified' and isinstance(observation, dict)
                and observation.get('binding_identity') == config.identity
                and bool(observation.get('attempts'))
                and all(attempt.get('status') == 'submitted_unknown' and attempt.get('error_type')
                        for attempt in observation['attempts']))
            result.update(status='closed_stream_refusal_observed' if refused else 'failed',
                          authenticated_stale_revision_refusal=refused)
            if not refused:
                result['limitation'] = 'A transport, authentication or unrelated refusal cannot establish the registered lifecycle outcome.'
    except BaseException as exc:
        result.update(status='failed', partial_evidence=True,
            failure=dict(type=type(exc).__name__, message_sha256=_sha(str(exc).encode())))
    if observer is not None and hasattr(observer, 'jobs'):
        result['reader_jobs'] = list(observer.jobs)
    result['finished_at'] = _now()
    if provenance is None:
        after = boundary_evidence._execution()
        if after['execution_hash'] != frozen['execution_hash'] or after['git_sha'] != frozen['git_sha']:
            result.update(status='failed', collected_status=result['status'], execution_changed_during_run=True)
            boundary_evidence._write(pending/'execution-after.json', after, exclusive=True)
    boundary_evidence._write(pending/'collected.json', result, exclusive=True)
    proof = dict(artifacts or {})
    for name in ('start.json', 'progress.jsonl', 'finalize-intent.json', 'collected.json', 'execution-after.json'):
        if (pending/name).exists(): proof['lifecycle/' + name] = pending/name
    return boundary_evidence.record(result, output, binding=config.binding, artifacts=proof,
                                    ledger=ledger, provenance=frozen)
