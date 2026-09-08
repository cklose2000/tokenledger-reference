"""One bounded native HTTP acceptance experiment for a disposable gateway.

No request is retried by this harness. After every case, an independent reader
checks the actual source. Server responses alone do not establish append or
deduplication. Failed/partial runs remain sealed observations of that outcome.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit
import hashlib
import http.client
import json
import re
import ssl
import threading
import time
import uuid

from tl.bigquery import boundary_evidence
from tl.bigquery.client import Client
from tl.bigquery.contract import validate_source_table
from tl.bigquery.hashing import encode
from tl.bigquery.schema import FIELDS
from tl.stream import ValidationError
from tl.stream.events import canonical, timestamp


SCHEMA = 'tokenledger-bigquery-gateway-probe/v1'
CASES = ('no_identity', 'forged_identity', 'wrong_audience', 'valid_append', 'identical_retry',
         'conflicting_retry', 'forged_provenance', 'unknown_activity', 'concurrent_equal',
         'concurrent_distinct', 'concurrent_conflicting')
MAX_HTTP_REQUESTS = 14
MAX_READER_QUERIES = 12


def _now(): return datetime.now(timezone.utc).isoformat()
def _sha(raw): return hashlib.sha256(raw).hexdigest()


def _origin(value):
    if not isinstance(value, str): raise ValidationError('an explicit Cloud Run origin is required')
    parsed = urlsplit(value)
    if (parsed.scheme != 'https' or not parsed.hostname or not parsed.hostname.endswith('.run.app')
            or parsed.username or parsed.password or parsed.query or parsed.fragment
            or parsed.path not in ('', '/') or parsed.port not in (None, 443)):
        raise ValidationError('probe target must be an explicit HTTPS Cloud Run service origin')
    return 'https://' + parsed.hostname


def _deployment_pin(config, url, expected_service_url, deployment):
    target = _origin(url)
    if bool(expected_service_url) == bool(deployment):
        raise ValidationError('supply exactly one independent service URL pin or native deployment observation')
    name = None
    if deployment is not None:
        if not isinstance(deployment, dict): raise ValidationError('invalid native deployment observation')
        pin = deployment.get('uri') or (deployment.get('status') or {}).get('url')
        name = deployment.get('name') or (deployment.get('metadata') or {}).get('name')
        if not isinstance(name, str) or not name:
            raise ValidationError('deployment observation needs its service identity')
        if name.startswith('projects/') and not name.startswith(
                f'projects/{config.binding.project}/locations/{config.binding.location}/services/'):
            raise ValidationError('deployment observation belongs to another project or location')
    else:
        pin = expected_service_url
    if _origin(pin) != target:
        raise ValidationError('HTTP target differs from the independently pinned deployed service')
    return dict(url=target, service=name, authority='explicit_service_url' if deployment is None else 'native_deployment_observation',
                audience=config.audience, gateway_identity=config.identity)


def identity_token(principal, audience):
    """Mint an in-memory producer ID token. Never persist token/header material."""
    import google.auth
    from google.auth import impersonated_credentials
    from google.auth.transport.requests import Request
    source, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
    target = impersonated_credentials.Credentials(source_credentials=source, target_principal=principal,
        target_scopes=['https://www.googleapis.com/auth/cloud-platform'], lifetime=900)
    credential = impersonated_credentials.IDTokenCredentials(target_credentials=target,
        target_audience=audience, include_email=True)
    credential.refresh(Request())
    return credential.token


def https_request(url, body, token):
    """TLS-verified, no redirects, proxies, retries, or credential logging."""
    origin = _origin(url)
    connection = http.client.HTTPSConnection(urlsplit(origin).hostname, 443,
        timeout=120, context=ssl.create_default_context())
    headers = {'Content-Type': 'application/json', 'Content-Length': str(len(body))}
    if token is not None:
        headers['Authorization'] = 'Bearer ' + token
        headers['X-Serverless-Authorization'] = 'Bearer ' + token
    try:
        connection.request('POST', '/append', body=body, headers=headers)
        response = connection.getresponse()
        raw = response.read(1_000_001)
        if len(raw) > 1_000_000:
            raise ValidationError('gateway response exceeds the bounded evidence size')
        return response.status, raw
    finally:
        connection.close()


def _event(key, economic_ts, country='US'):
    return dict(activity_id=key, ts=economic_ts, customer='synthetic-iam-customer',
        anonymous_customer_id=None, activity='customer_created', revenue_impact=None, link=None,
        feature_json=dict(segment='enterprise', channel='direct', country=country, parent_customer=None))


def _identity_refusal(responses):
    if len(responses) != 1: return False
    response = responses[0]
    text = response.get('response_text', '').lower()
    body = response.get('response')
    return (response['http_status'] in (401, 403) and response['transport_status'] == 'response_received'
        and not response.get('token_echo_redacted') and not any(word in text for word in ('quota', 'rate limit'))
        and ((isinstance(body, dict) and body.get('status') == 'identity_denied')
             or any(word in text for word in ('unauthorized', 'unauthenticated', 'forbidden', 'permission'))))


def _normal(row):
    value = dict(row)
    for key in ('ts', '_recorded_at'):
        value[key] = timestamp(value[key]).isoformat(timespec='microseconds')
    if isinstance(value['feature_json'], str): value['feature_json'] = json.loads(value['feature_json'])
    if value['revenue_impact'] is not None: value['revenue_impact'] = format(value['revenue_impact'], '.2f')
    return value


def source_fingerprint(table):
    """Logical source hash retaining the live dev lane, never relabeling it sim."""
    hashed = []
    positions = []
    ids = []
    for row in table.to_pylist():
        if row['_lane'] != 'dev': raise ValidationError('gateway observation requires the actual live-ingestion lane')
        if isinstance(row['feature_json'], str): row['feature_json'] = json.loads(row['feature_json'])
        ids.append(row['activity_id']); positions.append(row['_stream_position'])
        hashed.append((canonical({'activity_id': encode(row['activity_id'])}),
                       _sha(canonical(encode(row)).encode())))
    if len(ids) != len(set(ids)): raise ValidationError('duplicate gateway source IDs')
    return dict(algorithm='cross-engine-logical-json/v1', sha256=_sha(''.join(sha for _, sha in sorted(hashed)).encode('ascii')),
                rows=len(ids), columns=sorted(table.column_names), activity_count=len(ids),
                activity_id_min=min(ids) if ids else None, activity_id_max=max(ids) if ids else None,
                input_row_range=[min(positions), max(positions)] if positions else [None, None],
                admitted_lane='dev')


class Probe:
    def __init__(self, config, pin, principal, *, observer, token_factory, transport, progress, run_id):
        self.config, self.pin, self.principal = config, pin, principal
        self.observer, self.token_factory, self.transport, self.progress = observer, token_factory, transport, progress
        self.run_id = run_id
        self.started_at = _now()
        self.tokens = set()
        self.http_count = 0
        self.query_count = 0
        self.lock = threading.Lock()
        self.cases = []
        self.snapshots = []
        self.reader_jobs = []
        self.current = None
        self.current_case = None
        self.missing = []

    def emit(self, observation):
        if self.progress: self.progress(observation)

    def snapshot(self):
        if self.query_count >= MAX_READER_QUERIES:
            raise ValidationError('bounded reader query count exhausted')
        self.observer = self.observer or Client(self.config.binding)
        table = self.observer.sdk.get_table(self.config.binding.table)
        validate_source_table(table)
        if str(table.to_api_repr().get('creationTime')) != self.config.table_creation_time:
            raise ValidationError('source incarnation differs from configured gateway')
        self.query_count += 1
        table, job = self.observer.query(
            f'SELECT * FROM `{self.config.binding.table}` ORDER BY _stream_position LIMIT 1001', label='gateway_probe_source')
        self.reader_jobs.append(job)
        if table.column_names != [f[0] for f in FIELDS] or len(table) > min(1000, self.config.max_prefix_rows):
            raise ValidationError('probe source exceeds its disposable fixture scope')
        rows = [_normal(row) for row in table.to_pylist()]
        ids = set()
        for position, row in enumerate(rows, 1):
            if (row['_stream_position'] != position or row['activity_id'] in ids
                    or not row['activity_id'].startswith('synthetic-iam-') or row['_lane'] != 'dev'
                    or row['_schema_hash'] != self.config.catalog_hash
                    or row['_actor'] not in self.config.allowed_producers
                    or row['_source'] != self.config.allowed_producers[row['_actor']]):
                raise ValidationError('source is not the explicitly synthetic, unique gateway fixture prefix')
            ids.add(row['activity_id'])
        identity = source_fingerprint(table)
        self.snapshots.append(dict(inputs=identity, job=job, observed_at=_now()))
        return rows

    def token(self, audience):
        value = self.token_factory(self.principal, audience)
        if not isinstance(value, str) or len(value) > 16_384 or len(value.split('.')) != 3:
            raise ValidationError('token factory did not return a complete in-memory ID token')
        self.tokens.add(value)
        return value

    def request(self, case, event, token):
        raw = (canonical(dict(events=[event])) + '\n').encode('utf-8')
        with self.lock:
            if self.http_count >= MAX_HTTP_REQUESTS:
                raise ValidationError('bounded HTTP request count exhausted')
            self.http_count += 1
            number = self.http_count
        started = time.perf_counter()
        record = dict(case=case, request_number=number, request_sha256=_sha(raw), request_bytes=len(raw),
                      request=dict(events=[event]), started_at=_now(), http_status=None)
        try:
            status, body = self.transport(self.pin['url'], raw, token)
            if type(status) is not int or not isinstance(body, bytes) or len(body) > 1_000_000:
                raise ValidationError('invalid bounded HTTP response')
            text = body.decode('utf-8', errors='replace')
            redacted = False
            for secret in self.tokens:
                if secret in text:
                    text = text.replace(secret, '[REDACTED_ID_TOKEN]'); redacted = True
            record.update(http_status=status, response_sha256=_sha(body), response_text=text, token_echo_redacted=redacted)
            try: record['response'] = json.loads(text)
            except json.JSONDecodeError: record['response'] = None
            if 300 <= status < 400:
                record['transport_status'] = 'redirect_refused'
            else:
                record['transport_status'] = 'response_received'
        except Exception as exc:
            record.update(transport_status='failed_or_unknown', error_type=type(exc).__name__,
                          error_sha256=_sha(str(exc).encode()))
        record.update(finished_at=_now(), wall_seconds=time.perf_counter() - started)
        self.emit(dict(kind='http_observation', **record))
        return record

    def check_source(self, after, alternatives):
        before = self.current
        # Original rows and provenance must remain byte-equivalent, including
        # the recorded time on retries. This check does not trust server counts.
        if after[:len(before)] != before:
            raise ValidationError('existing source prefix changed during an HTTP case')
        added = after[len(before):]
        matched = None
        for expected in alternatives:
            if len(added) != len(expected): continue
            expected = {event['activity_id']: event for event in expected}
            if {row['activity_id'] for row in added} != set(expected): continue
            good = True
            for row in added:
                event = expected[row['activity_id']]
                for key in event:
                    value = timestamp(event[key]).isoformat(timespec='microseconds') if key == 'ts' else event[key]
                    if row[key] != value: good = False
                if (row['_actor'] != self.principal
                        or row['_source'] != self.config.allowed_producers[self.principal]
                        or timestamp(row['_recorded_at']) < timestamp(self.started_at)
                        or timestamp(row['_recorded_at']) > datetime.now(timezone.utc)):
                    good = False
            if good:
                matched = expected
                break
        if matched is None:
            raise ValidationError('actual appended rows differ from the registered case alternatives')
        self.current = after

    def response_ok(self, response, accepted):
        if response['transport_status'] != 'response_received' or response.get('token_echo_redacted'):
            return False
        status, body = response['http_status'], response.get('response')
        if accepted:
            return (status == 200 and isinstance(body, dict) and body.get('status') == 'accepted'
                and body.get('binding_identity') == self.config.identity and body.get('accepted') == 1
                and type(body.get('accepted')) is int and type(body.get('attempted')) is int
                and body.get('attempted') == 1 and body.get('source_schema_columns') == 14)
        return status == 422 and isinstance(body, dict) and body.get('status') == 'validation_rejected'

    def case(self, name, requests, alternatives, expectation, *, concurrent=False):
        self.current_case = name
        self.emit(dict(kind='case_started', case=name, observed_at=_now()))
        if concurrent:
            barrier = threading.Barrier(2)
            def perform(pair):
                barrier.wait(timeout=10)
                return self.request(name, *pair)
            with ThreadPoolExecutor(max_workers=2) as executor:
                responses = list(executor.map(perform, requests))
        else:
            responses = [self.request(name, *requests[0])]
        # Observe even a timed-out request. It may have committed; never replace
        # its ID or make another request to manufacture a successful trial.
        case = dict(case=name, status='unverified', responses=responses,
                    source_prefix_preserved=False, source_rows_matched=False)
        try:
            after = self.snapshot()
            self.check_source(after, alternatives)
            passed = expectation(responses)
            case.update(status='observed_expected' if passed else 'unexpected',
                        source=self.snapshots[-1]['inputs'], source_prefix_preserved=True, source_rows_matched=True)
        finally:
            self.cases.append(case)
            self.emit(dict(kind='case_finished', **case))
        if not passed:
            raise ValidationError('unexpected HTTP case outcome; remaining requests were not sent')

    def execute(self):
        self.current = self.snapshot()
        if len(self.current) + 5 > min(1000, self.config.max_prefix_rows):
            raise ValidationError('fixture lacks room for the five registered new events')
        prefix = 'synthetic-iam-' + self.run_id + '-'
        # Time is real server ingestion; the fixture economic day is explicitly
        # synthetic, safely before this experiment in every deployment timezone.
        economic_ts = '2026-01-01T00:00:00Z'
        first = _event(prefix + 'first', economic_ts)
        self.case('no_identity', [(first, None)], [[]], _identity_refusal)
        self.case('forged_identity', [(first, 'forged.invalid.signature')], [[]], _identity_refusal)
        token = self.token(self.config.audience)
        try:
            wrong = self.token(self.config.audience.rstrip('/') + '/wrong-audience')
        except Exception as exc:
            self.missing.append(dict(case='wrong_audience', status='not_run', reason='token_mint_failed',
                                     error_type=type(exc).__name__, error_sha256=_sha(str(exc).encode())))
            self.emit(dict(kind='case_not_run', **self.missing[-1]))
        else:
            self.case('wrong_audience', [(first, wrong)], [[]], _identity_refusal)
        yes = lambda responses: len(responses) == 1 and self.response_ok(responses[0], True)
        no = lambda responses: len(responses) == 1 and self.response_ok(responses[0], False)
        self.case('valid_append', [(first, token)], [[first]], yes)
        self.case('identical_retry', [(first, token)], [[]], yes)
        changed = _event(first['activity_id'], economic_ts, 'CA')
        self.case('conflicting_retry', [(changed, token)], [[]], no)
        self.case('forged_provenance', [(dict(_event(prefix + 'forged', economic_ts), _actor='forged'), token)], [[]], no)
        unknown = _event(prefix + 'unknown', economic_ts); unknown['activity'] = 'unregistered_fixture_activity'
        self.case('unknown_activity', [(unknown, token)], [[]], no)
        equal = _event(prefix + 'equal', economic_ts)
        self.case('concurrent_equal', [(equal, token), (equal, token)], [[equal]],
                  lambda rs: all(self.response_ok(r, True) for r in rs), concurrent=True)
        distinct = [_event(prefix + 'distinct-' + str(i), economic_ts) for i in (1, 2)]
        self.case('concurrent_distinct', [(value, token) for value in distinct], [distinct],
                  lambda rs: all(self.response_ok(r, True) for r in rs), concurrent=True)
        rivals = [_event(prefix + 'rival', economic_ts, country) for country in ('US', 'CA')]
        self.case('concurrent_conflicting', [(value, token) for value in rivals], [[rivals[0]], [rivals[1]]],
                  lambda rs: sum(self.response_ok(r, True) for r in rs) == 1
                      and sum(self.response_ok(r, False) for r in rs) == 1, concurrent=True)
        return dict(status='partial' if self.missing else 'gateway_cases_observed')

    def result(self):
        try:
            outcome = self.execute()
        except BaseException as exc:
            outcome = dict(status='failed', failure=dict(case=self.current_case, type=type(exc).__name__,
                           message_sha256=_sha(str(exc).encode())), partial_evidence=True)
        completed = {case['case'] for case in self.cases}
        missing = {case['case'] for case in self.missing}
        statuses = {case['case']: case['status'] for case in self.cases}
        return dict(schema_version=SCHEMA, run_id=self.run_id, started_at=self.started_at, finished_at=_now(),
            binding_identity=self.config.binding.identity, gateway_identity=self.config.identity,
            deployment=self.pin, producer_principal=self.principal,
            **outcome, cases=self.cases, snapshots=self.snapshots, missing=self.missing,
            reader_jobs=list(getattr(self.observer, 'jobs', self.reader_jobs)),
            not_run=[name for name in CASES if name not in completed | missing],
            original=self.snapshots[0]['inputs'] if self.snapshots else {},
            final=self.snapshots[-1]['inputs'] if self.snapshots else {},
            http_requests=self.http_count, reader_queries=self.query_count, max_http_requests=MAX_HTTP_REQUESTS,
            authenticated_append='observed' if statuses.get('valid_append') == 'observed_expected' else 'not_established',
            authentication_refusals='observed' if all(statuses.get(name) == 'observed_expected'
                for name in ('no_identity', 'forged_identity', 'wrong_audience')) else 'not_established',
            concurrency='observed' if all(statuses.get(name) == 'observed_expected'
                for name in ('concurrent_equal', 'concurrent_distinct', 'concurrent_conflicting')) else 'not_established',
            ongoing_activation=False, production_acceptance='not_established', no_automatic_http_retries=True,
            scope='one disposable synthetic fixture; HTTP identity, content and two-worker protocol observations')


def run(config, url, output, *, producer_principal=None, deployment=None, expected_service_url=None,
        ledger=None, artifacts=None, provenance=None, observer=None, token_factory=None, transport=None,
        progress=None, run_id=None):
    """Execute once, retain progress durably, seal the observed final outcome.

    The sibling .pending directory is deliberately retained even on failure. Its
    presence prevents accidental replacement/reuse of an interrupted run path.
    No token, Authorization header, or credential file is an evidence input.
    """
    if not re.fullmatch(r'tokenledger_iam_[a-z0-9_]{1,80}', config.binding.dataset):
        raise ValidationError('gateway probe requires an explicitly disposable tokenledger_iam_ source')
    principal = producer_principal
    if principal is None and len(config.allowed_producers) == 1: principal = next(iter(config.allowed_producers))
    if principal not in config.allowed_producers:
        raise ValidationError('select exactly one configured producer principal')
    pin = _deployment_pin(config, url, expected_service_url, deployment)
    maximum_queries = MAX_READER_QUERIES + MAX_HTTP_REQUESTS * (config.max_attempts + 1)
    if maximum_queries * config.binding.maximum_bytes_billed > config.binding.maximum_run_bytes_billed:
        raise ValidationError('probe observer and gateway requests exceed the configured aggregate byte ceiling')
    run_id = run_id or uuid.uuid4().hex
    if not re.fullmatch(r'[0-9a-f]{32}', run_id): raise ValidationError('probe run ID must be a fresh UUID hex')
    output = Path(output).absolute()
    pending = output.with_name(output.name + '.pending')
    boundary_evidence._reject_links(output); boundary_evidence._reject_links(pending)
    if output.exists() or pending.exists(): raise ValidationError('probe requires fresh output and pending paths')
    reserved_artifacts = {'gateway/start.json', 'gateway/progress.jsonl', 'gateway/collected.json', 'gateway/execution-after.json'}
    if reserved_artifacts.intersection(artifacts or {}):
        raise ValidationError('caller proof artifact uses a reserved gateway evidence name')
    pending.mkdir(parents=True)
    frozen = provenance or dict(**boundary_evidence._execution(), pin_scope='probe_before_execute_checked_after')
    boundary_evidence._write(pending/'start.json', dict(run_id=run_id, started_at=_now(), deployment=pin,
                                                     status='collecting', gateway=config.as_dict()), exclusive=True)
    events = pending/'progress.jsonl'; events.touch(exist_ok=False)
    lock = threading.Lock()
    def retain(value):
        import os
        with lock:
            with events.open('a', encoding='utf-8', newline='\n') as handle:
                handle.write(canonical(value) + '\n'); handle.flush(); os.fsync(handle.fileno())
            if progress: progress(value)
    probe = Probe(config, pin, principal, observer=observer,
        token_factory=token_factory or identity_token, transport=transport or https_request,
        progress=retain, run_id=run_id)
    result = probe.result()
    if provenance is None:
        after = boundary_evidence._execution()
        if after['execution_hash'] != frozen['execution_hash'] or after['git_sha'] != frozen['git_sha']:
            result.update(status='failed', collected_status=result['status'], execution_changed_during_run=True)
            boundary_evidence._write(pending/'execution-after.json', after, exclusive=True)
    boundary_evidence._write(pending/'collected.json', result, exclusive=True)
    proof = {**(artifacts or {}), 'gateway/start.json': pending/'start.json',
             'gateway/progress.jsonl': events, 'gateway/collected.json': pending/'collected.json'}
    if (pending/'execution-after.json').exists(): proof['gateway/execution-after.json'] = pending/'execution-after.json'
    return boundary_evidence.record(result, output, binding=config.binding, artifacts=proof,
                                    ledger=ledger, provenance=frozen)
