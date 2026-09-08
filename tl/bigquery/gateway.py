"""A bounded authenticated append endpoint, not a general warehouse proxy.

An administrator pins one empty table and one COMMITTED Storage Write stream.
Every worker reads the canonical prefix and competes at that stream's offset.
An offset collision is never treated as proof that *these* events were stored.
The subsequent exact prefix read decides that. There is no local global lock,
stream creation, rollover, SQL input, or caller-controlled provenance here.

This first boundary deliberately caps the prefix. It is a control acceptance
workload, not the 50k replication transport or an unbounded ingestion service.
"""
from dataclasses import dataclass
from collections.abc import Mapping
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import MappingProxyType
from urllib.parse import urlsplit
import hashlib
import json
import re
import time

import pyarrow as pa

from tl.bigquery.client import Client
from tl.bigquery.config import Binding
from tl.bigquery.contract import validate_source_table
from tl.bigquery.schema import FIELDS
from tl.bigquery.writer import proto_contract, serialized_rows
from tl.stream import Activity, ValidationError
from tl.stream.events import Catalog, canonical, money, timestamp
from tl.stream.store import prepare_row


CORE = frozenset(name for name, _, _ in FIELDS[:8])
REQUIRED = frozenset(('activity_id', 'ts', 'activity', 'feature_json'))


class AuthenticationError(ValidationError):
    """No verified allowed principal. No warehouse operation is permissible."""


class AppendUncertain(ValidationError):
    """No exact acceptance was established within the bound; retry same events."""

    def __init__(self, observation):
        super().__init__('append acceptance remains unknown; retry the identical event IDs and content')
        self.observation = observation


@dataclass(frozen=True)
class GatewayConfig:
    binding: Binding
    audience: str
    table_creation_time: str
    write_stream: str
    catalog_hash: str
    allowed_producers: dict
    base_position: int = 0
    max_events: int = 100
    max_request_bytes: int = 1_000_000
    max_prefix_rows: int = 10_000
    max_prefix_bytes: int = 16_000_000
    max_attempts: int = 3

    def __post_init__(self):
        if not isinstance(self.binding, Binding) or not self.binding.query_principal:
            raise ValidationError('gateway requires separate explicit reader and writer principals')
        url = urlsplit(self.audience)
        if (url.scheme != 'https' or not url.hostname or url.username or url.password
                or url.query or url.fragment or url.path not in ('', '/')):
            raise ValidationError('gateway audience must be an explicit HTTPS service origin')
        if not isinstance(self.table_creation_time, str) or not re.fullmatch(r'[1-9][0-9]*', self.table_creation_time):
            raise ValidationError('pin the source table REST creationTime in milliseconds')
        parent = f'projects/{self.binding.project}/datasets/{self.binding.dataset}/tables/activity/streams/'
        if (not isinstance(self.write_stream, str) or not self.write_stream.startswith(parent)
                or not re.fullmatch(r'[A-Za-z0-9]+', self.write_stream[len(parent):])):
            raise ValidationError('pin an application-created stream on the exact bound source table')
        if not isinstance(self.catalog_hash, str) or not re.fullmatch(r'[a-f0-9]{64}', self.catalog_hash):
            raise ValidationError('pin the activity catalog digest')
        if type(self.base_position) is not int or self.base_position != 0:
            raise ValidationError('this release requires a stream provisioned on an empty source; rollover is not implemented')
        for name, low, high in (('max_events', 1, 1000), ('max_request_bytes', 256, 4_000_000),
                                ('max_prefix_rows', 1, 100_000), ('max_prefix_bytes', 4096, 128_000_000),
                                ('max_attempts', 1, 5)):
            value = getattr(self, name)
            if type(value) is not int or not low <= value <= high:
                raise ValidationError('invalid bounded gateway setting: ' + name)
        if not isinstance(self.allowed_producers, Mapping) or not 1 <= len(self.allowed_producers) <= 20:
            raise ValidationError('pin a bounded producer to source allowlist')
        producers = dict(self.allowed_producers)
        for actor, source in producers.items():
            if (not isinstance(actor, str) or not re.fullmatch(r'[a-z][a-z0-9-]{4,28}[a-z0-9]@[a-z][a-z0-9-]+\.iam\.gserviceaccount\.com', actor)
                    or actor in (self.binding.query_principal, self.binding.writer_principal)
                    or not isinstance(source, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9./_-]{0,127}', source)):
                raise ValidationError('invalid producer identity or fixed source')
        object.__setattr__(self, 'allowed_producers', MappingProxyType(producers))

    def as_dict(self):
        from dataclasses import asdict
        result = {name: getattr(self, name) for name in self.__dataclass_fields__}
        result['binding'] = asdict(self.binding)
        result['allowed_producers'] = dict(self.allowed_producers)
        return result

    @property
    def identity(self):
        return hashlib.sha256(canonical(self.as_dict()).encode()).hexdigest()

    def save(self, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('x', encoding='utf-8', newline='\n') as handle:
            handle.write(canonical(self.as_dict()) + '\n')

    @classmethod
    def read(cls, path):
        try:
            raw = _json(Path(path).read_bytes())
            raw['binding'] = Binding(**raw['binding'])
            return cls(**raw)
        except (KeyError, TypeError) as exc:
            raise ValidationError('invalid immutable gateway configuration') from exc


def _json(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValidationError('duplicate JSON member')
            result[key] = value
        return result

    def nonfinite(_):
        raise ValidationError('nonfinite JSON number')

    try:
        value = json.loads(raw, object_pairs_hook=unique, parse_constant=nonfinite)
        canonical(value).encode('utf-8')  # Reject escaped, unpaired surrogates too.
        return value
    except (UnicodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ValidationError('invalid UTF-8 JSON request') from exc


def authenticate(header, config, *, verifier=None):
    """Verify the signed Authorization token, never a decoded proxy identity.

    On Cloud Run callers can send the same token in Authorization and
    X-Serverless-Authorization. The platform checks the latter; this application
    verifies the former independently, including the exact audience.
    """
    if not isinstance(header, str) or not header.startswith('Bearer ') or len(header) > 16_384:
        raise AuthenticationError('a signed bearer identity is required')
    token = header[7:]
    if len(token.split('.')) != 3 or not all(token.split('.')) or any(c.isspace() for c in token):
        raise AuthenticationError('a complete signed bearer identity is required')
    if verifier is None:
        from google.auth.transport.requests import Request
        from google.oauth2.id_token import verify_oauth2_token
        request = Request()
        verifier = lambda value, audience: verify_oauth2_token(value, request, audience=audience)
    try:
        claims = verifier(token, config.audience)
        if (claims.get('aud') != config.audience or claims.get('email_verified') is not True
                or claims.get('iss') not in ('accounts.google.com', 'https://accounts.google.com')
                or not isinstance(claims.get('sub'), str) or not claims['sub']
                or claims.get('email') not in config.allowed_producers):
            raise ValueError('identity does not match configured producer')
        return claims['email']
    except Exception as exc:
        # Neither bearer material nor upstream error details enter responses/logs.
        raise AuthenticationError('signed identity is not an allowed producer') from exc


def _equivalent(left, right):
    return all(left[name] == right[name] for name, _, _ in FIELDS
               if name not in ('_stream_position', '_recorded_at'))


def _prefix_size_bound(rows):
    """Conservative JSON wire bound including timestamp/type formatting room."""
    total = 0
    for row in rows:
        values = dict(row)
        values['feature_json'] = _json(values['feature_json'])
        for key in ('ts', '_recorded_at'):
            values[key] = timestamp(values[key]).isoformat(timespec='microseconds')
        # ASCII escapes upper-bound UTF-8 characters; nullable NUMERIC is text
        # here, whose quotes also upper-bound its native JSON numeric form.
        total += len(json.dumps(values, ensure_ascii=True, allow_nan=False).encode('ascii')) + 1024
    return total


class StorageAppender:
    """One request/connection; pinned offsets remain the distributed arbiter."""
    def __init__(self, binding, *, sdk=None, metadata_sdk=None):
        from google.cloud import bigquery_storage_v1
        from tl.bigquery.auth import credentials
        self.sdk = sdk or bigquery_storage_v1.BigQueryWriteClient(credentials=credentials(binding, 'writer'))
        self.binding = binding
        self.metadata_sdk = metadata_sdk
        self.cleanup_errors = []

    def metadata(self, name):
        # The writer's append-only OAuth scope need not authorize metadata reads.
        # IAM permissions and OAuth scopes are independent, explicit boundaries.
        if self.metadata_sdk is None:
            from google.cloud import bigquery_storage_v1
            from tl.bigquery.auth import credentials
            self.metadata_sdk = bigquery_storage_v1.BigQueryWriteClient(credentials=credentials(self.binding, 'query'))
        return self.metadata_sdk.get_write_stream(name=name)

    def append(self, name, offset, rows):
        from google.cloud.bigquery_storage_v1 import types
        from google.api_core import gapic_v1
        from google.api_core.exceptions import from_grpc_status
        descriptor, message_type = proto_contract()
        serialized = list(serialized_rows(pa.Table.from_pylist(rows), message_type))
        request = types.AppendRowsRequest(write_stream=name, offset=offset,
            proto_rows=types.AppendRowsRequest.ProtoData(
                writer_schema=types.ProtoSchema(proto_descriptor=descriptor),
                rows=types.ProtoRows(serialized_rows=serialized)))
        if request._pb.ByteSize() > 18_000_000:
            raise ValidationError('encoded append exceeds bounded transport size')
        responses = None
        try:
            # One bounded RPC retains the actual server error. The asynchronous
            # stream manager can mask it with StreamClosedError during shutdown.
            # Retry belongs solely to Gateway's exact-prefix/CAS loop above it.
            responses = self.sdk.append_rows(requests=iter([request]), retry=None, timeout=60,
                metadata=(gapic_v1.routing_header.to_grpc_metadata((('write_stream', name),)),))
            response = next(iter(responses))
            if response.error.code:
                raise from_grpc_status(response.error.code, response.error.message)
            if not response._pb.HasField('append_result'):
                raise ValidationError('storage response did not acknowledge an append')
            if not response.append_result._pb.HasField('offset') or response.append_result.offset != offset:
                raise ValidationError('storage acknowledged an unexpected or absent offset')
        finally:
            # Cancel this RPC, not the reused SDK transport. Cleanup cannot turn
            # a server refusal into another error, or hide an acknowledgment.
            cleanup = getattr(responses, 'cancel', None) or getattr(responses, 'close', None)
            if cleanup is not None:
                try: cleanup()
                except Exception as exc:
                    self.cleanup_errors.append(dict(stage='append_rpc_cleanup', error_type=type(exc).__name__))


class Gateway:
    def __init__(self, config, definitions, *, client=None, storage=None, now=None, sleep=None):
        self.config = config
        self.catalog = Catalog(Path(definitions))
        if self.catalog.digest != config.catalog_hash:
            raise ValidationError('gateway activity catalog differs from the configured pin')
        self.client = client
        self.storage = storage
        self.now = now or (lambda: datetime.now(timezone.utc))
        self.sleep = sleep or time.sleep

    def _ready(self):
        # Called only after authentication, complete request validation and bounds.
        self.client = self.client or Client(self.config.binding)
        self.storage = self.storage or StorageAppender(self.config.binding)
        table = self.client.sdk.get_table(self.config.binding.table)
        validate_source_table(table)
        if table.to_api_repr().get('creationTime') != self.config.table_creation_time:
            raise ValidationError('source table identity changed; no append or automatic recovery')
        stream = self.storage.metadata(self.config.write_stream)
        # COMMITTED is enum value 1. A missing/expired/finalized stream never
        # causes CreateWriteStream, FinalizeWriteStream or default-stream writes.
        if (stream.name != self.config.write_stream or int(stream.type_) != 1
                or (getattr(stream, 'location', '') or '').lower() != self.config.binding.location.lower()):
            raise ValidationError('pinned write stream identity/type/location changed')

    def _prefix(self):
        self._ready()
        table, _ = self.client.query(
            # Gate before downloading. A row-count bound alone does not bound
            # large JSON payloads. ERROR is in IF's selected branch, not an
            # unordered conjunction the optimizer might evaluate unconditionally.
            f'SELECT bounded.* FROM (SELECT * FROM `{self.config.binding.table}` '
            f'ORDER BY _stream_position LIMIT {self.config.max_prefix_rows + 1}) AS bounded '
            f'QUALIFY IF(SUM(BYTE_LENGTH(TO_JSON_STRING(bounded))) OVER () <= {self.config.max_prefix_bytes}, '
            "TRUE, ERROR('gateway prefix byte bound exceeded')) ORDER BY _stream_position",
            label='gateway_prefix')
        if (table.column_names != [f[0] for f in FIELDS] or len(table) > self.config.max_prefix_rows
                or table.nbytes > self.config.max_prefix_bytes):
            raise ValidationError('source exceeds the bounded exact prefix contract')
        rows = {}
        for position, actual in enumerate(table.to_pylist(), 1):
            if (type(actual['_stream_position']) is not int or actual['_stream_position'] != position
                    or actual['activity_id'] in rows or actual['_lane'] != 'dev'
                    or actual['_schema_hash'] != self.catalog.digest
                    or actual['_actor'] not in self.config.allowed_producers
                    or actual['_source'] != self.config.allowed_producers[actual['_actor']]):
                raise ValidationError('source is not the unique validated gateway prefix')
            event = Activity(**{key: (_json(actual[key]) if key == 'feature_json' and isinstance(actual[key], str)
                                     else actual[key]) for key in CORE})
            expected = prepare_row(event, actual['_recorded_at'], self.catalog,
                                   source=actual['_source'], actor=actual['_actor'], lane='dev')
            # BigQuery may reformat native JSON and NUMERIC representations.
            actual = dict(actual)
            actual['feature_json'] = canonical(_json(actual['feature_json']) if isinstance(actual['feature_json'], str)
                                               else actual['feature_json'])
            if actual['revenue_impact'] is not None:
                actual['revenue_impact'] = format(money(actual['revenue_impact']), '.2f')
            if any(actual[key] != value for key, value in expected.items()):
                raise ValidationError('noncanonical content in gateway source')
            rows[actual['activity_id']] = actual
        return rows

    def append(self, raw, principal):
        """Trusted in-process entry. HTTP callers must pass authenticate() first."""
        if principal not in self.config.allowed_producers:
            raise AuthenticationError('principal is not a configured producer')
        if not isinstance(raw, bytes) or not 1 <= len(raw) <= self.config.max_request_bytes:
            raise ValidationError('request byte bound exceeded')
        request = _json(raw)
        if (not isinstance(request, dict) or set(request) != {'events'}
                or not isinstance(request['events'], list)
                or not 1 <= len(request['events']) <= self.config.max_events):
            raise ValidationError('request must contain only a bounded nonempty events array')
        now = timestamp(self.now())
        incoming = {}
        for event in request['events']:
            if not isinstance(event, dict) or not REQUIRED <= event.keys() or event.keys() - CORE:
                raise ValidationError('events accept only ActivitySchema core fields, never provenance or CDC')
            row = prepare_row(Activity(**event), now, self.catalog,
                              source=self.config.allowed_producers[principal], actor=principal, lane='dev')
            if row['activity_id'] in incoming and row != incoming[row['activity_id']]:
                raise ValidationError('conflicting idempotency key in request')
            incoming[row['activity_id']] = row
        # Validation of the whole batch above precedes all cloud calls.
        attempts = []
        initial_present = None
        acknowledged = 0
        uncertain = False
        def unresolved(reason):
            return AppendUncertain(dict(attempts=attempts, status='unverified', reason=reason,
                                        binding_identity=self.config.identity, jobs=list(self.client.jobs)))
        for attempt in range(self.config.max_attempts + 1):
            try:
                existing = self._prefix()
            except ValidationError:
                if attempts:
                    raise unresolved('source_recheck_failed') from None
                raise
            except Exception:
                if not attempts:
                    raise ValidationError('pinned source or stream is unavailable; no automatic stream replacement') from None
                raise unresolved('source_recheck_unavailable') from None
            pending = []
            for activity_id, row in sorted(incoming.items()):
                previous = existing.get(activity_id)
                if previous is not None and not _equivalent(previous, row):
                    raise ValidationError('conflicting idempotency key in source')
                if previous is None:
                    pending.append(dict(row, _stream_position=len(existing) + len(pending) + 1))
            if initial_present is None:
                initial_present = len(incoming) - len(pending)
            if not pending:
                return dict(status='accepted', accepted=len(incoming), attempted=len(request['events']),
                            duplicates_at_start=initial_present, acknowledged_appended=acknowledged,
                            reconciled_after_uncertain=uncertain, watermark=len(existing),
                            binding_identity=self.config.identity, append_attempts=attempts,
                            jobs=list(self.client.jobs), source_schema_columns=14)
            if len(existing) + len(pending) > self.config.max_prefix_rows:
                raise ValidationError('append would exceed the bounded gateway prefix')
            if _prefix_size_bound([*existing.values(), *pending]) > self.config.max_prefix_bytes:
                raise ValidationError('append would exceed the conservative gateway prefix byte bound')
            if attempt == self.config.max_attempts:
                raise unresolved('retry_bound_exhausted')
            offset = len(existing) - self.config.base_position
            detail = dict(offset=offset, rows=len(pending), status='submitted_unknown')
            attempts.append(detail)
            cleanup_start = len(getattr(self.storage, 'cleanup_errors', []))
            try:
                self.storage.append(self.config.write_stream, offset, pending)
                detail['status'] = 'acknowledged'
                acknowledged += len(pending)
            except Exception as exc:
                # Includes ALREADY_EXISTS, competing connection, deadline and
                # response loss. Never assume payload equality from the code.
                detail['error_type'] = type(exc).__name__
                uncertain = True
            finally:
                cleanup = getattr(self.storage, 'cleanup_errors', [])[cleanup_start:]
                if cleanup: detail['cleanup_errors'] = list(cleanup)
            # Re-read after *every* append, including apparent success. The only
            # success response is backed by exact committed content comparison.
            if uncertain:
                self.sleep(min(0.25 * (attempt + 1), 1.0))
        raise AssertionError('unreachable bounded append loop')


def make_handler(config, definitions, *, gateway_factory=Gateway, verifier=None):
    class Handler(BaseHTTPRequestHandler):
        server_version = 'tokenledger-append'

        def log_message(self, format, *args):
            pass  # No request URLs, headers, tokens, events or data in logs.

        def _reply(self, code, value):
            data = (canonical(value) + '\n').encode()
            self.send_response(code)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(data)))
            self.send_header('Cache-Control', 'no-store')
            self.send_header('Connection', 'close')
            self.end_headers()
            self.wfile.write(data)
            self.close_connection = True

        def do_POST(self):
            if self.path != '/append':
                return self._reply(404, dict(status='not_found'))
            try:
                headers = self.headers.get_all('Authorization', [])
                if len(headers) != 1:
                    raise AuthenticationError('one signed identity is required')
                principal = authenticate(headers[0], config, verifier=verifier)
                lengths = self.headers.get_all('Content-Length', [])
                if (len(lengths) != 1 or not re.fullmatch(r'[1-9][0-9]*', lengths[0])
                        or int(lengths[0]) > config.max_request_bytes
                        or self.headers.get('Transfer-Encoding') is not None
                        or self.headers.get('Content-Encoding') is not None
                        or self.headers.get('Content-Type', '').lower() not in ('application/json', 'application/json; charset=utf-8')):
                    raise ValidationError('invalid bounded JSON transport')
                self.connection.settimeout(15)
                size = int(lengths[0])
                body = self.rfile.read(size)
                if len(body) != size:
                    raise ValidationError('incomplete JSON body')
                result = gateway_factory(config, definitions).append(body, principal)
                return self._reply(200, result)
            except AuthenticationError:
                return self._reply(403, dict(status='identity_denied'))
            except AppendUncertain as exc:
                return self._reply(503, dict(status='append_unverified', observation=exc.observation,
                                            retry='identical_event_ids_and_content'))
            except ValidationError:
                return self._reply(422, dict(status='validation_rejected'))
            except Exception:
                return self._reply(503, dict(status='unavailable', retry='identical_event_ids_and_content'))

        def do_GET(self):
            self._reply(405, dict(status='method_not_allowed'))

    return Handler


def serve(config, definitions='definitions/activities', *, host='0.0.0.0', port=8080):
    """Called by the noninteractive tl cloud bigquery gateway serve command."""
    # Fail before accepting traffic if the deployed catalog pin differs.
    Gateway(config, definitions)
    server = ThreadingHTTPServer((host, port), make_handler(config, definitions))
    server.daemon_threads = True
    try:
        server.serve_forever()
    finally:
        server.server_close()
