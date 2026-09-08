"""Native denial probes for an explicitly disposable synthetic source fixture.

This is evidence collection, not an IAM provisioning or production certification
API. All attempted mutations use the supplied restricted role credentials. The
observer only reads. A changed/unreadable source stops the run; it is never repaired.
"""
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import json
import re
import uuid

from tl.bigquery.client import Client
from tl.bigquery.contract import validate_source_table
from tl.bigquery.hashing import encode
from tl.bigquery.schema import FIELDS
from tl.stream import ValidationError
from tl.stream.events import canonical, money, timestamp


ROLES = ('producer', 'writer', 'query', 'rebuild')
ACTIONS = ('update', 'delete', 'merge', 'truncate', 'query_overwrite',
           'load_overwrite', 'copy_overwrite', 'metadata_update', 'table_delete', 'cdc_delete')
TABLE_PERMISSIONS = ('bigquery.tables.get', 'bigquery.tables.getData',
    'bigquery.tables.updateData', 'bigquery.tables.update', 'bigquery.tables.delete',
    'bigquery.tables.setIamPolicy')


@dataclass(frozen=True)
class RoleCredential:
    principal: str
    credential: object


def fixture_fingerprint(binding, table):
    """Hash the actual sim/dev provenance of a bounded disposable fixture.

    This does not widen the sim-only workload hasher. The namespace is explicit,
    all fourteen physical fields are retained, JSON uses the shared logical
    encoding, and gateway-assigned dev rows are never relabeled as simulation.
    The function verifies shape and ordering integrity, not source authenticity.
    """
    if not re.fullmatch(r'tokenledger_iam_[a-z0-9_]{1,80}', binding.dataset):
        raise ValidationError('fixture hashing requires an explicitly disposable tokenledger_iam_ binding')
    if table.column_names != [name for name, _, _ in FIELDS]:
        raise ValidationError('denial fixture must preserve the fourteen physical columns')
    if not 1 <= len(table) <= 1000 or table.nbytes > 16_000_000:
        raise ValidationError('denial fixture must contain 1 to 1000 activities and fit the 16 MB observation bound')
    rows=[]; ids=set(); positions=set(); lanes=set()
    for batch in table.to_batches(max_chunksize=1000):
        for row in batch.to_pylist():
            key=row['activity_id']; position=row['_stream_position']; lane=row['_lane']
            if not isinstance(key,str) or not key or key in ids:
                raise ValidationError('denial fixture activity IDs must be nonempty and unique')
            if type(position) is not int or position < 1 or position in positions:
                raise ValidationError('denial fixture stream positions must be positive and unique')
            if lane not in ('sim','dev'):
                raise ValidationError('denial fixture accepts only explicit sim or dev lanes')
            ids.add(key); positions.add(position); lanes.add(lane)
            try:
                if isinstance(row['feature_json'],str): row['feature_json']=json.loads(row['feature_json'])
            except (TypeError,ValueError) as exc:
                raise ValidationError('denial fixture feature JSON is invalid') from exc
            if not isinstance(row['feature_json'],dict):
                raise ValidationError('denial fixture feature JSON must be an object')
            order=canonical({'activity_id':encode(key)})
            rows.append((order,hashlib.sha256(canonical(encode(row)).encode('utf-8')).hexdigest()))
    if positions != set(range(1,len(rows)+1)):
        raise ValidationError('denial fixture must be a complete contiguous source prefix')
    whole=hashlib.sha256()
    for _, sha in sorted(rows): whole.update(sha.encode('ascii'))
    return dict(algorithm='boundary-source-logical-json/v1',sha256=whole.hexdigest(),
        rows=len(rows),columns=sorted(table.column_names),activity_id_min=min(ids),activity_id_max=max(ids),
        input_row_range=[1,len(rows)],activity_count=len(rows),lanes=sorted(lanes),
        scope='disposable boundary fixture; physical provenance preserved; not a business workload hash')


def _sha(value):
    return hashlib.sha256(canonical(value).encode('utf-8')).hexdigest()


def _json_row(row):
    result = {}
    for key, value in row.items():
        if isinstance(value, (datetime, date)): value = value.isoformat()
        elif isinstance(value, Decimal): value = format(value, 'f')
        if key == 'feature_json' and isinstance(value, str): value = json.loads(value)
        result[key] = value
    return result


def _error(exc, action):
    """A forbidden transport or quota error is not a passed IAM denial."""
    code = getattr(exc, 'code', None)
    if callable(code): code = code()
    number = code if isinstance(code, int) else getattr(code, 'value', None)
    if isinstance(number, tuple): number = number[0]
    reasons = sorted({item.get('reason', '') for item in (getattr(exc, 'errors', None) or [])
                      if isinstance(item, dict)})
    message = str(exc)
    lower = message.lower()
    quota = (any('quota' in r.lower() or 'ratelimit' in r.lower() for r in reasons)
             or any(word in lower for word in ('quota', 'rate limit', 'ratelimit')))
    permission = any(r in ('accessDenied', 'forbidden', 'permissionDenied') for r in reasons)
    permission = permission or 'permission denied' in lower or 'access denied' in lower
    permission = permission or ('does not have' in lower and 'permission' in lower)
    # Native Storage Write RPCs name permissions between "Permission" and
    # "denied". Match that observed permission/resource structure, not an
    # arbitrary Forbidden response or a bare mention of a permission.
    permission = permission or bool(re.search(
        r"\bpermission\s+'[a-z][a-z0-9_]{1,99}'(?:\s+or\s+'[a-z][a-z0-9_]{1,99}')?\s+denied\s+on\s+resource\b",
        lower))
    error_info=getattr(exc,'error_info',None)
    scope_denied = (any('scope' in r.lower() for r in reasons)
        or 'scope' in str(getattr(error_info,'reason','')).lower()
        or any(word in lower for word in ('insufficient authentication scopes', 'insufficient oauth scope',
                                         'access_token_scope_insufficient')))
    status = 'error'
    if number in (403, 7) and scope_denied: status = 'oauth_scope_denied'
    elif number in (403, 7) and permission and not quota: status = 'iam_denied'
    if (action == 'cdc_delete' and number in (400, 3)
            and ('primary key' in lower or '_change_type' in lower)
            and any(word in lower for word in ('required', 'requires', 'must', 'not supported', 'not found'))):
        status = 'structural_rejection'
    return dict(status=status, error_type=type(exc).__name__, code=number,
        reasons=[r for r in reasons if re.fullmatch(r'[A-Za-z0-9_.-]{1,80}', r)],
        error_sha256=hashlib.sha256(message.encode('utf-8')).hexdigest())


def _cdc_request(binding, row):
    """One full-schema CDC attempt; preserve every physical field and exact type."""
    import pyarrow as pa
    from google.cloud.bigquery_storage_v1 import types
    from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
    from tl.bigquery.writer import proto_contract, serialized_rows
    if set(row) != {name for name, _, _ in FIELDS} | {'_CHANGE_TYPE'} or row['_CHANGE_TYPE']!='DELETE':
        raise ValidationError('CDC denial probe requires all fourteen fields and DELETE only')
    typed={}
    for name, kind, _ in FIELDS:
        value=row[name]
        if value is not None:
            if kind=='TIMESTAMP': value=timestamp(value)
            elif kind.startswith('NUMERIC'): value=money(value)
            elif kind=='JSON': value=canonical(json.loads(value) if isinstance(value,str) else value)
        typed[name]=value
    descriptor,base_message=proto_contract()
    encoded=next(serialized_rows(pa.Table.from_pylist([typed]),base_message))
    descriptor.name='CdcProbe'
    descriptor.field.add(name='_CHANGE_TYPE',number=len(FIELDS)+1,label=1,type=9)
    file=descriptor_pb2.FileDescriptorProto(name='cdc_probe.proto',syntax='proto2')
    file.message_type.add().CopyFrom(descriptor)
    pool=descriptor_pool.DescriptorPool();pool.Add(file)
    message=message_factory.GetMessageClass(pool.FindMessageTypeByName('CdcProbe')).FromString(encoded)
    message._CHANGE_TYPE='DELETE'
    path=f'projects/{binding.project}/datasets/{binding.dataset}/tables/activity/streams/_default'
    return types.AppendRowsRequest(write_stream=path,
        proto_rows=types.AppendRowsRequest.ProtoData(
            writer_schema=types.ProtoSchema(proto_descriptor=descriptor),
            rows=types.ProtoRows(serialized_rows=[message.SerializeToString()])))


class NativeTransport:
    """Restricted credential adapter. Never receives the observer credential."""
    def __init__(self, binding, authority):
        from google.cloud import bigquery
        self.binding = binding
        self.authority = authority
        self.sdk = bigquery.Client(project=binding.project, location=binding.location,
                                   credentials=authority.credential)
        self.job_ids = []
        self.cleanup_errors = []

    def permissions(self):
        from google.auth.transport.requests import AuthorizedSession
        table = self.sdk.test_iam_permissions(self.binding.table, list(TABLE_PERMISSIONS))
        session = AuthorizedSession(self.authority.credential)
        try:
            response = session.post(
                f'https://cloudresourcemanager.googleapis.com/v1/projects/{self.binding.project}:testIamPermissions',
                json={'permissions': ['bigquery.jobs.create', 'bigquery.datasets.create']}, timeout=30)
            response.raise_for_status()
            project = response.json().get('permissions', [])
        finally: session.close()
        return dict(status='observed', table=sorted(table.get('permissions', [])),
                    project=sorted(project), scope='bound table and job project only')

    def execute(self, action, payload):
        from google.cloud import bigquery
        binding = self.binding
        job_id = 'tl_iam_' + uuid.uuid4().hex
        if action in ('update', 'delete', 'merge', 'truncate', 'query_overwrite'):
            config = bigquery.QueryJobConfig(use_legacy_sql=False, use_query_cache=False,
                maximum_bytes_billed=binding.maximum_bytes_billed,
                labels={'application': 'tokenledger', 'purpose': 'iam-denial'})
            if action == 'query_overwrite':
                config.destination = binding.table
                config.write_disposition = 'WRITE_TRUNCATE'
            self.job_ids.append(job_id)
            self.sdk.query(payload['sql'], job_config=config, job_id=job_id,
                           location=binding.location).result(timeout=120)
        elif action == 'load_overwrite':
            config = bigquery.LoadJobConfig(write_disposition='WRITE_TRUNCATE',
                source_format='NEWLINE_DELIMITED_JSON', autodetect=False,
                labels={'application': 'tokenledger', 'purpose': 'iam-denial'})
            self.job_ids.append(job_id)
            self.sdk.load_table_from_json([payload['row']], binding.table,
                job_config=config, job_id=job_id, location=binding.location).result(timeout=120)
        elif action == 'copy_overwrite':
            config = bigquery.CopyJobConfig(write_disposition='WRITE_TRUNCATE',
                labels={'application': 'tokenledger', 'purpose': 'iam-denial'})
            self.job_ids.append(job_id)
            self.sdk.copy_table(payload['copy_source'], binding.table, job_config=config,
                job_id=job_id, location=binding.location).result(timeout=120)
        elif action == 'metadata_update':
            # No preliminary restricted read: lack of get permission must not
            # mask a granted update permission.
            table = bigquery.Table(binding.table)
            table.description = payload['description']
            self.sdk.update_table(table, ['description'])
        elif action == 'table_delete': self.sdk.delete_table(binding.table, not_found_ok=False)
        elif action == 'cdc_delete':
            from google.cloud import bigquery_storage_v1
            from google.api_core import gapic_v1
            request=_cdc_request(binding,payload['row'])
            storage = bigquery_storage_v1.BigQueryWriteClient(credentials=self.authority.credential)
            responses=None
            try:
                # One raw bidi RPC preserves the original server status. The
                # asynchronous stream manager can instead surface StreamClosedError
                # while shutting down a refused stream, obscuring its cause.
                responses=storage.append_rows(requests=iter([request]),retry=None,timeout=30,
                    metadata=(gapic_v1.routing_header.to_grpc_metadata((('write_stream',request.write_stream),)),))
                response=next(iter(responses))
                if response.error.code:
                    from google.api_core.exceptions import from_grpc_status
                    raise from_grpc_status(response.error.code, response.error.message)
            finally:
                # GAPIC exposes transport.close, not BigQueryWriteClient.close.
                # Cleanup failure must never replace a denial or hide success.
                for cleanup in (getattr(responses,'cancel',None),storage.transport.close):
                    if cleanup is None: continue
                    try: cleanup()
                    except Exception as exc: self.cleanup_errors.append(_error(exc,'transport_cleanup'))
        else: raise ValidationError('unknown denial action')


def run(binding, *, expected_creation_time, expected_source_sha256,
        credentials_by_role, reader=None, transport_factory=NativeTransport,
        copy_source_creation_time=None, progress=None):
    """Probe a pinned tokenledger_iam_* fixture, recording every missing gate.

The caller must independently provision the fixture and record its creationTime
and exact canonical source hash. No existing benchmark namespace is permitted.
An optional sibling overwrite_input table enables the copy-job probe; its
creationTime must also be explicitly pinned. No tables are created or restored.
"""
    if not re.fullmatch(r'tokenledger_iam_[a-z0-9_]{1,80}', binding.dataset):
        raise ValidationError('denials require an explicitly disposable tokenledger_iam_ fixture')
    if not re.fullmatch(r'[1-9][0-9]{1,20}', str(expected_creation_time)):
        raise ValidationError('pin the fixture table creationTime before denial probes')
    if not re.fullmatch(r'[a-f0-9]{64}', expected_source_sha256):
        raise ValidationError('pin the exact original source SHA-256')
    if set(credentials_by_role) - set(ROLES): raise ValidationError('unknown denial identity role')
    principals = []
    for authority in credentials_by_role.values():
        if (not isinstance(authority, RoleCredential) or authority.credential is None
                or getattr(authority.credential, 'service_account_email', None) != authority.principal):
            raise ValidationError('each role requires explicitly matching service account credentials')
        principals.append(authority.principal)
    if len(set(principals)) != len(principals): raise ValidationError('denial identities must be distinct')
    # Each attempted action has two complete source observations; each SQL
    # probe can additionally consume its configured query ceiling. Reserve the
    # worst case before any query. Load/copy jobs do not use analysis pricing.
    actions_per_role = len(ACTIONS) - (copy_source_creation_time is None)
    maximum_query_bytes = (1 + len(principals) * (2 * actions_per_role + 5)) * binding.maximum_bytes_billed
    if maximum_query_bytes > binding.maximum_run_bytes_billed:
        raise ValidationError('denial probes exceed the explicit aggregate query byte ceiling')
    reader = reader or Client(binding)
    started = datetime.now(timezone.utc).isoformat()
    results = []; permissions = {}
    original_metadata = reader.sdk.get_table(binding.table).to_api_repr()
    original_description = original_metadata.get('description')

    def snapshot():
        source_table = reader.sdk.get_table(binding.table)
        metadata = source_table.to_api_repr()
        if str(metadata.get('creationTime')) != str(expected_creation_time):
            raise ValidationError('fixture source incarnation changed')
        validate_source_table(source_table)
        if metadata.get('description') != original_description:
            raise ValidationError('fixture metadata was changed by a denial attempt')
        table, job = reader.query(f'SELECT * FROM `{binding.table}` ORDER BY _stream_position LIMIT 1001', label='iam_source_hash')
        identity = fixture_fingerprint(binding,table)
        if identity['sha256'] != expected_source_sha256:
            raise ValidationError('denial fixture source differs from the pinned original')
        return identity, table, job

    initial, table, _ = snapshot()
    row = _json_row(table.slice(0, 1).to_pylist()[0])
    sentinel = row['activity_id'].replace("'", "''")
    target = f'`{binding.table}`'
    condition = f"activity_id = '{sentinel}'"
    payloads = {
        'update': {'sql': f"UPDATE {target} SET _source = 'denial_probe_changed' WHERE {condition}"},
        'delete': {'sql': f'DELETE FROM {target} WHERE {condition}'},
        'merge': {'sql': f"MERGE {target} AS t USING (SELECT '{sentinel}' AS activity_id) AS s ON t.activity_id = s.activity_id WHEN MATCHED THEN DELETE"},
        'truncate': {'sql': f'TRUNCATE TABLE {target}'},
        'query_overwrite': {'sql': f'SELECT * FROM {target} WHERE FALSE', 'destination': binding.table, 'write_disposition': 'WRITE_TRUNCATE'},
        'load_overwrite': {'row': {**row, '_source': 'denial_probe_changed'}, 'write_disposition': 'WRITE_TRUNCATE'},
        'copy_overwrite': {'copy_source': f'{binding.project}.{binding.dataset}.overwrite_input', 'write_disposition': 'WRITE_TRUNCATE'},
        'metadata_update': {'description': 'denial_probe_changed'},
        'table_delete': {'table': binding.table},
        'cdc_delete': {'row': {**row, '_CHANGE_TYPE': 'DELETE'}},
    }
    if copy_source_creation_time is not None:
        copy_meta = reader.sdk.get_table(payloads['copy_overwrite']['copy_source']).to_api_repr()
        if str(copy_meta.get('creationTime')) != str(copy_source_creation_time):
            raise ValidationError('overwrite copy fixture incarnation differs')
    stopped = False
    for role in ROLES:
        authority = credentials_by_role.get(role)
        transport = None if authority is None else transport_factory(binding, authority)
        if transport:
            try: permissions[role] = transport.permissions()
            except Exception as exc: permissions[role] = _error(exc, 'permission_inspection')
        for action in ACTIONS:
            result = dict(role=role, action=action, table=binding.table,
                principal=None if authority is None else authority.principal,
                action_sha256=_sha(payloads[action]), job_ids=[])
            if stopped or transport is None or (action == 'copy_overwrite' and copy_source_creation_time is None):
                result.update(status='not_run', reason='source_changed_or_unreadable' if stopped else
                    'role_not_bound' if transport is None else 'copy_source_not_bound')
                results.append(result); continue
            try:
                before, _, before_job = snapshot()
            except Exception as exc:
                result.update(status='source_changed_or_unreadable', observation_error=_error(exc, action))
                results.append(result); stopped = True; continue
            job_start = len(transport.job_ids)
            cleanup_start = len(getattr(transport,'cleanup_errors',[]))
            try:
                transport.execute(action, payloads[action])
                result.update(status='unexpected_success')
            except Exception as exc: result.update(_error(exc, action))
            result['job_ids'] = transport.job_ids[job_start:]
            result['cleanup_errors'] = getattr(transport,'cleanup_errors',[])[cleanup_start:]
            result['before'] = before; result['before_observation_job'] = before_job
            try:
                after, _, after_job = snapshot()
                result.update(after=after, after_observation_job=after_job, source_unchanged=True)
            except Exception as exc:
                result.update(source_unchanged=False, after_observation_error=_error(exc, action))
                stopped = True
            if result['status'] == 'unexpected_success': stopped = True
            results.append(result)
            if progress: progress(dict(role=role, action=action, status=result['status'], source_unchanged=result['source_unchanged']))
    expected = len(ROLES) * len(ACTIONS)
    all_refused = len(results) == expected and all(
        r['status'] in ('iam_denied', 'structural_rejection') and r.get('source_unchanged') for r in results)
    return dict(schema_version='tokenledger-bigquery-denials/v1',
        status='failed' if stopped else 'refusals_observed' if all_refused else 'incomplete',
        started_at=started, finished_at=datetime.now(timezone.utc).isoformat(),
        binding_identity=binding.identity, table=binding.table, creation_time=str(expected_creation_time),
        original=initial, cases=results, expected_cases=expected, permissions=permissions,
        maximum_query_bytes_reserved=maximum_query_bytes,
        production_acceptance='not_established',
        cross_project_jobs='not_run',
        remaining=['authenticated gateway append and validation', 'concurrent requests and conflicting retries',
                   'stream expiry/finalization and revision fencing', 'audit log correlation',
                   'inherited grants, service account authority and cross-project jobs review'],
        trusted_boundary=('Writer updateData is not globally INSERT-only: jobs.create in another billing project '
            'can permit TRUNCATE or other source mutation. Narrow operational token scopes do not remove '
            'trust in the gateway, credential minters, and deployment/IAM administrators.'))
