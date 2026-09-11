"""CLI/API ingress and bounded reads using the accepted authenticated gateway."""
from pathlib import Path
from datetime import datetime, timezone
import hashlib
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

from tl.bigquery.gateway import Gateway, GatewayConfig
from tl.bigquery.gateway_probe import identity_token, https_request, source_fingerprint
from tl.bigquery.schema import FIELDS
from tl.evolution import admission, evidence
from tl.receipts.metrics import code_revision, digest
from tl.stream import Activity, ValidationError
from tl.stream.events import Catalog, canonical, iso
from tl.stream.store import prepare_row


def event(activity, case_id, record, event_id):
    now = datetime.now(timezone.utc)
    value = dict(activity_id=event_id, ts=iso(now), customer='learning:' + case_id,
                 anonymous_customer_id=None, activity=activity, feature_json=admission.envelope(case_id, record),
                 revenue_impact=None, link=None)
    prepared = prepare_row(Activity(**value), now, Catalog(admission.CATALOG), source='learning/prepared', actor='unsubmitted', lane='dev')
    admission.body(prepared)
    return value


def submit(config, value, producer, url, *, output_root, ledger, sender=https_request, token_factory=identity_token):
    if config.admission_policy != admission.POLICY:
        raise ValidationError('submission requires the reporting learning gateway')
    if producer not in config.allowed_producers or value['activity'] not in config.allowed_activities[producer]:
        raise ValidationError('producer lacks the requested workflow capability')
    from tl.bigquery.gateway_probe import _origin
    # The new application uses its exact deployed service origin as audience.
    if _origin(url) != config.audience:
        raise ValidationError('learning endpoint differs from pinned audience')
    root = Path(output_root) / uuid.uuid4().hex
    root.mkdir(parents=True, exist_ok=False)
    pins = evidence.artifacts(); revision = code_revision(pins)
    raw = canonical(dict(events=[value])).encode()
    evidence.write(root / 'request.json', dict(events=[value]))
    try:
        status, response = sender(url, raw, token_factory(producer, config.audience))
        # Retain bounded response bytes, never the bearer token or headers.
        (root / 'response.json').write_bytes(response)
        answer = admission.strict_json(response)
        result = dict(status='accepted' if status == 200 and answer.get('status') == 'accepted' else 'rejected_or_unverified',
                      http_status=status, activity_id=value['activity_id'], gateway_response=answer,
                      authenticated_actor=producer, authority='gateway response observation; not independent source reperformance')
    except Exception as exc:
        result = dict(status='submission_unknown', activity_id=value['activity_id'], error_type=type(exc).__name__,
                      retry='same event file and ID only')
    record = evidence.seal(root, dict(schema_version=evidence.OBSERVATION, run_id=root.name, result=result,
        definition_version='learning-admission/v1', inputs=dict(activity_count=1, activity_id_min=value['activity_id'],
            activity_id_max=value['activity_id'], input_row_range=[None,None], sha256=hashlib.sha256(raw).hexdigest()),
        query_hash=digest(dict(endpoint='/append', gateway=config.identity)), git_sha=revision['git_sha'],
        execution_hash=digest(pins), binding_identity=config.identity), ledger)
    return dict(status=result['status'], receipt_id=record['receipt_id'], run_directory=str(root.resolve()), result=result)


class ReadMetadata:
    def __init__(self, binding):
        self.binding = binding

    def metadata(self, name):
        from google.cloud import bigquery_storage_v1
        from tl.bigquery.auth import credentials
        sdk = bigquery_storage_v1.BigQueryWriteClient(credentials=credentials(self.binding, 'query'))
        return sdk.get_write_stream(name=name)


def snapshot(config, *, output_root, ledger, client=None, metadata=None):
    if config.admission_policy != admission.POLICY:
        raise ValidationError('snapshot requires a reporting learning application')
    gateway = Gateway(config, admission.CATALOG, client=client, storage=metadata or ReadMetadata(config.binding))
    rows = list(gateway._prefix().values())
    admission.validate(rows, [], config)
    table = pa.Table.from_pylist(rows) if rows else pa.table({name: [] for name, _, _ in FIELDS})
    root = Path(output_root) / uuid.uuid4().hex
    root.mkdir(parents=True, exist_ok=False)
    pq.write_table(table, root / 'source.parquet')
    config.save(root / 'gateway.json')
    evidence.write(root / 'jobs.json', gateway.client.jobs)
    inputs = source_fingerprint(table)
    pins = evidence.artifacts(); revision = code_revision(pins)
    result = dict(status='verified_snapshot', watermark=len(rows), source=inputs,
                  state_validation=True, native_execution=True)
    record = evidence.seal(root, dict(schema_version=evidence.OBSERVATION, run_id=root.name, result=result,
        definition_version='learning-workflow/v1', inputs=inputs, query_hash=digest(dict(gateway=config.identity, query='bounded_prefix/v1')),
        git_sha=revision['git_sha'], execution_hash=digest(pins), binding_identity=config.identity), ledger)
    return dict(status=result['status'], receipt_id=record['receipt_id'], run_directory=str(root.resolve()), result=result)


def case_view_sql(config):
    if config.admission_policy != admission.POLICY:
        raise ValidationError('case view requires a reporting learning binding')
    return f'''CREATE VIEW `{config.binding.project}.{config.binding.dataset}.learning_cases` AS
WITH events AS (
 SELECT activity_id,activity,_actor,_recorded_at,_stream_position,
        JSON_VALUE(feature_json,'$.case_id') AS case_id,
        PARSE_JSON(JSON_VALUE(feature_json,'$.record_json')) AS record
 FROM `{config.binding.table}`
)
SELECT case_id,
       ARRAY_AGG(STRUCT(activity_id,activity,_actor,_recorded_at,_stream_position,record)
                 ORDER BY _stream_position) AS history,
       ARRAY_AGG(activity ORDER BY _stream_position DESC LIMIT 1)[OFFSET(0)] AS latest_activity
FROM events GROUP BY case_id
'''
