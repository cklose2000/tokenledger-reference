"""A publication boundary over captured source evidence, with frozen failed closes."""
import json
from pathlib import Path

from dataclasses import asdict
from tl.controls.sources import capture, ingest, load_capture, reconcile
from tl.receipts.metrics import input_manifest, read_receipts
from tl.stream import Activity, Stream, StreamReader, ValidationError
from tl.stream.events import canonical
from tl.views.engine import profile

ASOF = '2026-06-30'
KNOWN = '2026-07-15T00:00:00Z'
START, END = '2026-04-01', '2026-07-01'


def snapshot(root, watermark=None):
    return StreamReader(Path(root)/'world.duckdb').snapshot(
        asof=ASOF, known_at=KNOWN, watermark=watermark, include_provenance=True)


def gate(root, watermark=None):
    root = Path(root)
    table = snapshot(root, watermark)
    result = reconcile(table, [load_capture(root/'capture/manifest.json')], start=START, end=END)
    return dict(status='ready' if result['passed'] else 'held', reconciliation=result,
                inputs=input_manifest(table), watermark=max(table['_stream_position'].to_pylist(), default=0),
                asof=ASOF, known_at=KNOWN)


def publish(root, *, watermark=None, recover=False):
    """The challenge's close publisher cannot bypass its source-completeness gate."""
    root = Path(root)
    if recover and watermark is not None:
        raise ValidationError('recovery uses current source knowledge, not a historical watermark')
    ingestion = ingest(root/'world.duckdb', root/'capture/manifest.json') if recover else None
    decision = gate(root, watermark)
    if decision['status'] == 'held':
        return {**decision, 'publication': 'held', 'ingestion': ingestion}
    published = root/'publication.json'
    if published.exists():
        saved = json.loads(published.read_bytes())
        if saved['inputs'] != decision['inputs']:
            raise ValidationError('published close is immutable; create a new close for changed inputs')
        return {**decision, 'publication': 'already_published', 'run_id': saved['run_id'], 'ingestion': ingestion}
    run = profile(root/'world.duckdb', asof=ASOF, known_at=KNOWN,
                  watermark=decision['watermark'], names=['close_accounts'],
                  output_root=root/'runs', ledger=root/'metrics.jsonl')
    records = read_receipts(root/'metrics.jsonl')
    key = next(k for k in run['receipt_ids'] if records[k]['schema_version']=='tokenledger-views/v1')
    saved = dict(inputs=decision['inputs'], run_id=run['run_id'], receipt_id=key)
    published.write_text(canonical(saved)+'\n', encoding='utf-8', newline='\n')
    return {**decision, 'publication': 'published', **saved, 'ingestion': ingestion}


def prepare(root):
    root = Path(root); root.mkdir(parents=True, exist_ok=False)
    Stream(root/'world.duckdb').init()
    fixture=root/'fixture'; fixture.mkdir()
    features=dict(invoice_id='invoice',usage_batch_id='batch',contract_id=None,
        period_start='2026-06-01',period_end='2026-07-01',gross_usd='20.00',discount_usd='0.00',
        net_usd='20.00',channel_fee_usd='5.00',commit_applied_usd='0.00',cash_due_usd='20.00',channel='aws_marketplace')
    events=[
        Activity('customer','2026-06-01T12:00:00Z','customer_created',dict(segment='enterprise',channel='aws_marketplace',country='US',parent_customer=None),'synthetic-account'),
        Activity('model','2026-06-01T12:00:00Z','model_released',dict(model='synthetic-g1',generation=1,release_date='2026-06-01',training_cost_usd_estimate='1.00'),'synthetic-account'),
        Activity('tokens','2026-06-30T12:00:00Z','tokens_processed',dict(usage_batch_id='batch',model='synthetic-g1',model_release_date='2026-06-01',token_type='input',tokens=1000000,list_price_per_mtok=20,effective_price_per_mtok=20,channel='aws_marketplace',workload='api'),'synthetic-account'),
        Activity('invoice','2026-06-30T12:00:00Z','usage_invoiced',features,'synthetic-account'),
        Activity('cash','2026-06-30T12:00:00Z','revenue_recognized',dict(period='2026-06-01',net_usd='15.00',source='usage',recognition_kind='usage',source_document='invoice',channel='aws_marketplace'),'synthetic-account',revenue_impact='15.00')]
    events.append(events[3]) # An exact repeated invoice is an observation, not another transaction.
    (fixture/'source.jsonl').write_text(''.join(canonical(dict(event=asdict(event),recorded_at='2026-07-06T00:00:00Z'))+'\n' for event in events),encoding='utf-8',newline='\n')
    capture(root/'world.duckdb', root/'fixture/source.jsonl', root/'capture',
            source='sim:controls', start=START, end=END)
    source = load_capture(root/'capture/manifest.json')
    present = [(Activity(**r['event']), r['recorded_at']) for r in source['observations']
               if r['status']=='accepted' and r['activity_id']!='cash']
    Stream(root/'world.duckdb').append_simulation(present, source=source['source'], actor='synthetic-source-import')
    failed = publish(root)
    if failed['status']!='held' or (root/'publication.json').exists():
        raise ValidationError('missing source did not hold publication')
    (root/'failed-close.json').write_text(canonical(failed)+'\n', encoding='utf-8', newline='\n')
    recovered = publish(root, recover=True)
    before = (root/'publication.json').read_bytes()
    retried = publish(root, recover=True)
    if retried['ingestion']['inserted'] or before != (root/'publication.json').read_bytes():
        raise ValidationError('close recovery retry changed the source or publication')
    return dict(failed=failed, recovered=recovered, retry=retried)
