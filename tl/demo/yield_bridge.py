"""Business-facing July yield from freshly verified native populations."""
from decimal import Decimal, localcontext, ROUND_HALF_UP
from pathlib import Path
import json, uuid

import pyarrow.parquet as pq

from tl.compare.engine import artifacts
from tl.demo.bridge import hashed
from tl.metrics.engine import runtime
from tl.receipts.metrics import append_receipts, code_revision, digest, read_receipts, receipt_id
from tl.stream import ValidationError
from tl.stream.events import canonical

SCHEMA = 'tokenledger-demo-yield/v1'
VERSION = 'recognized-usage-yield-bridge/v1'


def ratio(cents, tokens):
    if not tokens:
        return None
    with localcontext() as context:
        context.prec = 80
        return format((Decimal(cents) * 10000 / tokens).quantize(
            Decimal('0.000000000001'), rounding=ROUND_HALF_UP), 'f')


def summarize(rows, month):
    selected = [r for r in rows if str(r['month']) == month]
    totals = {name: sum(int(r[name]) for r in selected) for name in
              ('tokens', 'net_revenue_cents', 'channel_fee_cents', 'revenue_before_channel_fees_cents')}
    if totals['net_revenue_cents'] + totals['channel_fee_cents'] != totals['revenue_before_channel_fees_cents']:
        raise ValidationError('yield revenue basis does not reconcile')
    return {**totals, 'net_of_discounts_and_fees_per_mtok': ratio(totals['net_revenue_cents'], totals['tokens']),
            'net_of_discounts_per_mtok': ratio(totals['revenue_before_channel_fees_cents'], totals['tokens'])}


def calculate(old_id, new_id, bridge_id, *, output_root, ledger):
    records = read_receipts(ledger)
    old, new, bridge = [records[k] for k in (old_id, new_id, bridge_id)]
    if any(p.get('schema_version') != 'tokenledger-views/v1' or
           p['result']['output'] != 'net_rev_per_mtok' for p in (old, new)):
        raise ValidationError('yield bridge requires native token-yield populations')
    if bridge.get('schema_version') != 'tokenledger-demo-bridge/v1':
        raise ValidationError('yield bridge requires the controlled account bridge')
    accounts = [records[bridge['result'][k]] for k in ('original_receipt_id', 'current_receipt_id')]
    for metric, account in zip((old, new), accounts):
        for field in ('asof', 'known_at', 'watermark', 'definition_version', 'execution_hash', 'inputs'):
            if metric[field] != account[field]:
                raise ValidationError('yield and accounting snapshots or policies differ')
    month = old['asof'][:7] + '-01'
    before, after = [summarize(pq.read_table(Path(output_root) / p['run_id'] /
                       'net_rev_per_mtok.parquet').to_pylist(), month) for p in (old, new)]
    if before['tokens'] <= 0 or before['tokens'] != after['tokens']:
        raise ValidationError('controlled late-invoice demo requires a positive unchanged token denominator')
    delta = {k: after[k] - before[k] for k in
             ('tokens', 'net_revenue_cents', 'channel_fee_cents', 'revenue_before_channel_fees_cents')}
    delta.update(net_of_discounts_and_fees_per_mtok=ratio(delta['net_revenue_cents'], before['tokens']),
                 net_of_discounts_per_mtok=ratio(delta['revenue_before_channel_fees_cents'], before['tokens']))
    lines = [r for r in bridge['result']['rows'] if r['month'] == month]
    credit = lambda codes: -sum(r['delta_cents'] for r in lines if r['account'] in codes)
    if delta['net_revenue_cents'] != credit({'4000', '4095'}) or delta['revenue_before_channel_fees_cents'] != credit({'4000'}):
        raise ValidationError('recognized usage yield change does not tie to the account bridge')
    if not delta['net_revenue_cents']:
        raise ValidationError('the demo fixture must change the headline business result')
    return dict(metric='recognized_usage_revenue_per_million_tokens', month=month, asof=old['asof'],
        original_known_at=old['known_at'], current_known_at=new['known_at'],
        revenue_basis='Recognized usage revenue, net of discounts and marketplace fees; alternative before marketplace fees also shown',
        denominator='Raw processed tokens: input, output, cache_read and cache_write, each counted once with unit weight',
        aggregation='Sum recognized revenue and sum tokens across disjoint cuts; never average per-cut yields',
        units=dict(tokens='tokens', net_revenue_cents='USD cents', channel_fee_cents='USD cents',
                   revenue_before_channel_fees_cents='USD cents', net_of_discounts_and_fees_per_mtok='USD/Mtok',
                   net_of_discounts_per_mtok='USD/Mtok'),
        rounding='12 decimal places, half up; delta computed from exact revenue change and unchanged token denominator',
        original=before, current=after, delta=delta, unchanged=['tokens'],
        original_receipt_id=old_id, current_receipt_id=new_id, account_bridge_receipt_id=bridge_id,
        responsible_activity_ids=bridge['result']['responsible_activity_ids'],
        attribution_scope=bridge['result']['causal_basis'])


def create(old_id, new_id, bridge_id, *, db, output_root, ledger):
    from tl.metrics.engine import replay_receipts
    pins = artifacts(); revision = code_revision(pins)
    replay_receipts([old_id, new_id, bridge_id], db=db, output_root=output_root, ledger=ledger)
    result = calculate(old_id, new_id, bridge_id, output_root=output_root, ledger=ledger)
    records = read_receipts(ledger); old, new = [records[k] for k in (old_id, new_id)]
    run_id = uuid.uuid4().hex; root = Path(output_root) / run_id; root.mkdir()
    manifest = dict(schema_version=SCHEMA, run_id=run_id, stream_path=str(Path(db).resolve()),
        execution_artifacts=pins, execution_hash=digest(pins), runtime=runtime(), definition_version=VERSION,
        query_hash=digest(pins), inputs=dict(original=old['inputs'], current=new['inputs']), asof=old['asof'],
        known_at=new['known_at'], watermark=new['watermark'], parents=[old_id, new_id, bridge_id], **revision)
    if artifacts() != pins: raise ValidationError('yield implementation changed during execution')
    (root/'run.json').write_text(canonical(manifest)+'\n', encoding='utf-8', newline='\n')
    record = {k: manifest[k] for k in ('schema_version','run_id','definition_version','query_hash','inputs',
                                      'asof','known_at','watermark','git_sha','execution_hash')}
    record.update(result=result, manifest_sha256=hashed(root/'run.json'))
    record['receipt_id'] = receipt_id(record)
    (root/'yield.json').write_text(canonical(dict(receipt_id=record['receipt_id'], **result))+'\n', encoding='utf-8', newline='\n')
    append_receipts(ledger, [record])
    return dict(receipt_id=record['receipt_id'], run_id=run_id, **result)


def replay(ids, *, db, output_root, ledger, python=None):
    from tl.metrics.engine import replay_receipts
    records = read_receipts(ledger); answers = []
    for key in ids:
        record = records[key]; root = Path(output_root)/record['run_id']
        if hashed(root/'run.json') != record['manifest_sha256']: raise ValidationError('yield manifest changed')
        manifest = json.loads((root/'run.json').read_bytes())
        for field in ('schema_version','run_id','definition_version','query_hash','inputs','asof','known_at',
                      'watermark','git_sha','execution_hash'):
            if manifest[field] != record[field]: raise ValidationError('yield receipt contract changed')
        if digest(manifest['execution_artifacts']) != manifest['execution_hash']:
            raise ValidationError('yield implementation identity changed')
        if python or artifacts() != manifest['execution_artifacts'] or runtime() != manifest['runtime']:
            from tl.receipts.archive import historical_replay
            answers.extend(historical_replay([key], manifest=manifest, db=db or manifest['stream_path'],
                output_root=output_root, ledger=ledger, python=python)); continue
        replay_receipts(manifest['parents'], db=db or manifest['stream_path'], output_root=output_root, ledger=ledger)
        actual = calculate(*manifest['parents'], output_root=output_root, ledger=ledger)
        if actual != record['result'] or json.loads((root/'yield.json').read_bytes()) != dict(receipt_id=key, **actual):
            raise ValidationError('yield reproduction differs')
        answers.append(dict(receipt_id=key, verified=True, recomputed=True, result=actual))
    return answers
