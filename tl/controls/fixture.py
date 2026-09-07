"""Small hand-authored source and separate ERP amounts; no accounting query generates the GL."""
from dataclasses import asdict
from pathlib import Path
from tl.stream import Activity
from tl.stream.events import canonical
from tl.controls.evidence import encoded,sha


def write_fixture(destination):
    root=Path(destination);root.mkdir(parents=True,exist_ok=False)
    events=[]
    def emit(key,activity,features,*,day='2026-06-30',revenue=None):
        event=Activity(key,day+'T12:00:00Z',activity,features,'synthetic-account',revenue_impact=revenue)
        events.append(dict(event=asdict(event),recorded_at='2026-07-06T00:00:00Z'))
    emit('customer','customer_created',dict(segment='enterprise',channel='aws_marketplace',country='US',parent_customer=None),day='2026-06-01')
    emit('model','model_released',dict(model='synthetic-g1',generation=1,release_date='2026-06-01',training_cost_usd_estimate='1.00'),day='2026-06-01')
    emit('contract','contract_signed',dict(contract_id='commit',term_months=1,commit_usd='100.00',discount_pct=.1,channel='aws_marketplace',start='2026-06-01',end='2026-07-01'),day='2026-06-01')
    emit('tokens','tokens_processed',dict(usage_batch_id='batch',model='synthetic-g1',model_release_date='2026-06-01',token_type='input',tokens=1000000,list_price_per_mtok=100,effective_price_per_mtok=90,channel='aws_marketplace',workload='api'))
    emit('invoice','usage_invoiced',dict(invoice_id='invoice',usage_batch_id='batch',contract_id='commit',period_start='2026-06-01',period_end='2026-07-01',gross_usd='100.00',discount_usd='10.00',net_usd='90.00',channel_fee_usd='5.00',commit_applied_usd='70.00',cash_due_usd='20.00',channel='aws_marketplace'))
    def recognized(key,amount,source,kind,document):
        emit(key,'revenue_recognized',dict(period='2026-06-01',net_usd=amount,source=source,recognition_kind=kind,source_document=document,channel='aws_marketplace'),revenue=amount)
    recognized('drawdown','70.00','commit_drawdown','usage','invoice')
    recognized('cash','15.00','usage','usage','invoice')
    emit('credit','credit_issued',dict(credit_id='credit',invoice_id='invoice',period='2026-06-01',amount_usd='3.00',reason='synthetic service credit'))
    recognized('credit-revenue','-3.00','usage','credit','credit')
    emit('subscription','subscription_started',dict(subscription_id='subscription',plan='team',seats=2,price_per_seat='60.00',billing_period='monthly',period_start='2026-06-01',period_end='2026-07-01'),day='2026-06-01')
    recognized('subscription-revenue','120.00','subscription','subscription','subscription')
    emit('expiry','contract_expired',dict(contract_id='commit',unconsumed_commit_usd='30.00',reason='synthetic expiry'))
    recognized('expiry-revenue','30.00','commit_drawdown','commit_expiry','commit')
    # Identical duplicate source observation exercises deduplication.
    events.append(events[4])
    (root/'source.jsonl').write_bytes(b''.join(encoded(row) for row in events))
    erp=b'period,account,source,amount_usd\n2026-06-01,4000,sim:controls,90.00\n2026-06-01,4010,sim:controls,120.00\n2026-06-01,4020,sim:controls,30.00\n2026-06-01,4090,sim:controls,-3.00\n'
    (root/'erp.csv').write_bytes(erp)
    (root/'erp-manifest.json').write_bytes(encoded(dict(schema_version='tokenledger-synthetic-erp/v1',synthetic=True,
        origin='hand-authored synthetic ERP fixture; not generated from the stream or scaffolds',coverage_start='2026-04-01',coverage_end='2026-07-01',
        presentation='gross_revenue_separate_fees',sha256=sha(erp))))
    return dict(status='created',synthetic=True,path=str(root))
