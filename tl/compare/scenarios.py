"""Fixed USD-only evidence increments. No recognized-revenue oracle rows."""
from tl.stream import Activity,Stream


def inject(db,scenario):
    if scenario not in ('late','day'):
        raise ValueError('unknown comparison scenario')
    late=scenario=='late'
    end='2026-06-30T23:59:59Z' if late else '2026-07-20T23:59:59Z'
    start='2026-06-01' if late else '2026-07-20'
    stop='2026-07-01' if late else '2026-07-21'
    arrival='2026-07-10T12:00:00Z' if late else '2026-07-21T12:00:00Z'
    key='comparison:'+scenario
    token=Activity(key+':tokens',end,'tokens_processed',dict(
        usage_batch_id=key,model='synthetic-g5',model_release_date='2026-02-01',token_type='input',
        tokens=1000000000 if late else 500000000,list_price_per_mtok=3.0,effective_price_per_mtok=3.0,
        channel='direct',workload='api'),'customer:0000001')
    amount='3000.00' if late else '1500.00'
    invoice=Activity(key+':invoice',end,'usage_invoiced',dict(invoice_id=key,usage_batch_id=key,
        contract_id=None,period_start=start,period_end=stop,gross_usd=amount,discount_usd='0.00',
        net_usd=amount,channel_fee_usd='0.00',commit_applied_usd='0.00',cash_due_usd=amount,channel='direct'),
        'customer:0000001')
    # Original June already knows usage; only its invoice is late.
    events=[(token,end if late else arrival),(invoice,arrival)]
    result=Stream(db).append_simulation(events,source='sim:comparison-v1',actor='GPT-6')
    return dict(scenario=scenario,events=[e.activity_id for e,_ in events],result=result,
                economic_end=end,invoice_recorded_at=arrival)
