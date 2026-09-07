"""Columnar implementation of the spine's accounting/v1 policy.

Authored from its integer-cent policy and retained Python implementation.
The independent dbt baseline is neither imported nor used in these queries.
"""


SUBSCRIPTIONS = ('subscription_started','subscription_renewed','subscription_upgraded',
                 'subscription_downgraded','subscription_cancelled','seat_added','seat_removed')


def register(g):
    g.typed('contracts', ('contract_signed','contract_renewed'),
            dict(contract_id='VARCHAR',commit_usd='DECIMAL(18,2)',channel='VARCHAR',start='DATE',end='DATE'))
    g.typed('invoices', ('usage_invoiced',), dict(invoice_id='VARCHAR',usage_batch_id='VARCHAR',
            contract_id='VARCHAR',period_start='DATE',period_end='DATE',gross_usd='DECIMAL(18,2)',
            discount_usd='DECIMAL(18,2)',net_usd='DECIMAL(18,2)',channel_fee_usd='DECIMAL(18,2)',channel='VARCHAR'))
    g.typed('credits', ('credit_issued',),dict(credit_id='VARCHAR',invoice_id='VARCHAR',period='DATE',amount_usd='DECIMAL(18,2)'))
    g.typed('expiries', ('contract_expired',),dict(contract_id='VARCHAR'))
    g.typed('subscriptions', SUBSCRIPTIONS,dict(subscription_id='VARCHAR',plan='VARCHAR',seats='BIGINT',
            seats_after='BIGINT',price_per_seat='DECIMAL(18,2)',period_end='DATE',billing_period='VARCHAR'))
    # Invoice ordering and cumulative committed funds match accounting/v1.
    g.add('funding', '''WITH ordered AS (
      SELECT i.*,c.activity_id AS contract_activity,c.f.commit_usd*100 AS commit_cents,
        c.f.start AS contract_start,c.f.end AS contract_end,
        (i.f.net_usd*100)::BIGINT AS net_cents,(i.f.channel_fee_usd*100)::BIGINT AS fee_cents,
        coalesce(sum(i.f.net_usd*100) OVER(PARTITION BY i._source,i.f.contract_id
          ORDER BY i.f.period_end,i.activity_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) AS prior_cents
      FROM @invoices i LEFT JOIN @contracts c ON i._source=c._source AND i.f.contract_id=c.f.contract_id
      WHERE i.f.contract_id IS NOT NULL
      UNION ALL SELECT i.*,NULL::VARCHAR AS contract_activity,NULL::DECIMAL(18,2) AS commit_cents,
        NULL::DATE AS contract_start,NULL::DATE AS contract_end,
        (i.f.net_usd*100)::BIGINT AS net_cents,(i.f.channel_fee_usd*100)::BIGINT AS fee_cents,
        0::HUGEINT AS prior_cents FROM @invoices i WHERE i.f.contract_id IS NULL)
      SELECT *,least(net_cents,greatest(coalesce(commit_cents,0)-prior_cents,0))::BIGINT AS draw_cents,
        CASE WHEN contract_activity IS NULL THEN [activity_id]
             ELSE list_sort([activity_id,contract_activity]) END AS upstream_ids FROM ordered''')
    g.add('invoice_months', '''SELECT i.*,m.month,
      date_diff('day',i.f.period_start,i.f.period_end)::HUGEINT AS days,
      date_diff('day',i.f.period_start,greatest(i.f.period_start,m.month))::HUGEINT AS lo,
      date_diff('day',i.f.period_start,least(i.f.period_end,m.month_end))::HUGEINT AS hi
      FROM @funding i JOIN @calendar m ON i.f.period_start<m.month_end AND i.f.period_end>m.month''')
    # Cumulative half-up in integer arithmetic, once per amount and service month.
    terms=[]
    for source,target in [('net_cents','net'),('fee_cents','fee'),('draw_cents','draw')]:
        terms.append(f'(((2*{source}::HUGEINT*hi+days)//(2*days))-((2*{source}::HUGEINT*lo+days)//(2*days)))::BIGINT AS {target}')
    g.add('invoice_amounts','SELECT *, '+','.join(terms)+' FROM @invoice_months')
    g.add('expiry_amounts', '''WITH spent AS (
      SELECT _source,f.contract_id AS contract_id,sum(draw_cents)::BIGINT AS spent,max(f.period_end) AS last_service
      FROM @funding GROUP BY 1,2)
      SELECT e.*,date_trunc('month',e.ts)::DATE AS month,c.f.channel AS channel,c.activity_id AS contract_activity,
        (c.f.commit_usd*100-coalesce(s.spent,0))::BIGINT AS amount,
        list_sort([e.activity_id,c.activity_id]) AS upstream_ids,s.last_service
      FROM @expiries e LEFT JOIN @contracts c ON e._source=c._source AND e.f.contract_id=c.f.contract_id
      LEFT JOIN spent s ON e._source=s._source AND e.f.contract_id=s.contract_id''')
    g.add('credit_amounts', '''SELECT c.*,i.activity_id AS invoice_activity,i.f.channel AS channel,
      (c.f.amount_usd*100)::BIGINT AS amount,list_sort([c.activity_id,i.activity_id]) AS upstream_ids
      FROM @credits c LEFT JOIN @invoices i ON c._source=i._source AND c.f.invoice_id=i.f.invoice_id''')
    g.add('subscription_states', '''WITH events AS (
      SELECT *,CASE WHEN activity IN ('seat_added','seat_removed') THEN 20
        WHEN activity IN ('subscription_upgraded','subscription_downgraded') THEN 10
        WHEN activity='subscription_cancelled' THEN 30 ELSE 0 END AS priority
      FROM @subscriptions)
      SELECT *,ts::DATE AS start_day,
        least(lead(ts::DATE,1,(SELECT period_end FROM @context)) OVER w,
          coalesce(last_value(f.period_end IGNORE NULLS) OVER w,(SELECT period_end FROM @context)),
          (SELECT period_end FROM @context)) AS stop_day,
        coalesce(last_value(f.price_per_seat IGNORE NULLS) OVER w*100,0)::BIGINT AS price_cents,
        CASE WHEN activity='subscription_cancelled' THEN 0 ELSE coalesce(f.seats_after,f.seats) END AS paid_seats
      FROM events WINDOW w AS (PARTITION BY _source,customer,f.subscription_id ORDER BY ts,priority,activity_id)''')
    g.add('subscription_months', '''SELECT s._source,s.customer,s.f.subscription_id AS subscription_id,m.month,
       sum(s.price_cents::HUGEINT*s.paid_seats*date_diff('day',greatest(s.start_day,m.month),least(s.stop_day,m.month_end))) AS numerator,
       max(m.days)::HUGEINT AS days,list_sort(list_distinct(list(s.activity_id))) AS upstream_ids
      FROM @subscription_states s JOIN @calendar m ON s.start_day<m.month_end AND s.stop_day>m.month
      GROUP BY 1,2,3,4''')
    g.add('subscription_amounts', '''SELECT *,
      'subscription:'||subscription_id||':'||month::VARCHAR AS activity_id,
      ((2*abs(numerator)+days)//(2*days)*sign(numerator))::BIGINT AS amount FROM @subscription_months''')
    g.add('recognized', '''WITH earnings AS (
      SELECT activity_id,_source,customer,month,'commit_drawdown' AS source,'usage' AS recognition_kind,
        f.invoice_id AS source_document,f.channel AS channel,draw AS revenue_cents,upstream_ids FROM @invoice_amounts
      UNION ALL SELECT activity_id,_source,customer,month,'usage','usage',f.invoice_id,f.channel,net-fee-draw,upstream_ids FROM @invoice_amounts
      UNION ALL SELECT activity_id,_source,customer,month,'commit_drawdown','commit_expiry',f.contract_id,channel,amount,upstream_ids FROM @expiry_amounts
      UNION ALL SELECT activity_id,_source,customer,f.period,'usage','credit',f.credit_id,channel,-amount,upstream_ids FROM @credit_amounts
      UNION ALL SELECT activity_id,_source,customer,month,'subscription','subscription',subscription_id,'direct',amount,upstream_ids FROM @subscription_amounts)
      SELECT activity_id||':'||source||':'||recognition_kind||':'||month::VARCHAR AS activity_id,
        _source,customer,month,source,recognition_kind,source_document,channel,revenue_cents::BIGINT AS revenue_cents,upstream_ids
      FROM earnings WHERE revenue_cents<>0 AND month<(SELECT period_end FROM @context)''')
    # Net the two receivable postings before expansion. No duplicate line aggregation.
    g.add('journals', '''WITH postings AS (
      SELECT c.activity_id,c._source,c.customer,date_trunc('month',c.ts)::DATE AS month,
        ['1100','2100'] AS accounts,[(c.f.commit_usd*100)::BIGINT,(-c.f.commit_usd*100)::BIGINT] AS amounts,
        [c.activity_id] AS upstream_ids FROM @contracts c
      UNION ALL SELECT activity_id,_source,customer,month,['2100','1100','4000','4095'],
        [draw,net-draw-fee,-net,fee],upstream_ids FROM @invoice_amounts
      UNION ALL SELECT activity_id,_source,customer,month,['2100','4020'],[amount,-amount],upstream_ids FROM @expiry_amounts
      UNION ALL SELECT activity_id,_source,customer,f.period,['4090','1100'],[amount,-amount],upstream_ids FROM @credit_amounts
      UNION ALL SELECT activity_id,_source,customer,month,['1100','4010'],[amount,-amount],upstream_ids FROM @subscription_amounts),
      expanded AS (SELECT activity_id,_source,customer,month,unnest(accounts) AS account,unnest(amounts)::BIGINT AS amount_cents,upstream_ids FROM postings)
      SELECT * FROM expanded WHERE amount_cents<>0''')
    g.add('accounting_errors', '''
      SELECT 'duplicate_invoice' AS reason,count(*)-count(DISTINCT (_source,f.invoice_id)) AS failures FROM @invoices
      UNION ALL SELECT 'duplicate_contract',count(*)-count(DISTINCT (_source,f.contract_id)) FROM @contracts
      UNION ALL SELECT 'duplicate_credit',count(*)-count(DISTINCT (_source,f.credit_id)) FROM @credits
      UNION ALL SELECT 'duplicate_expiry',count(*)-count(DISTINCT (_source,f.contract_id)) FROM @expiries
      UNION ALL SELECT 'invalid_invoice',count(*) FROM @funding WHERE f.period_end<=f.period_start OR net_cents<0
        OR fee_cents<0 OR fee_cents>net_cents OR f.gross_usd-f.discount_usd<>f.net_usd
        OR (f.contract_id IS NOT NULL AND contract_activity IS NULL)
        OR f.period_start<contract_start OR f.period_end>contract_end OR (draw_cents>0 AND fee_cents<>0)
      UNION ALL SELECT 'orphan_credit',count(*) FROM @credit_amounts WHERE invoice_activity IS NULL
      UNION ALL SELECT 'invalid_expiry',count(*) FROM @expiry_amounts WHERE contract_activity IS NULL OR last_service>ts::DATE
      UNION ALL SELECT 'nonmonthly_subscription',count(*) FROM @subscriptions WHERE coalesce(f.billing_period,'monthly')<>'monthly'
    ''')
