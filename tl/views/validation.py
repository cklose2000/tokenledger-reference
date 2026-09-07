"""Demand-scoped reporting checks. Faster execution cannot skip data controls."""


def register(g):
    g.add('core_errors', '''SELECT * FROM @accounting_errors
      UNION ALL SELECT 'unique_customer_creation',count(*) FROM (
        SELECT customer FROM _visible WHERE activity='customer_created' GROUP BY 1 HAVING count(*)<>1)
      UNION ALL SELECT 'parent_graph_resolves',abs((SELECT count(*) FROM _visible WHERE activity='customer_created')-(SELECT count(*) FROM @customer_identity))
      UNION ALL SELECT 'parents_exist',count(*) FROM @customer_identity c ANTI JOIN @customer_identity p ON c.ultimate_parent=p.customer
      UNION ALL SELECT 'financial_customer_exists',count(*) FROM (
        SELECT customer FROM @invoices UNION ALL SELECT customer FROM @contracts UNION ALL SELECT customer FROM @subscriptions UNION ALL SELECT customer FROM @credits
      ) e ANTI JOIN @customer_identity c USING(customer)
      UNION ALL SELECT 'one_invoice_per_batch',count(*) FROM (SELECT _source,f.usage_batch_id FROM @invoices GROUP BY 1,2 HAVING count(*)>1)
      UNION ALL SELECT 'paid_state_has_start',count(*) FROM @subscriptions e WHERE e.activity<>'subscription_started' AND NOT EXISTS (
        SELECT 1 FROM @subscriptions s WHERE s.activity='subscription_started' AND s._source=e._source AND s.customer=e.customer
          AND s.f.subscription_id=e.f.subscription_id AND s.ts<=e.ts)
      UNION ALL SELECT 'overlapping_paid_subscriptions',count(*) FROM @subscription_states a JOIN @subscription_states b
        ON a.customer=b.customer AND (a._source<>b._source OR a.f.subscription_id<>b.f.subscription_id)
        AND greatest(a.start_day,b.start_day)<least(a.stop_day,b.stop_day) WHERE a.paid_seats>0 AND b.paid_seats>0
    ''')
    g.add('compute_errors', '''SELECT 'meters_match_contracts' AS reason,
      abs((SELECT count(*) FROM _visible WHERE activity='capacity_consumed')-(SELECT count(*) FROM @compute_ledger WHERE row_kind='consumption')) AS failures
      UNION ALL SELECT 'unique_capacity_documents',count(*) FROM (
        SELECT _source,json_extract_string(feature_json,'$.capacity_id') FROM _visible WHERE activity='capacity_contracted' GROUP BY 1,2 HAVING count(*)>1)
    ''')
    g.add('token_errors', '''WITH tokens AS MATERIALIZED (SELECT * FROM @token_ledger),
      earned AS (SELECT i._source,i.f.usage_batch_id AS usage_batch_id,i.month,sum(i.net-i.fee)::HUGEINT AS cents,sum(i.fee)::HUGEINT AS fees FROM @invoice_amounts i GROUP BY 1,2,3),
      allocated AS (SELECT _source,usage_batch_id,month,sum(net_revenue_cents)::HUGEINT AS cents,sum(channel_fee_cents)::HUGEINT AS fees FROM tokens GROUP BY 1,2,3),
      compute AS (SELECT month,model,sum(cost_cents)::HUGEINT AS cents FROM @compute_ledger WHERE row_kind='consumption' AND purpose='inference' GROUP BY 1,2),
      cost_allocated AS (SELECT month,model,sum(allocated_cost_cents)::HUGEINT AS cents FROM tokens GROUP BY 1,2)
      SELECT 'token_identity_and_allocation_complete' AS reason,abs((SELECT count(*) FROM @usage)-(SELECT count(*) FROM tokens)) AS failures
      UNION ALL SELECT 'unique_token_allocation',count(*) FROM (SELECT activity_id FROM tokens GROUP BY 1 HAVING count(*)<>1)
      UNION ALL SELECT 'usage_allocations_reconcile',count(*) FROM earned a FULL JOIN allocated b USING(_source,usage_batch_id,month)
        WHERE coalesce(a.cents,0)<>coalesce(b.cents,0) OR coalesce(a.fees,0)<>coalesce(b.fees,0)
      UNION ALL SELECT 'inference_cost_allocations_reconcile',count(*) FROM compute a FULL JOIN cost_allocated b USING(month,model) WHERE coalesce(a.cents,0)<>coalesce(b.cents,0)
      UNION ALL SELECT 'usage_batch_customer',count(*) FROM (
        SELECT _source,batch FROM (SELECT _source,customer,f.usage_batch_id AS batch FROM @usage UNION ALL SELECT _source,customer,f.usage_batch_id AS batch FROM @invoices)
        GROUP BY 1,2 HAVING count(DISTINCT customer)>1)
      UNION ALL SELECT 'unique_model_catalog',count(*) FROM (SELECT json_extract_string(feature_json,'$.model') FROM _visible WHERE activity='model_released' GROUP BY 1 HAVING count(*)>1)
    ''')
    g.add('seat_errors', '''WITH seats AS MATERIALIZED (SELECT * FROM @seat_ledger),
      earned AS (SELECT _source,customer,subscription_id,month,amount AS cents FROM @subscription_amounts),
      allocated AS (SELECT _source,customer,subscription_id,month,sum(revenue_cents)::HUGEINT AS cents FROM seats GROUP BY 1,2,3,4)
      SELECT 'subscription_allocations_reconcile' AS reason,count(*) AS failures FROM earned a FULL JOIN allocated b USING(_source,customer,subscription_id,month)
        WHERE coalesce(a.cents,0)<>coalesce(b.cents,0)
      UNION ALL SELECT 'seat_rollforward',count(*) FROM seats WHERE beginning_seats+gross_adds-seat_removals-cancelled_seats+transfer_in-transfer_out<>ending_seats
      UNION ALL SELECT 'revenue_has_exposure',count(*) FROM seats WHERE revenue_cents<>0 AND seat_days=0
      UNION ALL SELECT 'unique_trials',count(*) FROM (SELECT _source,json_extract_string(feature_json,'$.trial_id') FROM _visible WHERE activity='trial_started' GROUP BY 1,2 HAVING count(*)>1)
    ''')
    g.add('journal_errors', '''WITH balances AS (SELECT month,sum(amount_cents)::HUGEINT AS cents FROM @journals GROUP BY 1),
      revenue AS (SELECT month,sum(revenue_cents)::HUGEINT AS cents FROM @recognized GROUP BY 1),
      journal AS (SELECT month,-sum(amount_cents)::HUGEINT AS cents FROM @journals WHERE account IN ('4000','4010','4020','4090','4095') GROUP BY 1)
      SELECT 'journal_balance' AS reason,count(*) AS failures FROM balances WHERE cents<>0
      UNION ALL SELECT 'recognized_revenue_bridge',count(*) FROM revenue r FULL JOIN journal j USING(month) WHERE coalesce(r.cents,0)<>coalesce(j.cents,0)
    ''')


def required(names):
    domains={'core_errors'}
    if set(names)&{'net_rev_per_mtok','cost_per_mtok','model_vintage'}: domains.update(('token_errors','compute_errors'))
    if 'rev_per_mw' in names: domains.add('compute_errors')
    if 'subscriptions' in names: domains.add('seat_errors')
    if set(names)&{'journal_lines','close_accounts','close_rollforward','fact_revenue'}: domains.add('journal_errors')
    return domains
