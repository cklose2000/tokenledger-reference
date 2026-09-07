"""Fail closed before emitting disclosure rows. All checks use the frozen snapshot."""

from pathlib import Path
from tl.stream.events import Catalog,ValidationError


def verify(session):
    conn=session.conn
    if session.snapshot.num_rows == 0:
        raise ValidationError('no activities at the requested reporting cutoffs')
    hashes=conn.execute('SELECT DISTINCT _schema_hash FROM _snapshot').fetchall()
    if hashes != [(Catalog(Path('definitions/activities')).digest,)]:
        raise ValidationError('reporting requires the retained activity registry matching every input row')
    checks={
      'unique_customer_creation': "SELECT count(*) FROM (SELECT customer FROM _snapshot WHERE activity='customer_created' GROUP BY 1 HAVING count(*)!=1)",
      'unique_trial_cohorts': """SELECT count(*) FROM (SELECT _source,json_extract_string(feature_json,'$.trial_id') AS trial_id
        FROM _snapshot WHERE activity='trial_started' GROUP BY 1,2 HAVING count(*)>1)""",
      'parent_graph_resolves': "SELECT abs((SELECT count(*) FROM _snapshot WHERE activity='customer_created')-(SELECT count(*) FROM report.customer_timeline))",
      'parents_exist': "SELECT count(*) FROM report.customer_timeline c WHERE NOT EXISTS(SELECT 1 FROM report.customer_timeline p WHERE p.customer=c.ultimate_parent)",
      'token_identity_and_allocation_complete': "SELECT abs((SELECT count(*) FROM _snapshot WHERE activity='tokens_processed')-(SELECT count(*) FROM report.token_ledger))",
      'unique_token_allocation': "SELECT count(*) FROM (SELECT activity_id FROM report.token_ledger GROUP BY 1 HAVING count(*)!=1)",
      'all_recognition_mapped': "SELECT abs((SELECT count(*) FROM _snapshot WHERE activity='revenue_recognized')-(SELECT count(*) FROM report.revenue_ledger))",
      'meters_match_contracts': "SELECT abs((SELECT count(*) FROM _snapshot WHERE activity='capacity_consumed')-(SELECT count(*) FROM report.compute_ledger WHERE row_kind='consumption'))",
      'unique_source_documents': """SELECT count(*) FROM (
        SELECT _source,activity,coalesce(json_extract_string(feature_json,'$.invoice_id'),json_extract_string(feature_json,'$.capacity_id'),json_extract_string(feature_json,'$.model')) AS document
        FROM _snapshot WHERE activity IN ('usage_invoiced','capacity_contracted','model_released') GROUP BY 1,2,3 HAVING count(*)>1)""",
      'one_invoice_per_usage_batch': """SELECT count(*) FROM (SELECT _source,json_extract_string(feature_json,'$.usage_batch_id') AS batch
        FROM _snapshot WHERE activity='usage_invoiced' GROUP BY 1,2 HAVING count(*)>1)""",
      'usage_batches_belong_to_one_customer': """SELECT count(*) FROM (SELECT _source,json_extract_string(feature_json,'$.usage_batch_id') AS batch
        FROM _snapshot WHERE activity IN ('tokens_processed','usage_invoiced') GROUP BY 1,2 HAVING count(DISTINCT customer)>1)""",
      'recognition_month': """SELECT count(*) FROM report.revenue_ledger WHERE month!=date_trunc('month',ts)::DATE""",
      'recognition_has_source_document': """SELECT count(*) FROM report.revenue_ledger r WHERE NOT EXISTS (
        SELECT 1 FROM _snapshot s WHERE s._source=r._source AND s.customer=r.customer AND s.ts<=r.ts AND (
          (r.recognition_kind='usage' AND s.activity='usage_invoiced' AND json_extract_string(s.feature_json,'$.invoice_id')=r.source_document
            AND CAST(json_extract_string(s.feature_json,'$.period_start') AS DATE)<=r.ts::DATE AND CAST(json_extract_string(s.feature_json,'$.period_end') AS DATE)>r.ts::DATE)
          OR (r.recognition_kind='subscription' AND s.activity IN ('subscription_started','subscription_renewed','subscription_upgraded','subscription_downgraded')
            AND json_extract_string(s.feature_json,'$.subscription_id')=r.source_document AND CAST(json_extract_string(s.feature_json,'$.period_start') AS DATE)<=r.ts::DATE
            AND CAST(json_extract_string(s.feature_json,'$.period_end') AS DATE)>r.ts::DATE)
          OR (r.recognition_kind='credit' AND s.activity='credit_issued' AND json_extract_string(s.feature_json,'$.credit_id')=r.source_document
            AND CAST(json_extract_string(s.feature_json,'$.period') AS DATE)=r.month)
          OR (r.recognition_kind='commit_expiry' AND s.activity='contract_expired' AND json_extract_string(s.feature_json,'$.contract_id')=r.source_document
            AND date_trunc('month',s.ts)::DATE=r.month)))""",
      'revenue_reconciles_by_month': """WITH a AS (SELECT date_trunc('month',ts)::DATE AS month,sum(revenue_impact*100)::HUGEINT AS cents FROM _snapshot WHERE activity='revenue_recognized' GROUP BY 1),
        b AS (SELECT month,sum(revenue_cents)::HUGEINT AS cents FROM report.revenue_ledger GROUP BY 1),
        c AS (SELECT month,sum(net_revenue_cents)::HUGEINT AS cents FROM report.customer_month GROUP BY 1)
        SELECT count(*) FROM a FULL OUTER JOIN b USING(month) FULL OUTER JOIN c USING(month)
        WHERE coalesce(a.cents,0)!=coalesce(b.cents,0) OR coalesce(b.cents,0)!=coalesce(c.cents,0)""",
      'usage_allocations_reconcile': """WITH a AS (SELECT _source,usage_batch_id,sum(revenue_cents)::HUGEINT AS cents FROM report.revenue_ledger WHERE family='consumption' GROUP BY 1,2),
        b AS (SELECT _source,usage_batch_id,sum(net_revenue_cents)::HUGEINT AS cents FROM report.token_ledger GROUP BY 1,2)
        SELECT count(*) FROM a FULL OUTER JOIN b USING(_source,usage_batch_id) WHERE coalesce(a.cents,0)!=coalesce(b.cents,0)""",
      'inference_cost_allocations_reconcile': """WITH a AS (SELECT month,model,sum(cost_cents)::HUGEINT AS cents FROM report.compute_ledger WHERE purpose='inference' GROUP BY 1,2),
        b AS (SELECT month,model,sum(allocated_cost_cents)::HUGEINT AS cents FROM report.token_ledger GROUP BY 1,2)
        SELECT count(*) FROM a FULL OUTER JOIN b USING(month,model) WHERE coalesce(a.cents,0)!=coalesce(b.cents,0)""",
      'subscription_allocations_reconcile': """WITH a AS (SELECT _source,customer,source_document AS subscription_id,month,sum(revenue_cents)::HUGEINT AS cents FROM report.revenue_ledger WHERE family='subscription' GROUP BY 1,2,3,4),
        b AS (SELECT _source,customer,subscription_id,month,sum(revenue_cents)::HUGEINT AS cents FROM report.seat_ledger GROUP BY 1,2,3,4)
        SELECT count(*) FROM a FULL OUTER JOIN b USING(_source,customer,subscription_id,month) WHERE coalesce(a.cents,0)!=coalesce(b.cents,0)""",
      'seat_rollforward': "SELECT count(*) FROM report.seat_ledger WHERE beginning_seats+gross_adds-seat_removals-cancelled_seats+transfer_in-transfer_out!=ending_seats",
      'paid_state_has_start': """SELECT count(*) FROM _snapshot e WHERE e.activity IN ('subscription_renewed','subscription_upgraded','subscription_downgraded','subscription_cancelled','seat_added','seat_removed')
        AND NOT EXISTS(SELECT 1 FROM _snapshot s WHERE s.activity='subscription_started' AND s._source=e._source AND s.customer=e.customer AND s.ts<=e.ts
          AND json_extract_string(s.feature_json,'$.subscription_id')=json_extract_string(e.feature_json,'$.subscription_id'))""",
      'subscription_exposure_supported': """WITH events AS (
        SELECT _source,customer,json_extract_string(feature_json,'$.subscription_id') AS subscription_id,ts,activity_id,
          CASE WHEN activity='subscription_cancelled' THEN 0 WHEN activity IN ('seat_added','seat_removed') THEN CAST(json_extract_string(feature_json,'$.seats_after') AS BIGINT)
            ELSE CAST(json_extract_string(feature_json,'$.seats') AS BIGINT) END AS seats,
          CASE WHEN activity IN ('seat_added','seat_removed') THEN 20 WHEN activity IN ('subscription_upgraded','subscription_downgraded') THEN 10 WHEN activity='subscription_cancelled' THEN 30 ELSE 0 END AS priority
        FROM _snapshot WHERE activity IN ('subscription_started','subscription_renewed','subscription_upgraded','subscription_downgraded','subscription_cancelled','seat_added','seat_removed')
      ), intervals AS (
        SELECT *,ts::DATE AS start_day,lead(ts::DATE,1,(SELECT period_end FROM _context)) OVER(PARTITION BY _source,customer,subscription_id ORDER BY ts,priority,activity_id) AS end_day FROM events
      ) SELECT count(*) FROM intervals a JOIN intervals b ON a.customer=b.customer
        AND (a._source!=b._source OR a.subscription_id!=b.subscription_id)
        AND greatest(a.start_day,b.start_day)<least(a.end_day,b.end_day) WHERE a.seats>0 AND b.seats>0""",
      'recognized_subscription_has_exposure': "SELECT count(*) FROM report.seat_ledger WHERE revenue_cents!=0 AND seat_days=0",
    }
    result={}
    # v1 has no independent recognition-level fee schedule. Its add-back is
    # supported only for fully recognized invoices (or not-yet-recognized ones).
    fee_errors = conn.execute("""WITH invoices AS (
      SELECT activity_id,_source,json_extract_string(feature_json,'$.invoice_id') AS invoice_id,
        CAST(json_extract_string(feature_json,'$.net_usd') AS DECIMAL(18,2))*100 AS net_cents,
        CAST(json_extract_string(feature_json,'$.channel_fee_usd') AS DECIMAL(18,2))*100 AS fee_cents
      FROM _snapshot WHERE activity='usage_invoiced'
    ), earned AS (
      SELECT _source,source_document,sum(revenue_cents) AS cents,list(activity_id) AS recognition_ids
      FROM report.revenue_ledger WHERE family='consumption' GROUP BY 1,2
    ) SELECT i.activity_id,e.recognition_ids FROM invoices i JOIN earned e
      ON i._source=e._source AND i.invoice_id=e.source_document
      WHERE e.cents!=i.net_cents-i.fee_cents OR (i.fee_cents>0 AND i.net_cents=i.fee_cents)
      ORDER BY i.activity_id LIMIT 20""").fetchall()
    if fee_errors:
        raise ValidationError(f'unsupported invoice recognition/fee basis; responsible activity_ids: {fee_errors}')
    result['invoice_recognition_fee_basis']=dict(passed=True,violations=0,
        policy='v1 fully recognized net-minus-fee; partial and positive-fee zero bases unsupported')
    for name,query in checks.items():
        violations=conn.execute(query).fetchone()[0]
        result[name]=dict(passed=violations==0,violations=int(violations))
        if violations:
            raise ValidationError(f'reporting invariant failed: {name} ({violations} violations)')
    return result
