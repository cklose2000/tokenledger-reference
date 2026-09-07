"""Re-perform schema validation and cross-event checks without writing tables."""

from collections import Counter
import json
from pathlib import Path

from tl.reporting.policy import RecognitionPolicy
from .events import Activity, Catalog, ValidationError
from .store import StreamReader


def validate_stream(path, definitions, *, source_manifest=None):
    catalog = Catalog(Path(definitions))
    policy = RecognitionPolicy()
    counts = Counter()
    errors = []
    with StreamReader(path).connect() as conn:
        cursor = conn.execute("""SELECT activity_id, ts, customer, anonymous_customer_id, activity,
                                        feature_json::VARCHAR, revenue_impact, link, _recorded_at, _schema_hash
                                 FROM stream.activity ORDER BY _stream_position""")
        while rows := cursor.fetchmany(8192):
            for key, ts, customer, anonymous, activity, features, revenue, link, arrival, schema_hash in rows:
                counts[activity] += 1
                try:
                    catalog.validate(Activity(key, ts, activity, json.loads(features), customer, anonymous, revenue, link))
                    if arrival < ts:
                        raise ValidationError("arrival precedes event")
                    if schema_hash != catalog.digest:
                        raise ValidationError("registry hash mismatch; retain the original registry for validation")
                except (ValidationError, ValueError) as exc:
                    if len(errors) < 100:
                        errors.append({"activity_id": key, "error": str(exc)})
        checks = {}
        queries = {
            "customer_exists": """
              SELECT count(*) FROM stream.activity e
              WHERE e.customer IS NOT NULL AND e.customer != 'internal:synthetic-lab'
                AND NOT EXISTS (SELECT 1 FROM stream.activity c WHERE c.activity='customer_created'
                                AND c.customer=e.customer AND c.ts<=e.ts)
            """,
            "usage_revenue_ties_to_invoice": """
              WITH earned AS (
                SELECT _source, json_extract_string(feature_json, '$.source_document') AS document,
                       sum(revenue_impact) AS revenue
                FROM stream.activity WHERE activity='revenue_recognized'
                  AND json_extract_string(feature_json, '$.recognition_kind')='usage'
                GROUP BY 1, 2
              ), invoices AS (
                SELECT _source, json_extract_string(feature_json, '$.invoice_id') AS document,
                       CAST(json_extract_string(feature_json,'$.net_usd') AS DECIMAL(18,2))
                       - CAST(json_extract_string(feature_json,'$.channel_fee_usd') AS DECIMAL(18,2)) AS net
                FROM stream.activity WHERE activity='usage_invoiced'
              )
              SELECT count(*) FROM invoices i FULL OUTER JOIN earned e USING (_source, document)
              WHERE i.document IS NULL OR i.net != coalesce(e.revenue, 0)
            """,
            "credits_have_exact_negative_recognition": """
              WITH credits AS (
                SELECT _source, json_extract_string(feature_json,'$.credit_id') AS document,
                       CAST(json_extract_string(feature_json,'$.amount_usd') AS DECIMAL(18,2)) AS amount
                FROM stream.activity WHERE activity='credit_issued'
              ), earned AS (
                SELECT _source, json_extract_string(feature_json,'$.source_document') AS document,
                       sum(revenue_impact) AS amount
                FROM stream.activity WHERE activity='revenue_recognized'
                  AND json_extract_string(feature_json,'$.recognition_kind')='credit'
                GROUP BY 1, 2
              )
              SELECT count(*) FROM credits c FULL OUTER JOIN earned e USING (_source, document)
              WHERE c.document IS NULL OR e.document IS NULL OR c.amount != -e.amount
            """,
            "no_self_mergers": """SELECT count(*) FROM stream.activity WHERE activity='customer_merged'
                 AND customer=json_extract_string(feature_json,'$.ultimate_parent')""",
            "service_periods_are_nonempty": """
              SELECT count(*) FROM stream.activity
              WHERE json_extract_string(feature_json,'$.period_start') IS NOT NULL
                AND CAST(json_extract_string(feature_json,'$.period_start') AS DATE)
                    >= CAST(json_extract_string(feature_json,'$.period_end') AS DATE)
            """,
            "recognition_period_matches_event_month": """
              SELECT count(*) FROM stream.activity WHERE activity='revenue_recognized'
                AND CAST(json_extract_string(feature_json,'$.period') AS DATE)
                    != CAST(date_trunc('month', ts) AS DATE)
            """,
            "recognition_source_documents_exist": """
              WITH documents AS (
                SELECT _source, customer, ts, 'usage' AS kind,
                       json_extract_string(feature_json,'$.invoice_id') AS document,
                       CAST(json_extract_string(feature_json,'$.period_start') AS DATE) AS start_date,
                       CAST(json_extract_string(feature_json,'$.period_end') AS DATE) AS end_date
                FROM stream.activity WHERE activity='usage_invoiced'
                UNION ALL
                SELECT _source, customer, ts, 'subscription',
                       json_extract_string(feature_json,'$.subscription_id'),
                       CAST(json_extract_string(feature_json,'$.period_start') AS DATE),
                       CAST(json_extract_string(feature_json,'$.period_end') AS DATE)
                FROM stream.activity WHERE activity IN
                  ('subscription_started','subscription_renewed','subscription_upgraded','subscription_downgraded')
                UNION ALL
                SELECT _source, customer, ts, 'credit',
                       json_extract_string(feature_json,'$.credit_id'),
                       CAST(json_extract_string(feature_json,'$.period') AS DATE),
                       CAST(json_extract_string(feature_json,'$.period') AS DATE) + INTERVAL '1 month'
                FROM stream.activity WHERE activity='credit_issued'
                UNION ALL
                SELECT _source, customer, ts, 'commit_expiry',
                       json_extract_string(feature_json,'$.contract_id'),
                       date_trunc('month', ts), date_trunc('month', ts) + INTERVAL '1 month'
                FROM stream.activity WHERE activity='contract_expired'
              )
              SELECT count(*) FROM stream.activity r WHERE r.activity='revenue_recognized'
                AND NOT EXISTS (
                  SELECT 1 FROM documents d WHERE d._source=r._source AND d.customer=r.customer
                    AND d.kind=json_extract_string(r.feature_json,'$.recognition_kind')
                    AND d.document=json_extract_string(r.feature_json,'$.source_document')
                    AND d.ts<=r.ts AND CAST(r.ts AS DATE)>=d.start_date AND CAST(r.ts AS DATE)<d.end_date
                )
            """,
            "commit_expiry_ties_to_unconsumed_balance": """
              WITH expired AS (
                SELECT _source, customer, date_trunc('month',ts) AS period,
                       json_extract_string(feature_json,'$.contract_id') AS document,
                       sum(CAST(json_extract_string(feature_json,'$.unconsumed_commit_usd') AS DECIMAL(18,2))) AS amount
                FROM stream.activity WHERE activity='contract_expired' GROUP BY 1,2,3,4
              ), earned AS (
                SELECT _source, customer, date_trunc('month',ts) AS period,
                       json_extract_string(feature_json,'$.source_document') AS document,
                       sum(revenue_impact) AS amount
                FROM stream.activity WHERE activity='revenue_recognized'
                  AND json_extract_string(feature_json,'$.recognition_kind')='commit_expiry' GROUP BY 1,2,3,4
              )
              SELECT count(*) FROM expired x FULL OUTER JOIN earned e USING (_source,customer,period,document)
              WHERE x.document IS NULL OR x.amount != coalesce(e.amount,0)
            """,
        }
        for name, query in queries.items():
            violations = conn.execute(query).fetchone()[0]
            checks[name] = {"passed": violations == 0, "violations": violations}
        pairs = sorted(policy.pairs)
        placeholders = ','.join('(?,?)' for _ in pairs)
        violations = conn.execute(f"""
          SELECT count(*) FROM stream.activity r WHERE r.activity='revenue_recognized'
            AND NOT EXISTS (SELECT 1 FROM (VALUES {placeholders}) p(source,kind)
              WHERE p.source=json_extract_string(r.feature_json,'$.source')
                AND p.kind=json_extract_string(r.feature_json,'$.recognition_kind'))
        """, [value for pair in pairs for value in pair]).fetchone()[0]
        checks["recognition_pairs_are_versioned"] = {"passed": violations == 0, "violations": violations}
        watermark = conn.execute("SELECT coalesce(max(_stream_position),0) FROM stream.activity").fetchone()[0]
        reconciliations = []
        if source_manifest is not None:
            paths = source_manifest if isinstance(source_manifest, list) else [source_manifest]
            for path in paths:
                manifest = json.loads(Path(path).read_text(encoding="utf-8"))
                if manifest.get('schema_version')=='tokenledger-source-capture/v1':
                    from tl.controls.sources import load_capture,reconcile
                    capture=load_capture(path,definitions)
                    table=conn.execute('SELECT * FROM stream.activity WHERE _source=?',[capture['source']]).fetch_arrow_table()
                    reconciliations.append(reconcile(table,[capture],start=capture['coverage_start'],end=capture['coverage_end']))
                    continue
                actual = conn.execute("""
                  SELECT strftime(ts AT TIME ZONE 'UTC','%Y-%m'), activity, count(*),
                         CAST(coalesce(sum(revenue_impact),0)*100 AS BIGINT)
                  FROM stream.activity WHERE _source=? GROUP BY 1,2 ORDER BY 1,2
                """, [manifest["source"]]).fetchall()
                expected = [(row["period"], row["activity"], row["count"], row["revenue_cents"]) for row in manifest["periods"]]
                reconciliations.append({"passed": actual == expected, "source": manifest["source"],
                                       "manifest": str(path), "period_activity_groups": len(actual)})
    passed = not errors and all(check["passed"] for check in checks.values())
    passed = passed and all(item["passed"] for item in reconciliations)
    return {"passed": passed, "rows": sum(counts.values()), "activity_counts": dict(sorted(counts.items())),
            "recognition_policy": {"version": policy.version, "sha256": policy.digest},
            "catalog_hash": catalog.digest, "watermark": watermark, "errors": errors,
            "checks": checks, "source_reconciliation": reconciliations}
