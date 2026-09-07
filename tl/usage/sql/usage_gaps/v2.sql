WITH registry AS (
 SELECT customer AS account,ts AS verified_at,
 from_json(feature_json,'{{"provider":"VARCHAR","surface":"VARCHAR","scope":"VARCHAR"}}') AS f
 FROM _snapshot WHERE activity='application_inventory'
 QUALIFY row_number() OVER (PARTITION BY customer,json_extract_string(feature_json,'$.surface') ORDER BY ts DESC,activity_id DESC)=1
), observed AS (
 SELECT account,max(ts) AS latest_observed_at,max(_recorded_at) AS latest_imported_at
 FROM usage_observations CROSS JOIN _usage_window
 WHERE ts>=start_utc AND ts<end_utc GROUP BY account
), surfaces AS (
 SELECT r.account,r.f.provider AS provider,r.f.surface AS surface,r.f.scope AS scope,
 'surface_coverage' AS gap,r.verified_at,o.latest_observed_at,
 CASE WHEN r.f.surface='codex-cli' AND o.account IS NOT NULL
 THEN 'Observe remaining sessions; retain unknown opening, boundary and tail intervals'
 ELSE 'Collect a permitted usage source for this confirmed surface' END AS next_action,
 CASE WHEN r.f.surface='codex-cli' AND o.account IS NOT NULL THEN
   CASE WHEN o.latest_imported_at < known_at - INTERVAL 7 DAY THEN 'stale' ELSE 'partial' END
 ELSE 'unavailable' END AS status
 FROM registry r CROSS JOIN _usage_window LEFT JOIN observed o USING(account)
)
SELECT * FROM surfaces
UNION ALL
SELECT DISTINCT account,provider,'all','account','account_total',NULL::TIMESTAMPTZ,NULL::TIMESTAMPTZ,
 'Account-wide coverage and cross-session lineage are unresolved; do not sum session counters','unavailable'
FROM surfaces
UNION ALL
SELECT DISTINCT account,provider,'all','billing','billed_charges',NULL::TIMESTAMPTZ,NULL::TIMESTAMPTZ,
 'Import an actual charge source; subscription fees and costs are not inferred from tokens','unavailable'
FROM surfaces
UNION ALL
SELECT customer,'openai','codex-cli',json_extract_string(feature_json,'$.scope_id'),
 'quarantined_source',_recorded_at,NULL::TIMESTAMPTZ,
 json_extract_string(feature_json,'$.reason') || '. ' || json_extract_string(feature_json,'$.next_action'),
 'quarantined'
FROM _snapshot CROSS JOIN _usage_window WHERE activity='usage_coverage'
 AND starts_with(link,'evidence:failures/')
 AND json_extract_string(feature_json,'$.source')='codex-local-token-count/v1'
 AND cast(json_extract_string(feature_json,'$.interval_start') AS TIMESTAMPTZ)<end_utc
 AND cast(json_extract_string(feature_json,'$.interval_end') AS TIMESTAMPTZ)>start_utc
UNION ALL
SELECT account,'openai','codex-cli',scope_id,'last_counter_reconciliation',
 max(imported_at),max(observed_at),
 'Last-counter diagnostics do not reconcile and are excluded from usage totals; inspect the retained source',
 'diagnostic_mismatch'
FROM (
 SELECT account,scope_id,ts AS observed_at,max(_recorded_at) AS imported_at,
 max(quantity::HUGEINT) FILTER (WHERE token_kind='total') AS total,
 max(quantity::HUGEINT) FILTER (WHERE token_kind='input') AS input,
 max(quantity::HUGEINT) FILTER (WHERE token_kind='output') AS output
 FROM usage_observations CROSS JOIN _usage_window
 WHERE temporality='interval_total' AND ts>=start_utc AND ts<end_utc
 GROUP BY account,scope_id,ts,source_line
) diagnostics WHERE total IS NOT NULL AND input IS NOT NULL AND output IS NOT NULL
 AND total!=input+output GROUP BY account,scope_id
ORDER BY account,gap,surface;
