SELECT account,source,scope_id,token_kind,'local_cumulative_counter_growth' AS basis,
 'unknown' AS model,'unavailable' AS cost_basis,
 min(ts) AS first_observed_at,max(ts) AS last_observed_at,
 CASE WHEN count(*) FILTER (WHERE interval_status IN ('missing_counter','counter_reset'))>0
 THEN NULL ELSE sum(observed_tokens) END AS observed_tokens,
 count(*) AS observations,
 count(*) FILTER (WHERE interval_status='observed') AS measured_intervals,
 count(*) FILTER (WHERE interval_status!='observed') AS excluded_intervals,
 CASE WHEN count(*) FILTER (WHERE interval_status='counter_reset')>0 THEN 'counter_reset_unknown'
 WHEN count(*) FILTER (WHERE interval_status='missing_counter')>0 THEN 'missing_counter_unknown'
 WHEN count(observed_tokens)=0 THEN 'insufficient_observations'
 ELSE 'partial_observed' END AS status
FROM usage_intervals GROUP BY account,source,scope_id,token_kind
ORDER BY account,source,scope_id,token_kind;
