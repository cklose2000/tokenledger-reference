-- Primary customer creation; append latest known relationships and commercial state.
WITH RECURSIVE created AS (
  SELECT customer, ts AS created_at, activity_id,
         json_extract_string(feature_json,'$.segment') AS segment,
         json_extract_string(feature_json,'$.channel') AS channel,
         json_extract_string(feature_json,'$.country') AS country,
         json_extract_string(feature_json,'$.parent_customer') AS parent
  FROM _snapshot WHERE activity='customer_created'
), changes AS (
  SELECT customer, ts, activity_id, json_extract_string(feature_json,'$.ultimate_parent') AS parent
  FROM _snapshot WHERE activity='customer_merged'
  UNION ALL SELECT customer, created_at, activity_id, parent FROM created
), edges AS (
  SELECT customer, parent FROM changes
  QUALIFY row_number() OVER (PARTITION BY customer ORDER BY ts DESC, activity_id DESC)=1
), walk(customer,node,path) AS (
  SELECT customer, customer, [customer] FROM created
  UNION ALL
  SELECT w.customer, e.parent, list_append(w.path,e.parent)
  FROM walk w JOIN edges e ON e.customer=w.node
  WHERE e.parent IS NOT NULL AND NOT list_contains(w.path,e.parent)
), parents AS (
  SELECT w.customer, w.node AS ultimate_parent FROM walk w
  LEFT JOIN edges e ON e.customer=w.node WHERE e.parent IS NULL
), activity_times AS (
  SELECT customer, min(ts) AS first_activity, max(ts) AS last_activity,
         max(CASE WHEN activity='customer_churned' THEN ts END) AS last_churn
  FROM _snapshot GROUP BY customer
), contracts AS (
  SELECT customer, json_extract_string(feature_json,'$.contract_id') AS contract_id,
         activity AS contract_state, ts AS contract_state_at
  FROM _snapshot WHERE activity IN ('contract_signed','contract_renewed','contract_expired')
  QUALIFY row_number() OVER (PARTITION BY customer ORDER BY ts DESC,activity_id DESC)=1
), subscriptions AS (
  SELECT customer, activity AS subscription_state, ts AS subscription_state_at
  FROM _snapshot WHERE activity IN ('subscription_started','subscription_renewed','subscription_cancelled')
  QUALIFY row_number() OVER (PARTITION BY customer ORDER BY ts DESC,activity_id DESC)=1
)
SELECT c.customer, c.segment, c.channel, c.country, p.ultimate_parent,
       a.first_activity, a.last_activity, a.last_churn,
       k.contract_id, k.contract_state, k.contract_state_at,
       s.subscription_state, s.subscription_state_at
FROM created c JOIN parents p USING(customer) JOIN activity_times a USING(customer)
LEFT JOIN contracts k USING(customer) LEFT JOIN subscriptions s USING(customer)
