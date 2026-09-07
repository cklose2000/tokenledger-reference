CREATE VIEW usage_observations AS
WITH typed AS (
 SELECT activity_id,customer AS account,ts,_recorded_at,
   from_json(feature_json,'{"source":"VARCHAR","scope_id":"VARCHAR","token_kind":"VARCHAR","temporality":"VARCHAR","quantity":"BIGINT","observation_id":"VARCHAR","model":"VARCHAR","revision":"INTEGER"}') AS f
 FROM _snapshot WHERE activity='usage_observed'
)
SELECT activity_id,account,ts,_recorded_at,f.source AS source,f.scope_id AS scope_id,
 f.token_kind AS token_kind,f.temporality AS temporality,f.quantity AS quantity,
 f.model AS model,f.revision AS revision,
 try_cast(split_part(f.observation_id,':',3) AS BIGINT) AS source_line
FROM typed WHERE f.source='codex-local-token-count/v1';
