-- Optional diagnostic. No correction, source write or reporting gate change.
WITH inspected AS (
  SELECT q.case_id, q.population, q.requested_date,
         count(r.case_id) AS population_rows,
         count(DISTINCT r.month) AS reporting_dates,
         min(r.month) AS reporting_date,
         count(DISTINCT CASE WHEN r.lens IN
           ('base_t12m','floor_100k','subscription_inclusive','t3m_annualized') THEN r.lens END) AS registered_lenses,
         CAST(sum(CASE WHEN r.month=q.requested_date THEN 1 ELSE 0 END) AS BIGINT) AS selected_rows,
         CAST(sum(CASE WHEN r.case_id IS NOT NULL AND (r.status<>'defined' OR r.nrr IS NULL) THEN 1 ELSE 0 END) AS BIGINT) AS unavailable_rows
  FROM requests q LEFT JOIN report_rows r ON r.case_id=q.case_id
  GROUP BY q.case_id,q.population,q.requested_date
), classified AS (
  SELECT *, CASE
    WHEN population<>'nrr' THEN 'unsupported_population'
    WHEN population_rows=0 THEN 'no_population'
    WHEN reporting_dates<>1 THEN 'ambiguous_population'
    WHEN population_rows<>4 OR registered_lenses<>4 THEN 'incomplete_population'
    WHEN selected_rows>0 AND unavailable_rows>0 THEN 'selected_unavailable'
    WHEN selected_rows>0 THEN 'selected'
    WHEN requested_date=CAST(date_trunc('month',reporting_date) AS DATE) THEN 'date_grain_mismatch'
    ELSE 'no_matching_date' END AS status
  FROM inspected
)
SELECT case_id,status,
       CASE WHEN status='date_grain_mismatch' THEN CAST(reporting_date AS VARCHAR) END AS suggested_date,
       population_rows,selected_rows,unavailable_rows
FROM classified ORDER BY case_id
