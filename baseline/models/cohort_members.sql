select w.*,t.threshold_cents from {{ref('int_cohort_windows')}} w
cross join (select unnest([10000000,100000000,1000000000])::bigint threshold_cents) t where w.revenue_cents>t.threshold_cents
