select 'base_t12m' lens,12 AS months,1 annualization,false include_subscription,{{var('nrr_floor_cents')}}::bigint floor_cents
union all select 't3m_annualized',3,4,false,1000000
union all select 'floor_100k',12,1,false,10000000
union all select 'subscription_inclusive',12,1,true,1000000
