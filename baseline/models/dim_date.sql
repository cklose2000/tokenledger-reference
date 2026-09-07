select d::date AS month,(d+interval '1 month')::date AS month_end,
 date_diff('day',d,d+interval '1 month') AS days
from generate_series(date '{{var("first_month")}}',date '{{var("asof")}}',interval '1 month') t(d)
