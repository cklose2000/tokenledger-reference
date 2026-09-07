"""The same fourteen physical fields; temporal fields remain derived."""
FIELDS = (
    ('activity_id', 'STRING', False), ('ts', 'TIMESTAMP', False),
    ('customer', 'STRING', True), ('anonymous_customer_id', 'STRING', True),
    ('activity', 'STRING', False), ('feature_json', 'JSON', False),
    ('revenue_impact', 'NUMERIC(18,2)', True), ('link', 'STRING', True),
    ('_recorded_at', 'TIMESTAMP', False), ('_stream_position', 'INT64', False),
    ('_source', 'STRING', False), ('_actor', 'STRING', False),
    ('_lane', 'STRING', False), ('_schema_hash', 'STRING', False),
)


def ddl(binding):
    columns = ',\n  '.join(name+' '+kind+('' if nullable else ' NOT NULL')
                           for name, kind, nullable in FIELDS)
    return f'''CREATE TABLE IF NOT EXISTS `{binding.table}` (
  {columns}
)
PARTITION BY DATE(ts)
CLUSTER BY customer, activity
OPTIONS (description='Synthetic ActivitySchema v2; validated append boundary; no CDC');
'''


def visible(binding, *, period_end, known_at, watermark):
    # All parameters originate from the validated shared StreamReader snapshot.
    return (f'SELECT * FROM `{binding.table}` WHERE ts < TIMESTAMP({repr(str(period_end))}) '
            f'AND _recorded_at < TIMESTAMP({repr(known_at)}) AND _stream_position <= {int(watermark)}')
