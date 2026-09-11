"""Closed date diagnostic: no arbitrary SQL, date substitution or metric changes."""
from datetime import date
from pathlib import Path
import hashlib

import duckdb
import pyarrow as pa
import yaml

from tl.stream import ValidationError
from tl.views.session import literal, values

SQL = Path('tl/evolution/sql/nrr_date_diagnostic_v1.sql')
CASES = Path('definitions/reporting-learning/nrr-date-cases.v1.yaml')
LENSES = frozenset(('base_t12m', 'floor_100k', 'subscription_inclusive', 't3m_annualized'))
SCENARIOS = frozenset(('original', 'empty', 'missing_lens', 'duplicate_lens', 'multiple_dates', 'unavailable'))


def pins():
    return {p.as_posix(): hashlib.sha256(p.read_bytes().replace(b'\r\n', b'\n')).hexdigest() for p in (SQL, CASES)}


def cases():
    spec = yaml.safe_load(CASES.read_text(encoding='utf-8'))
    items = spec['cases']
    if len({r['case_id'] for r in items}) != len(items) or any(r['scenario'] not in SCENARIOS for r in items):
        raise ValidationError('invalid frozen diagnostic cases')
    return spec


def oracle(rows, requested_date, population='nrr'):
    """Scalar set-based reference, independent of the SQL candidate."""
    requested = date.fromisoformat(requested_date)
    normalized = [{**row, 'month': date.fromisoformat(str(row['month']))} for row in rows]
    dates = {row['month'] for row in normalized}
    selected = [row for row in normalized if row['month'] == requested]
    suggestion = None
    if population != 'nrr':
        status = 'unsupported_population'
    elif not normalized:
        status = 'no_population'
    elif len(dates) != 1:
        status = 'ambiguous_population'
    elif len(normalized) != len(LENSES) or {row['lens'] for row in normalized} != LENSES:
        status = 'incomplete_population'
    elif selected:
        status = ('selected_unavailable' if any(row['status'] != 'defined' or row['nrr'] is None for row in selected)
                  else 'selected')
    elif requested == next(iter(dates)).replace(day=1):
        status = 'date_grain_mismatch'
        suggestion = next(iter(dates)).isoformat()
    else:
        status = 'no_matching_date'
    return dict(status=status, suggested_date=suggestion, population_rows=len(rows), selected_rows=len(selected),
                unavailable_rows=sum(row['status'] != 'defined' or row['nrr'] is None for row in normalized))


def scenario(rows, name):
    """Frozen negative controls are explicit perturbations of the same report."""
    from datetime import timedelta
    rows = [dict(row) for row in rows]
    if name == 'original': return rows
    if name == 'empty': return []
    if name == 'missing_lens': return [row for row in rows if row['lens'] != 'base_t12m']
    if name == 'duplicate_lens': return rows + [row for row in rows if row['lens'] == 'base_t12m']
    if name == 'multiple_dates':
        return rows + [{**row, 'month': date.fromisoformat(str(row['month'])) - timedelta(days=31)} for row in rows]
    if name == 'unavailable': return [{**row, 'status': 'undefined_zero_base', 'nrr': None} for row in rows]
    raise ValidationError('unknown frozen diagnostic scenario')


def query(report_sql, requests, *, dialect='bigquery'):
    """Wrap a registered NRR query. report_sql comes from a verified export only."""
    import sqlglot
    if dialect not in ('duckdb', 'bigquery') or not requests:
        raise ValidationError('explicit supported diagnostic target and requests required')
    normalized = []
    for item in requests:
        if item.get('scenario', 'original') not in SCENARIOS:
            raise ValidationError('unknown diagnostic scenario')
        normalized.append(dict(case_id=item['case_id'], population=item.get('population', 'nrr'),
                               requested_date=date.fromisoformat(item['requested_date']),
                               scenario=item.get('scenario', 'original')))
    request_sql = values(normalized)
    # Author once in DuckDB SQL; only the validated underlying native report is
    # already in the target dialect. Neither reports nor candidates are stored.
    population_sql = '''SELECT q.case_id, r.month, r.lens,
        CASE WHEN q.scenario='unavailable' THEN 'undefined_zero_base' ELSE r.status END AS status,
        CASE WHEN q.scenario='unavailable' THEN NULL ELSE r.nrr END AS nrr
      FROM requests q CROSS JOIN original r
      WHERE q.scenario<>'empty' AND (q.scenario<>'missing_lens' OR r.lens<>'base_t12m')
      UNION ALL
      SELECT q.case_id, r.month, r.lens, r.status, r.nrr FROM requests q CROSS JOIN original r
      WHERE q.scenario='duplicate_lens' AND r.lens='base_t12m'
      UNION ALL
      SELECT q.case_id, CAST(r.month-INTERVAL '31 days' AS DATE), r.lens, r.status, r.nrr
      FROM requests q CROSS JOIN original r WHERE q.scenario='multiple_dates' '''
    candidate_sql = SQL.read_text(encoding='utf-8').strip().rstrip(';')
    def translate(sql):
        return sqlglot.transpile(sql, read='duckdb', write=dialect)[0]
    original = sqlglot.parse_one(report_sql, read=dialect)
    original_with = original.args.pop('with_', None)
    candidate = sqlglot.parse_one(translate(candidate_sql), read=dialect)
    candidate_with = candidate.args.pop('with_', None)
    wrapper = sqlglot.parse_one('WITH original AS (' + original.sql(dialect=dialect)
        + '), requests AS (' + translate(request_sql) + '), report_rows AS (' + translate(population_sql)
        + '), diagnostic AS (' + candidate.sql(dialect=dialect) + ') SELECT * FROM diagnostic ORDER BY case_id', read=dialect)
    target = wrapper.args['with_']
    combined = [*(original_with.expressions if original_with else []),
                *target.expressions[:-1],
                *(candidate_with.expressions if candidate_with else []), target.expressions[-1]]
    names = [cte.alias.lower() for cte in combined]
    if len(names) != len(set(names)):
        raise ValidationError('report CTE name collides with diagnostic scope')
    # BigQuery forbids every nested WITH inside a recursive statement, including
    # nonrecursive diagnostic CTEs. Keep both dependency graphs at outer scope.
    target.set('expressions', combined)
    target.set('recursive', bool(original_with and original_with.args.get('recursive')))
    return wrapper.sql(dialect=dialect)


def offline(rows, requests):
    """Replay from the retained report population; never a claim of fresh cloud execution."""
    schema = pa.schema([('month', pa.date32()), ('lens', pa.string()), ('status', pa.string()), ('nrr', pa.float64())])
    normalized = [{**{key: row[key] for key in ('lens', 'status', 'nrr')},
                   'month': date.fromisoformat(str(row['month']))} for row in rows]
    table = pa.Table.from_pylist(normalized, schema=schema)
    with duckdb.connect() as conn:
        conn.register('retained', table)
        return conn.execute(query('SELECT * FROM retained', requests, dialect='duckdb')).to_arrow_table()
