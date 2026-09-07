"""Spine output projections governed by the frozen comparison registry."""
from pathlib import Path
import yaml
import pyarrow as pa
from tl.metrics.definitions import Definition,FAMILIES


def registry(project='baseline'):
    return yaml.safe_load((Path(project)/'outputs.yaml').read_text(encoding='utf-8'))


def spine_rows(session,name,spec,version='v2'):
    if name in FAMILIES:
        return Definition(name,version if name=='consumption_nrr' else None,spec.get('cuts')).execute(session)
    if name=='journal_lines':
        return session.journals()
    if name in ('close_accounts','close_rollforward'):
        return getattr(session,name)()
    if name=='fact_revenue':
        return session.recognized.to_pylist()
    if name=='nrr_members':
        definition=Definition('consumption_nrr',version)
        definition.execute(session)
        # Same released spine definition, projected before aggregation.
        sql=definition.sql[:definition.sql.index(', totals AS (')]
        return session.rows(sql+''' SELECT lens,ultimate_parent,base_cents,current_cents,in_cohort FROM eligible''')
    if name=='cohort_members':
        definition=Definition('customer_cohorts')
        definition.execute(session)
        sql=definition.sql[:definition.sql.index('\nSELECT q.quarter_end::DATE')]
        return session.rows(sql+''' SELECT r.quarter_end::DATE AS month,r.ultimate_parent,r.revenue_cents,t.threshold_cents
           FROM revenue r CROSS JOIN _cohort_thresholds t WHERE r.revenue_cents>t.threshold_cents''')
    raise ValueError('unmapped spine output: '+name)


def spine_table(session,name,spec,version='v2'):
    if name=='journal_lines': return session.journal
    if name=='fact_revenue': return session.recognized
    rows=spine_rows(session,name,spec,version)
    return pa.Table.from_pylist(rows) if rows else session.conn.execute(session.last_query).fetch_arrow_table()
