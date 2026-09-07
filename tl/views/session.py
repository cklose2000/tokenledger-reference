"""Native view execution/v1. Business inputs never leave the source for staging."""
from datetime import date, timedelta
from calendar import monthrange
from decimal import Decimal
from pathlib import Path
from time import perf_counter
import json
import math
import re

import duckdb
import pyarrow.compute as pc
import yaml

from tl.compare.outputs import registry
from tl.metrics.definitions import Definition, FAMILIES
from tl.reporting.policy import RecognitionPolicy
from tl.stream import StreamReader, ValidationError
from tl.stream.events import timestamp,Catalog
from tl.views.compiler import Graph
from tl.views.accounting import register as accounting


def literal(value):
    if value is None: return 'NULL'
    if isinstance(value, bool): return 'true' if value else 'false'
    if isinstance(value, int): return str(value)
    if isinstance(value, date): return "DATE '"+value.isoformat()+"'"
    return "'"+str(value).replace("'","''")+"'"


def values(rows):
    names=list(rows[0])
    return 'SELECT * FROM (VALUES '+','.join('('+','.join(literal(row[n]) for n in names)+')' for row in rows)+') AS params('+','.join('"'+n.replace('"','""')+'"' for n in names)+')'


class ViewSession:
    execution_version='duckdb-views/v1'
    batch_groups={
        'allocation': (('net_rev_per_mtok','cost_per_mtok','model_vintage'),('token_errors',)),
        'journals': (('journal_lines','close_accounts','close_rollforward'),('journal_errors',)),
        'nrr': (('consumption_nrr','nrr_members'),()),
        'cohorts': (('customer_cohorts','cohort_members'),()),
    }

    def __init__(self,path,*,asof,known_at=None,watermark=None,threads=4,memory_limit='4GB'):
        tick=perf_counter()
        self.asof=date.fromisoformat(asof)
        if self.asof.day!=monthrange(self.asof.year,self.asof.month)[1]:
            raise ValidationError('reporting requires a calendar month-end')
        self.period_end=self.asof+timedelta(days=1)
        self.conn=duckdb.connect()
        self.conn.execute("SET TimeZone='UTC'")
        self.conn.execute('SET threads=?',[threads])
        self.conn.execute('SET memory_limit=?',[memory_limit])
        self.conn.execute('SET enable_progress_bar=false')
        self.graph=Graph()
        try:
            context=StreamReader(path).attach_snapshot(self.conn,asof=asof,known_at=known_at,watermark=watermark)
            self.watermark=context['watermark'];self.known_at=timestamp(context['known_at'])
            if self.conn.execute("SELECT count(*) FROM _visible WHERE _lane<>'sim'").fetchone()[0]:
                raise ValidationError('provider comparison accepts simulation-lane data only')
            if self.conn.execute('SELECT DISTINCT _schema_hash FROM _visible').fetchall()!=[(Catalog(Path('definitions/activities')).digest,)]:
                raise ValidationError('native reporting requires nonempty input under the retained activity registry')
            first=self.conn.execute('SELECT min(ts)::DATE FROM _visible').fetchone()[0]
            self.first_month=(first or self.asof).replace(day=1)
            g=self.graph
            g.add('context',values([dict(asof=self.asof,period_end=self.period_end,first_month=self.first_month)]))
            g.add('calendar',f'''SELECT month::DATE AS month,(month+INTERVAL '1 month')::DATE AS month_end,
                date_diff('day',month,month+INTERVAL '1 month')::BIGINT AS days
                FROM range({literal(self.first_month)},{literal(self.period_end)},INTERVAL '1 month') r(month)''')
            allocation=[]
            for path in sorted(Path('definitions/allocation').glob('*.yaml')):
                spec=yaml.safe_load(path.read_text(encoding='utf-8'))
                if spec['unit']!='MWh per million tokens' or spec['synthetic'] is not True or spec['version']!='v1' or spec['model']!=path.stem:
                    raise ValidationError('unsupported allocation definition')
                for kind,coef in spec['coefficients'].items():
                    nano=Decimal(str(coef))*10**9
                    if type(coef) not in (int,float) or not math.isfinite(coef) or nano<=0 or nano!=int(nano):
                        raise ValidationError('invalid allocation coefficient')
                    allocation.append(dict(model=spec['model'],token_type=kind,coefficient_nano=int(nano)))
            if not allocation: raise ValidationError('no allocation definitions')
            g.add('allocation',values(allocation))
            policy=RecognitionPolicy()
            g.add('recognition_policy',values([dict(source=s,recognition_kind=k,family=f) for (s,k),f in policy.pairs.items()]))
            accounting(g)
            g.typed('usage',('tokens_processed',),dict(usage_batch_id='VARCHAR',model='VARCHAR',token_type='VARCHAR',
                channel='VARCHAR',workload='VARCHAR',tokens='BIGINT',effective_price_per_mtok='DOUBLE'))
            for path in sorted(Path('tl/views/sql').glob('*.sql')):
                g.file(path.stem,path)
            g.add('revenue_amounts', '''SELECT r.*,p.family,c.ultimate_parent,c.segment
              FROM @recognized r JOIN @recognition_policy p USING(source,recognition_kind)
              JOIN @customer_identity c USING(customer)''')
            # Sparse revenue at its natural grain. The NRR query supplies the
            # complete parent universe separately, preserving zero members.
            g.add('parent_month', '''SELECT c.ultimate_parent,r.month,
              sum(CASE WHEN p.family='consumption' THEN r.revenue_cents ELSE 0 END)::BIGINT AS consumption_cents,
              sum(CASE WHEN p.family='subscription' THEN r.revenue_cents ELSE 0 END)::BIGINT AS subscription_cents,
              sum(r.revenue_cents)::BIGINT AS net_revenue_cents
              FROM @recognized r JOIN @recognition_policy p USING(source,recognition_kind)
              JOIN @customer_identity c USING(customer) GROUP BY 1,2''')
            from tl.views.validation import register
            register(g)
            self.setup_seconds=perf_counter()-tick
            self.validated=False
            self.validated_domains=set();self.validation_results={}
            self.last_query=None;self.last_dependencies=[]
        except BaseException:
            self.conn.close();raise

    def query(self,sql):
        compiled,deps=self.graph.compile(sql)
        self.last_query=compiled;self.last_dependencies=deps
        self.last_executed_query=compiled
        tick=perf_counter();cursor=self.conn.execute(compiled)
        self.last_execute_seconds=perf_counter()-tick
        tick=perf_counter();table=cursor.to_arrow_table(65536)
        self.last_arrow_seconds=perf_counter()-tick
        return table

    def validate_accounting(self):
        self.validate_dependencies([])
        self.validated=True

    def validate_dependencies(self,names,*,defer=()):
        from tl.views.validation import required
        for domain in sorted(required(names)-self.validated_domains-set(defer)):
            rows=self.query('SELECT * FROM @'+domain).to_pylist()
            failed=[r for r in rows if r['failures']!=0]
            if failed: raise ValidationError('reporting evidence failed: '+json.dumps(failed))
            self.validation_results[domain]=rows;self.validated_domains.add(domain)

    def output_sql(self,name,version='v2',project='baseline'):
        if version not in ('v2','v3'): raise ValidationError('NRR version must be v2 or v3')
        if name in ('journal_lines','fact_revenue'):
            return 'SELECT * FROM @'+('journals' if name=='journal_lines' else 'recognized')
        if name=='close_accounts':
            return 'SELECT month,account,sum(amount_cents)::BIGINT AS amount_cents FROM @journals GROUP BY 1,2'
        if name=='close_rollforward':
            return '''WITH amounts AS (SELECT month,account,sum(amount_cents)::BIGINT AS movement_cents FROM @journals GROUP BY 1,2),
              grid AS (SELECT c.month,a.account,coalesce(j.movement_cents,0)::BIGINT AS movement_cents
                FROM @calendar c CROSS JOIN (SELECT DISTINCT account FROM amounts) a LEFT JOIN amounts j USING(month,account))
              SELECT *,coalesce(sum(movement_cents) OVER(PARTITION BY account ORDER BY month
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0)::BIGINT AS beginning_cents,
                sum(movement_cents) OVER(PARTITION BY account ORDER BY month)::BIGINT AS ending_cents FROM grid'''
        family={'nrr_members':'consumption_nrr','cohort_members':'customer_cohorts'}.get(name,name)
        if family not in FAMILIES: raise ValidationError('unregistered comparison output')
        spec=registry(project)['outputs'][name]
        definition=Definition(family,version if family=='consumption_nrr' else None,spec.get('cuts'))
        for key,rows in definition.parameters.items():
            self.graph.nodes[key.removeprefix('_')]=values(rows)
        sql=definition.sql
        if family=='rev_per_mw':
            sql=sql.replace('report.revenue_ledger','@revenue_amounts')
        if family=='consumption_nrr':
            # Keep the released arithmetic unchanged; bind its parent_month
            # input to a sparse relation completed only at the parent boundary.
            marker='), windows AS ('
            sql='''WITH parent_month AS (
              SELECT u.ultimate_parent,p.month,coalesce(p.consumption_cents,0)::BIGINT AS consumption_cents,
                coalesce(p.subscription_cents,0)::BIGINT AS subscription_cents
              FROM (SELECT DISTINCT ultimate_parent FROM @customer_identity) u
              LEFT JOIN @parent_month p USING(ultimate_parent)
            '''+sql[sql.index(marker):]
            if name=='nrr_members':
                sql=sql[:sql.index(', totals AS (')]+' SELECT lens,ultimate_parent,base_cents,current_cents,in_cohort FROM eligible'
        if family=='customer_cohorts':
            sql=sql.replace('report.customer_month','@parent_month')
            if name=='cohort_members':
                sql=sql[:sql.index('\nSELECT q.quarter_end::DATE')]+''' SELECT r.quarter_end::DATE AS month,r.ultimate_parent,r.revenue_cents,t.threshold_cents
                    FROM revenue r CROSS JOIN _cohort_thresholds t WHERE r.revenue_cents>t.threshold_cents'''
        for node in ('customer_timeline','revenue_ledger','compute_ledger','seat_ledger','token_ledger','customer_month'):
            sql=sql.replace('report.'+node,'@'+node)
        sql=re.sub(r'\b_(context|calendar|nrr_lenses|cohort_thresholds)\b',lambda m:'@'+m[1],sql)
        return sql.replace('_snapshot','_visible')

    def output(self,name,version='v2',project='baseline'):
        family=('token_errors' if name in ('net_rev_per_mtok','cost_per_mtok','model_vintage') else
                'seat_errors' if name=='subscriptions' else
                'journal_errors' if name in ('journal_lines','close_accounts','close_rollforward','fact_revenue') else None)
        if family:
            table=self._checked_outputs([name],version,project,checks=[family])[name]
            # Receipts pin the logical output expression. Actual composite SQL
            # and its check results are retained separately in run evidence.
            self.last_query,self.last_dependencies=self.graph.compile(self.output_sql(name,version,project))
            return table
        self.validate_dependencies([name])
        return self.query(self.output_sql(name,version,project))

    def outputs(self,names,version='v2',project='baseline'):
        """A requested KPI pack shares operators within one query, not a cache.

        Column schemas are restored after UNION BY NAME. Individual receipts
        remain reproducible through the same independent output expressions.
        """
        # Only tested, related report families share an execution graph.
        # Mixing unrelated recursive graphs in a UNION can stall DuckDB's
        # pipeline scheduling, even on a small fixture. Never select it here.
        if names and len(names)==len(set(names)):
            for members,checks in self.batch_groups.values():
                if set(names)<=set(members):
                    return self._checked_outputs(names,version,project,checks=list(checks))
        raise ValidationError('batch execution requires distinct outputs from one registered family')

    def _checked_outputs(self,names,version,project,*,checks):
        self.validate_dependencies(names,defer=checks)
        checks=[name for name in checks if name not in self.validated_domains]
        queries={name:self.output_sql(name,version,project) for name in names}
        compiled={name:self.graph.compile(sql)[0] for name,sql in queries.items()}
        schemas={name:self.conn.execute('SELECT * FROM ('+sql+') q LIMIT 0').to_arrow_table().schema
                 for name,sql in compiled.items()}
        union=' UNION ALL BY NAME '.join('SELECT '+literal(name)+' AS _population,* FROM ('+sql+') q' for name,sql in queries.items())
        if checks:
            union+=' UNION ALL BY NAME '+' UNION ALL BY NAME '.join("SELECT '__check__' AS _population,"+literal(name)+' AS _check_domain,reason,failures FROM @'+name for name in checks)
        table=self.query(union)
        self.last_executed_query=self.last_query
        if checks:
            rows=table.filter(pc.equal(table['_population'],'__check__')).select(['_check_domain','reason','failures']).to_pylist()
            failed=[r for r in rows if r['failures']!=0]
            if failed: raise ValidationError('reporting evidence failed: '+json.dumps(failed))
            for domain in checks:
                self.validation_results[domain]=[{k:r[k] for k in ('reason','failures')} for r in rows if r['_check_domain']==domain]
                self.validated_domains.add(domain)
        self.batch_queries=compiled
        return {name:table.filter(pc.equal(table['_population'],name)).select(schemas[name].names).cast(schemas[name]) for name in names}

    def close(self): self.conn.close()
    def __enter__(self): return self
    def __exit__(self,*args): self.close()
