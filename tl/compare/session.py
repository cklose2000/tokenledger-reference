"""Spine comparison adapter. Existing reporting and historical replay stay unchanged."""
from pathlib import Path
from time import perf_counter
from tl.compare.accounting import recognize
from tl.scaffolds.session import ReportSession,SCAFFOLDS
from tl.stream import ValidationError


class ComparisonSession(ReportSession):
    def __init__(self,*args,**kwargs):
        start=perf_counter()
        super().__init__(*args,**kwargs)
        self.common_snapshot_seconds=perf_counter()-start
        if set(self.snapshot['_lane'].unique().to_pylist())-{'sim'}:
            self.close()
            raise ValidationError('architecture comparison accepts simulation-lane data only')
        self.recognized,self.journal=recognize(self.snapshot,period_end=self.period_end)
        self.conn.register('_derived_recognition',self.recognized)
        self.conn.register('_journal',self.journal)
        sql=self.sql['revenue_ledger']
        first=sql.index('), recognized AS (')
        last=sql.index('\nSELECT r.*,',first)
        sql=sql[:first]+'''), recognized AS (
 SELECT activity_id,_source,customer,month::TIMESTAMPTZ AS ts,month,source,
 recognition_kind,source_document,channel,revenue_cents FROM _derived_recognition
)'''+sql[last:]
        self.conn.execute('CREATE OR REPLACE VIEW report.revenue_ledger AS '+sql)
        self.sql['revenue_ledger']=sql
        seat_sql=self.sql['seat_ledger']
        seat_sql=seat_sql.replace('AS price,','AS price,\n         CAST(json_extract_string(feature_json,\'$.period_end\') AS DATE) AS service_end,')
        seat_sql=seat_sql.replace("lead(ts::DATE,1,(SELECT period_end FROM _context)) OVER w AS next_day,",
          "least(lead(ts::DATE,1,(SELECT period_end FROM _context)) OVER w,last_value(service_end IGNORE NULLS) OVER w) AS next_day,")
        self.sql['seat_ledger']=seat_sql
        self.conn.execute('CREATE OR REPLACE VIEW report.seat_ledger AS '+seat_sql)
        token_sql=self.sql['token_ledger']
        token_sql=token_sql.replace("SELECT _source,json_extract_string(feature_json,'$.usage_batch_id') AS usage_batch_id,",
            "SELECT activity_id,_source,json_extract_string(feature_json,'$.usage_batch_id') AS usage_batch_id,")
        token_sql=token_sql.replace('SELECT _source,source_document AS invoice_id,sum(revenue_cents)::BIGINT AS revenue_cents',
            'SELECT _source,source_document AS invoice_id,month,sum(revenue_cents)::BIGINT AS revenue_cents')
        token_sql=token_sql.replace("FROM report.revenue_ledger WHERE family='consumption' GROUP BY 1,2",
            "FROM report.revenue_ledger WHERE family='consumption' GROUP BY 1,2,3")
        token_sql=token_sql.replace('), cost AS (',"""), fees AS (
          SELECT activity_id,month,sum(amount_cents)::BIGINT AS month_fee_cents FROM _journal WHERE account='4095' GROUP BY 1,2
        ), cost AS (""")
        first=token_sql.index('coalesce(CAST(((2*i.fee_cents')
        last=token_sql.index(' AS invoice_fee_cents,',first)
        token_sql=token_sql[:first]+'coalesce(f.month_fee_cents,0)'+token_sql[last:]
        token_sql=token_sql.replace('e.invoice_id=i.invoice_id',
            'e.invoice_id=i.invoice_id AND e.month=t.month\n  LEFT JOIN fees f ON f.activity_id=i.activity_id AND f.month=t.month')
        token_sql=token_sql.replace('PARTITION BY t._source,t.usage_batch_id','PARTITION BY t._source,t.usage_batch_id,t.month')
        token_sql=token_sql.replace('PARTITION BY _source,usage_batch_id','PARTITION BY _source,usage_batch_id,month')
        self.sql['token_ledger']=token_sql
        self.conn.execute('CREATE OR REPLACE VIEW report.token_ledger AS '+token_sql)
        # Count these caches. Materialization is explicit and source-rebuildable.
        for name in SCAFFOLDS:
            self.conn.execute(f'CREATE TEMP TABLE _cache_{name} AS SELECT * FROM report.{name}')
            self.conn.execute(f'CREATE OR REPLACE VIEW report.{name} AS SELECT * FROM _cache_{name}')
        self.build_seconds=perf_counter()-start
        self.invariants=self.rows('''WITH balanced AS (
          SELECT _source,activity_id,month,sum(amount_cents) amount FROM _journal GROUP BY 1,2,3
        ), revenue AS (SELECT month,sum(revenue_cents) amount FROM _derived_recognition GROUP BY 1),
        journal AS (SELECT month,-sum(amount_cents) amount FROM _journal WHERE account LIKE '4%' GROUP BY 1)
        SELECT 'journal_balance' AS invariant,count(*)::BIGINT failures FROM balanced WHERE amount<>0
        UNION ALL SELECT 'revenue_journal_bridge',count(*)::BIGINT FROM revenue r FULL JOIN journal j USING(month)
        WHERE coalesce(r.amount,0)<>coalesce(j.amount,0)''')
        self.invariants+=self.rows('''SELECT 'token_population' AS invariant,
          abs((SELECT count(*) FROM _snapshot WHERE activity='tokens_processed')-
              (SELECT count(*) FROM report.token_ledger))::BIGINT AS failures
          UNION ALL SELECT 'customer_population',abs((SELECT count(*) FROM _snapshot WHERE activity='customer_created')-
              (SELECT count(*) FROM report.customer_timeline))::BIGINT
          UNION ALL SELECT 'revenue_scaffold_bridge',abs((SELECT coalesce(sum(revenue_cents),0) FROM _derived_recognition)-
              (SELECT coalesce(sum(revenue_cents),0) FROM report.revenue_ledger))::BIGINT''')
        self.invariants+=self.rows('''WITH r AS (
          SELECT month,sum(revenue_cents) amount FROM report.revenue_ledger WHERE family='consumption' GROUP BY 1),
          t AS (SELECT month,sum(net_revenue_cents) amount FROM report.token_ledger GROUP BY 1)
          SELECT 'monthly_token_revenue_bridge' AS invariant,count(*)::BIGINT failures
          FROM r FULL JOIN t USING(month) WHERE coalesce(r.amount,0)<>coalesce(t.amount,0)''')
        if any(row['failures'] for row in self.invariants):
            self.close();raise ValidationError('comparison spine accounting invariant failed')
        self.build_seconds=perf_counter()-start

    def journals(self):
        return self.rows('SELECT * FROM _journal')

    def rows(self,sql):
        self.last_query=sql
        return super().rows(sql)

    def close_accounts(self):
        return self.rows('SELECT month,account,sum(amount_cents)::BIGINT amount_cents FROM _journal GROUP BY 1,2')

    def close_rollforward(self):
        return self.rows('''WITH grid AS (
         SELECT c.month,a.account,coalesce(sum(j.amount_cents),0)::BIGINT movement_cents
         FROM _calendar c CROSS JOIN (SELECT DISTINCT account FROM _journal) a
         LEFT JOIN _journal j ON j.month=c.month AND j.account=a.account GROUP BY 1,2)
         SELECT *,coalesce(sum(movement_cents) OVER(PARTITION BY account ORDER BY month
           ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0)::BIGINT beginning_cents,
         sum(movement_cents) OVER(PARTITION BY account ORDER BY month)::BIGINT ending_cents FROM grid''')
