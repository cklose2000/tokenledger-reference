"""Row-grain comparison, including population and availability differences."""
import math
from decimal import Decimal
from tl.stream.events import ValidationError,canonical
from tl.receipts.metrics import normalized
import duckdb
import pyarrow as pa
import json


def clean(row):
    return {k:([normalized(v) for v in value] if isinstance(value,list) else normalized(value))
            for k,value in row.items()}


def compare_rows(left,right,keys,*,limit=100):
    """Currency/count exact; floating measures within absolute 1e-6.

    Provenance is retained beside differences, not used as a financial key.
    No cancellation through aggregation and no silent intersection of columns.
    """
    def index(rows):
        indexed={};duplicates=[]
        for row in rows:
            if any(key not in row for key in keys):
                raise ValidationError('registered output key missing from result')
            key=canonical({k:normalized(row[k]) for k in keys})
            if key in indexed:
                duplicates.append(key)
            indexed[key]=row
        return indexed,duplicates
    a,ad=index(left);b,bd=index(right)
    findings=[];different=0;slots=0
    for key in sorted(a.keys()|b.keys()):
        x,y=a.get(key),b.get(key)
        fields=[]
        if x is None or y is None:
            fields=['missing_spine' if x is None else 'missing_baseline']
        else:
            for name in (x.keys()|y.keys())-set(keys)-{'upstream_ids'}:
                slots+=1
                if name not in x or name not in y:
                    fields.append(name);continue
                u,v=x[name],y[name]
                if u is None or v is None:
                    equal=u is v
                elif isinstance(u,(int,float,Decimal)) and isinstance(v,(int,float,Decimal)):
                    if not math.isfinite(float(u)) or not math.isfinite(float(v)):
                        equal=False
                    elif name.endswith('_cents') or isinstance(u,int) and isinstance(v,int):
                        equal=u==v
                    else:
                        equal=abs(Decimal(str(u))-Decimal(str(v)))<=Decimal('0.000001')
                else:
                    equal=u==v
                if not equal:
                    fields.append(name)
        if fields:
            different+=1
            if len(findings)<limit:
                findings.append(dict(key=key,fields=sorted(fields),spine=None if x is None else clean(x),
                    baseline=None if y is None else clean(y),explanation_status='unexplained',
                    activity_ids=sorted(set((x or {}).get('upstream_ids',[])+(y or {}).get('upstream_ids',[])))))
    return dict(status='equivalent' if not(different or ad or bd) else 'different',
        spine_rows=len(left),baseline_rows=len(right),measure_slots=slots,different_rows=different,
        missing_spine=len(b.keys()-a.keys()),missing_baseline=len(a.keys()-b.keys()),
        duplicate_spine=len(ad),duplicate_baseline=len(bd),duplicate_keys=(ad+bd)[:limit],
        differences=findings,differences_truncated=different>limit)


def compare_tables(left,right,keys,*,limit=100):
    """Vectorized equivalent of the row matcher for the fixed large workload."""
    def q(name): return '"'+name.replace('"','""')+'"'
    if any(k not in left.column_names or k not in right.column_names for k in keys):
        raise ValidationError('registered output key missing from result')
    conn=duckdb.connect();conn.execute('SET threads=4');conn.execute("SET memory_limit='3GB'")
    conn.register('left_input',left);conn.register('right_input',right)
    try:
        duplicate=[]
        for side in ('left','right'):
            duplicate.append(conn.execute(f'SELECT coalesce(sum(n-1),0)::BIGINT FROM (SELECT count(*) n FROM {side}_input GROUP BY '+','.join(map(q,keys))+')').fetchone()[0])
        fields=[];names=[]
        for name in sorted((set(left.column_names)|set(right.column_names))-set(keys)-{'upstream_ids'}):
            names.append(name)
            if name not in left.column_names or name not in right.column_names:
                fields.append('true');continue
            a,b='a.'+q(name),'b.'+q(name)
            la,lb=left.schema.field(name).type,right.schema.field(name).type
            numeric=lambda t:pa.types.is_integer(t) or pa.types.is_floating(t) or pa.types.is_decimal(t)
            if numeric(la) and numeric(lb) and not name.endswith('_cents') and not(pa.types.is_integer(la) and pa.types.is_integer(lb)):
                fields.append(f'(({a} IS NULL)<>({b} IS NULL) OR ({a} IS NOT NULL AND {b} IS NOT NULL AND (NOT isfinite({a}) OR NOT isfinite({b}) OR abs({a}-{b})>0.000001)))')
            else: fields.append(f'({a} IS DISTINCT FROM {b})')
        join=' AND '.join(f'a.{q(k)} IS NOT DISTINCT FROM b.{q(k)}' for k in keys)
        mismatch=' OR '.join(fields) or 'false'
        conn.execute(f'''CREATE TEMP VIEW compared AS SELECT a,b,a._present AS lp,b._present AS rp,
           ({mismatch}) AS differs FROM (SELECT *,true _present FROM left_input) a
           FULL JOIN (SELECT *,true _present FROM right_input) b ON {join}''')
        count,missing_left,missing_right,matched=conn.execute('''SELECT
          count(*) FILTER(WHERE lp IS NULL OR rp IS NULL OR differs),
          count(*) FILTER(WHERE lp IS NULL),count(*) FILTER(WHERE rp IS NULL),
          count(*) FILTER(WHERE lp AND rp) FROM compared''').fetchone()
        findings=[]
        query='SELECT to_json(a),to_json(b) FROM compared WHERE lp IS NULL OR rp IS NULL OR differs'
        cursor=conn.execute(query if limit is None else query+' LIMIT ?',[] if limit is None else [limit])
        for a,b in cursor.fetchall():
            a=json.loads(a);b=json.loads(b)
            if not a.pop('_present'): a=None
            if not b.pop('_present'): b=None
            findings.append(dict(spine=a,baseline=b,explanation_status='unexplained',
                activity_ids=sorted(set((a or {}).get('upstream_ids',[])+(b or {}).get('upstream_ids',[])))))
        return dict(status='equivalent' if not(count or any(duplicate)) else 'different',
            spine_rows=len(left),baseline_rows=len(right),measure_slots=matched*len(names),different_rows=count,
            missing_spine=missing_left,missing_baseline=missing_right,duplicate_spine=duplicate[0],duplicate_baseline=duplicate[1],
            differences=findings,differences_truncated=limit is not None and count>limit)
    finally: conn.close()
