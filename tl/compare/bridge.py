"""Exact output change populations and conservative new-evidence dependencies."""
from pathlib import Path
import json
import pyarrow.parquet as pq
import duckdb
from tl.compare.matching import compare_tables
from tl.compare.outputs import registry
from tl.stream.events import ValidationError


def bridge(before,after):
    a,b=Path(before['run_directory']),Path(after['run_directory'])
    ma,mb=[json.loads((p/'run.json').read_text()) for p in (a,b)]
    if ma['asof']!=mb['asof']:
        raise ValidationError('comparison bridge requires the same economic reporting period')
    conn=duckdb.connect()
    try:
        conn.register('old_snapshot',pq.read_table(a/'snapshot.parquet',columns=['activity_id']))
        conn.register('new_snapshot',pq.read_table(b/'snapshot.parquet',columns=['activity_id','activity']))
        candidates=[r[0] for r in conn.execute("SELECT n.activity_id FROM new_snapshot n ANTI JOIN old_snapshot o USING(activity_id) WHERE n.activity<>'revenue_recognized' ORDER BY 1").fetchall()]
    finally: conn.close()
    outputs={}
    for name,spec in registry()['outputs'].items():
        left=pq.read_table(a/f'{name}-spine.parquet').drop(['receipt_id'])
        right=pq.read_table(b/f'{name}-spine.parquet').drop(['receipt_id'])
        outputs[name]=compare_tables(left,right,spec['key'],limit=None)
        outputs[name].update(before_receipt=next(r['receipt_id'] for r in before['results'] if r['output']==name),
                             after_receipt=next(r['receipt_id'] for r in after['results'] if r['output']==name))
    return dict(asof=ma['asof'],before_run=before['run_id'],after_run=after['run_id'],outputs=outputs,
                newly_visible_upstream_activity_ids=candidates,
                attribution='Conservative new-evidence candidates. Output row upstream_ids are dependency lineage, not a minimal causal proof.')
