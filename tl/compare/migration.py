"""A local shadow, read-endpoint switch and rollback; no production cutover."""
from pathlib import Path
import json
import duckdb
import pyarrow.parquet as pq
from tl.compare.engine import rows_digest
from tl.compare.matching import compare_tables
from tl.compare.outputs import registry
from tl.stream import ValidationError


def demonstrate(run,destination):
    root=Path(run['run_directory']);name='net_rev_per_mtok';keys=registry()['outputs'][name]['key']
    original=pq.read_table(root/f'{name}-baseline.parquet').drop(['receipt_id'])
    shadow=pq.read_table(root/f'{name}-spine.parquet').drop(['receipt_id'])
    if compare_tables(original,shadow,keys)['status']!='equivalent':
        raise ValidationError('shadow output is not equivalent; no endpoint switch')
    destination=Path(destination)
    if destination.exists(): raise ValidationError('migration rehearsal requires a new isolated database')
    with duckdb.connect(str(destination)) as conn:
        conn.register('original_input',original);conn.register('shadow_input',shadow)
        conn.execute('CREATE TABLE old_mart AS SELECT * FROM original_input')
        conn.execute('CREATE TABLE spine_mart AS SELECT * FROM shadow_input')
        conn.execute('CREATE VIEW published AS SELECT * FROM old_mart')
        before=rows_digest(conn.execute('SELECT * FROM published').fetch_arrow_table(),keys)
        conn.execute('CREATE OR REPLACE VIEW published AS SELECT * FROM spine_mart')
        switched=rows_digest(conn.execute('SELECT * FROM published').fetch_arrow_table(),keys)
        conn.execute('CREATE OR REPLACE VIEW published AS SELECT * FROM old_mart')
        restored=rows_digest(conn.execute('SELECT * FROM published').fetch_arrow_table(),keys)
    if before!=restored or switched!=rows_digest(shadow,keys):
        raise ValidationError('read endpoint switch or rollback failed')
    return dict(status='demonstrated_locally',mart=name,rows=len(original),before_sha256=before,
        switched_sha256=switched,rollback_sha256=restored,comparison_receipt=next(r['receipt_id'] for r in run['results'] if r['output']==name),
        source_run=run['run_id'],owner_role='Finance Systems director (proposed)',
        approval='engineering rehearsal only; no production authority asserted',retirement='not_performed; consumer ownership must be established')
