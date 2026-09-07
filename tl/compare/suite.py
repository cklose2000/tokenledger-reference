"""Fixed experiment matrix. Repetitions and failures remain visible."""
from pathlib import Path
import json
import shutil
import uuid
import yaml
import hashlib

from tl.compare.engine import run_compare,replay,record_observation
from tl.compare.scenarios import inject
from tl.compare.report import render
from tl.compare.migration import demonstrate
from tl.compare.bridge import bridge
from tl.receipts.metrics import digest
from tl.stream import StreamReader,ValidationError
from tl.stream.events import canonical
from tl.generate.synthetic import Config


def clone_baseline(run,destination):
    source=Path(json.loads((Path(run['run_directory'])/'run.json').read_text())['baseline_root'])
    destination=Path(destination);destination.mkdir(parents=True,exist_ok=False)
    shutil.copyfile(source/'baseline.duckdb',destination/'baseline.duckdb')
    profile=yaml.safe_load((source/'profiles.yml').read_text())
    profile['tokenledger_baseline']['outputs']['local']['path']=str((destination/'baseline.duckdb').resolve())
    (destination/'profiles.yml').write_text(yaml.safe_dump(profile),encoding='utf-8')
    return destination


def suite(db,root,*,repetitions=3,project='baseline',report='docs/collapse-report.md',progress=None):
    if repetitions<1: raise ValidationError('at least one measured repetition is required')
    root=Path(root).resolve();work=root/'comparisons'/uuid.uuid4().hex;work.mkdir(parents=True)
    source=work/'world.duckdb';shutil.copyfile(db,source)
    with StreamReader(source).connect() as conn:
        population=conn.execute("SELECT count(*) FROM stream.activity WHERE activity='customer_created'").fetchone()[0]
        prefixes=[r[0] for r in conn.execute('SELECT DISTINCT _source FROM stream.activity').fetchall()]
    expected='sim:'+Config(customers=population).run_id
    if prefixes!=[expected]:
        raise ValidationError('suite needs the unchanged seed-42, 30-month default generated world before fixed increments')
    late=inject(source,'late')
    runs=[];bundle=work/'comparison.json'
    def save():
        value=dict(schema_version='tokenledger-comparison-suite/v1',source=str(source),
                   workload=dict(seed=42,months=30,customers=population,scale_gate=population==50000),
                   repetitions=repetitions,ledger=str(root/'metrics.jsonl'),runs=runs,late_fixture=late,
                   status='in_progress',bigquery={'status':'not_run'})
        bundle.write_text(canonical(value)+'\n',encoding='utf-8',newline='\n')
    def run(workload,trial,**kwargs):
        if progress: progress(dict(workload=workload,trial=trial,status='started'))
        result=run_compare(source,project=project,output_root=root/'runs',ledger=root/'metrics.jsonl',**kwargs)
        result.update(workload=workload,trial=str(trial));runs.append(result);save()
        if progress: progress(dict(workload=workload,trial=trial,status=result['status'],run_id=result['run_id']))
        if result['status']!='equivalent':
            render(bundle,report)
            raise ValidationError(f'comparison differences retained in {bundle}')
        return result
    # One full-build warmup; the three registered original closes remain distinct.
    run('full_july','warmup',asof='2026-07-31')
    originals={}
    for asof in ('2026-05-31','2026-06-30'):
        originals[asof]=run('original_'+asof,1,asof=asof)
    for trial in range(1,repetitions+1):
        originals['2026-07-31']=run('full_july',trial,asof='2026-07-31')
    # Rebuild from the identical original June state for every timing attempt.
    late_run=None
    for trial in ['warmup',*range(1,repetitions+1)]:
        base=clone_baseline(originals['2026-06-30'],work/f'late-{trial}')
        late_run=run('late_june',trial,asof='2026-06-30',known_at='2026-07-15T00:00:00Z',baseline_root=base,mode='incremental')
    day=inject(source,'day');after_day=None
    floor_run=None
    for trial in ['warmup',*range(1,repetitions+1)]:
        base=clone_baseline(originals['2026-07-31'],work/f'day-{trial}')
        after_day=run('one_day',trial,asof='2026-07-31',baseline_root=base,mode='incremental')
    for trial in ['warmup',*range(1,repetitions+1)]:
        base=clone_baseline(after_day,work/f'floor-{trial}')
        floor_run=run('nrr_floor',trial,asof='2026-07-31',version='v3',baseline_root=base,mode='definition')
    # Exhaustive replay of every registered KPI population in the final v2 run.
    ids=[row['receipt_id'] for row in after_day['results']]
    verified=replay(ids,db=source,output_root=root/'runs',ledger=root/'metrics.jsonl')
    bridges=dict(late=bridge(originals['2026-06-30'],late_run),day=bridge(originals['2026-07-31'],after_day),floor=bridge(after_day,floor_run))
    (work/'bridges.json').write_text(canonical(bridges)+'\n',encoding='utf-8',newline='\n')
    migrations=[demonstrate(run,work/(label+'-migration.duckdb')) for label,run in [('original',originals['2026-06-30']),('restated',late_run)]]
    value=json.loads(bundle.read_text());value.update(status='local_equivalent',day_fixture=day,
        replay=dict(attempted_populations=len(ids),verified_populations=len(verified),
                    rows=sum(row['result']['spine_rows'] for row in verified),method='exhaustive spine populations; retained baseline bytes'),
        bridges=dict(path=str(work/'bridges.json'),sha256=hashlib.sha256((work/'bridges.json').read_bytes()).hexdigest()),migrations=migrations,
        local_acceptance='pending_regression_review',bigquery={'status':'not_run'})
    input_scope=json.loads((Path(after_day['run_directory'])/'run.json').read_text())['inputs']
    observed={key:value[key] for key in ('workload','repetitions','replay','bridges','migrations','day_fixture','late_fixture','bigquery')}
    value['observation']=record_observation(observed,inputs=input_scope,project=project,output_root=root/'runs',ledger=root/'metrics.jsonl')
    bundle.write_text(canonical(value)+'\n',encoding='utf-8',newline='\n')
    result=render(bundle,report)
    return dict(status=value['status'],bundle=str(bundle),report=result,personal_next='G0 -> U1 -> L1',bigquery=value['bigquery'])
