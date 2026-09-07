"""Repeat the accepted synthetic matrix using fresh native executions."""
from pathlib import Path
from time import perf_counter
import json
import platform
import random
import shutil
import uuid

from tl.compare.scenarios import inject
from tl.compare.suite import clone_baseline
from tl.generate.synthetic import Config
from tl.receipts.metrics import append_receipts,receipt_id
from tl.stream import StreamReader,ValidationError
from tl.views.benchmark import write_json
from tl.views.report import render
from tl.views.worker import run_worker


def suite(db,root,*,repetitions=3,project='baseline',report='docs/duckdb-execution-report.md',executable=None,progress=None):
    root=Path(root).resolve();work=root/'comparisons'/uuid.uuid4().hex;work.mkdir(parents=True)
    source=work/'world.duckdb';shutil.copyfile(db,source)
    with StreamReader(source).connect() as conn:
        population=conn.execute("SELECT count(*) FROM stream.activity WHERE activity='customer_created'").fetchone()[0]
        prefixes=[r[0] for r in conn.execute('SELECT DISTINCT _source FROM stream.activity').fetchall()]
    if prefixes!=['sim:'+Config(customers=population).run_id]:
        raise ValidationError('suite needs an unchanged seed-42, 30-month default generated world')
    late=inject(source,'late');bundle=work/'comparison.json';runs=[];rng=random.Random(42);orders={};order_counts={}
    spec=dict(schema_version='tokenledger-views-suite/v1',status='in_progress',source=str(source),ledger=str(root/'metrics.jsonl'),
        workload=dict(seed=42,months=30,customers=population,scale_gate=population==50000),repetitions=repetitions,
        execution_order_seed=42,os_cache='uncontrolled; one warmup per repeated workload; order randomized',
        machine=dict(platform=platform.platform(),processor=platform.processor()),late_fixture=late,runs=runs,bigquery=dict(status='not_run'))
    spec['baseline_storage']='Transient build databases released after dependent trials; manifests, SQL execution logs and complete output Parquet retained.'
    spec['side_order_policy']='Seeded starting side per workload, alternating thereafter so every repeated workload exercises both orders.'
    spec['process_policy']='Each comparison, native execution, baseline source projection, NRR request and replay uses a fresh process. Arrow/engine allocator state does not carry between timed sides or trials. Process wall time and peak RSS are retained; child dbt RSS is not included.'
    write_json(bundle,spec)
    def release(run):
        path=Path(json.loads((Path(run['run_directory'])/'run.json').read_text())['baseline_root'])/'baseline.duckdb'
        resolved=path.resolve()
        if not resolved.is_relative_to(root) or resolved.name!='baseline.duckdb':
            raise ValidationError('refusing to release a database outside this comparison root')
        # Only a closed database created by this suite is released. The source
        # stream, retained population files and other operator data stay intact.
        resolved.unlink()
    def run(label,trial,**kwargs):
        if label not in orders: orders[label]=rng.randrange(2);order_counts[label]=0
        order=['baseline_first','spine_first'][(orders[label]+order_counts[label])%2];order_counts[label]+=1
        if progress: progress(dict(workload=label,trial=trial,order=order,status='started'))
        result,process_observation=run_worker('compare',dict(db=source,project=project,output_root=root/'runs',ledger=root/'metrics.jsonl',order=order,executable=executable,**kwargs),progress)
        result['process_observation']=process_observation
        result.update(workload=label,trial=str(trial));runs.append(result);write_json(bundle,spec)
        if progress: progress(dict(workload=label,trial=trial,status=result['status'],run_id=result['run_id']))
        if result['status']!='equivalent': raise ValidationError('comparison differs; all evidence retained in '+str(bundle))
        return result
    release(run('full_july','warmup',asof='2026-07-31'))
    originals={}
    for asof in ('2026-05-31','2026-06-30'):
        originals[asof]=run('original_'+asof,1,asof=asof)
        if asof=='2026-05-31': release(originals[asof])
    for trial in range(1,repetitions+1):
        originals['2026-07-31']=run('full_july',trial,asof='2026-07-31')
        if trial<repetitions: release(originals['2026-07-31'])
    for trial in ['warmup',*range(1,repetitions+1)]:
        base=clone_baseline(originals['2026-06-30'],work/f'late-{trial}')
        late_run=run('late_june',trial,asof='2026-06-30',known_at='2026-07-15T00:00:00Z',baseline_root=base,mode='incremental')
        release(late_run)
    spec['day_fixture']=inject(source,'day')
    for trial in ['warmup',*range(1,repetitions+1)]:
        base=clone_baseline(originals['2026-07-31'],work/f'day-{trial}')
        after_day=run('one_day',trial,asof='2026-07-31',baseline_root=base,mode='incremental')
        if trial!=repetitions: release(after_day)
    nrr_runs=[]
    for trial in ['warmup',*range(1,repetitions+1)]:
        base=clone_baseline(after_day,work/f'floor-{trial}')
        floor_run=run('nrr_floor',trial,asof='2026-07-31',version='v3',baseline_root=base,mode='definition')
        release(floor_run)
        # Full thirteen-output parity above, and a separately clocked actual
        # user request here. Hashing and validation still execute on every run.
        nrr,process_observation=run_worker('profile',dict(db=source,asof='2026-07-31',version='v3',names=['consumption_nrr','nrr_members'],project=project,
            output_root=root/'runs',ledger=root/'metrics.jsonl'),progress)
        nrr['process_observation']=process_observation
        nrr.update(trial=str(trial));nrr_runs.append(nrr);spec['nrr_only']=nrr_runs;write_json(bundle,spec)
    start=perf_counter();verified,replay_process=run_worker('replay_compare',dict(ids=[r['receipt_id'] for r in after_day['results']],db=source,output_root=root/'runs',ledger=root/'metrics.jsonl'))
    replay_seconds=perf_counter()-start
    start=perf_counter();nrr_verified,single_process=run_worker('replay_profile',dict(ids=nrr_runs[-1]['receipt_ids'][:1],db=source,output_root=root/'runs',ledger=root/'metrics.jsonl'))
    single_seconds=perf_counter()-start
    for retained in (originals['2026-06-30'],originals['2026-07-31'],after_day): release(retained)
    spec.update(status='local_equivalent',replay=dict(attempted_populations=len(after_day['results']),verified_populations=len(verified),
        rows=sum(r['result']['spine_rows'] for r in verified),seconds=replay_seconds,process_observation=replay_process,
        method='fresh complete input fingerprint and spine query; retained baseline population verified',
        single_nrr=dict(verified=len(nrr_verified),seconds=single_seconds,receipt_id=nrr_verified[0]['receipt_id'],process_observation=single_process)),
        local_acceptance='pending_independent_qa')
    # The suite observation binds the matrix, replay measurements, and all run
    # references. Each run also has its own content-bound timing receipt.
    first=json.loads((Path(after_day['run_directory'])/'run.json').read_text())
    record=dict(schema_version='tokenledger-views-suite-observation/v1',run_id=work.name,result=spec.copy(),
        definition_version='views-suite/v1',inputs=first['inputs'],query_hash=first['execution_hash'],git_sha=first['git_sha'],
        execution_hash=first['execution_hash'],asof=first['asof'],known_at=first['known_at'],watermark=first['watermark'])
    record['receipt_id']=receipt_id(record);append_receipts(root/'metrics.jsonl',[record]);spec['observation_receipt_id']=record['receipt_id']
    write_json(bundle,spec)
    rendered=render(bundle,report)
    return dict(status=spec['status'],bundle=str(bundle),report=rendered,bigquery=spec['bigquery'])
