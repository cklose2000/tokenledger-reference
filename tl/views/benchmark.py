"""Fresh same-runtime baseline builds and receipted view-only comparisons.

The baseline keeps its independently authored SQL and ordinary dbt selection.
Native source projection avoids imposing an obsolete Arrow transfer on it.
Query and evidence costs are timed separately; no saved hash replaces replay.
"""
from pathlib import Path
from time import perf_counter
import hashlib
import json
import os
import re
import shutil
import subprocess
import uuid

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from tl.compare import baseline
from tl.compare.engine import artifacts, inventory
from tl.compare.matching import compare_tables
from tl.compare.outputs import registry
from tl.metrics.engine import runtime
from tl.receipts.metrics import code_revision, digest, receipt_id, append_receipts, read_receipts
from tl.stream.events import canonical, ValidationError, iso
from tl.views.engine import profile, replay as replay_views
from tl.views.hashing import input_manifest, table_fingerprint
from tl.views.session import ViewSession


def write_json(path,value):
    Path(path).write_text(canonical(value)+'\n',encoding='utf-8',newline='\n')


def baseline_runtime(executable=None):
    path=Path(executable) if executable else Path('.venv-baseline-views')/('Scripts/dbt.exe' if os.name=='nt' else 'bin/dbt')
    python=path.parent/('python.exe' if os.name=='nt' else 'python')
    if not path.is_file() or not python.is_file():
        raise ValidationError('install baseline/requirements-views-lock.txt in .venv-baseline-views or supply --dbt-executable')
    # Resolving a POSIX venv symlink selects the base Python and loses its packages.
    result=subprocess.run([str(python.absolute()),'-c',
        'import json,duckdb,platform;from importlib.metadata import version;print(json.dumps(dict(duckdb=duckdb.__version__,python=platform.python_version(),dbt_core=version("dbt-core"),dbt_duckdb=version("dbt-duckdb"))))'],capture_output=True,text=True,check=True)
    found=json.loads(result.stdout)
    if found['duckdb']!=duckdb.__version__ or found['python']!=runtime()['python']:
        raise ValidationError('comparison requires identical DuckDB and Python runtimes on both paths')
    return str(path.resolve()),found


def prepare_source(db,root,*,asof,known_at,watermark,mode):
    from tl.views.worker import run_worker
    result,process=run_worker('prepare_source',dict(db=db,root=root,asof=asof,
        known_at=known_at,watermark=watermark,mode=mode))
    result['projection_execution_seconds']=result['seconds']
    result['seconds']=process['wall_seconds']
    result['process_observation']=process
    return result


def _prepare_source(db,root,*,asof,known_at,watermark,mode):
    """Same frozen reader, native CTAS; no Python business calculation."""
    tick=perf_counter();root=Path(root);root.mkdir(parents=True,exist_ok=True)
    path=root/'baseline.duckdb'
    if mode=='full' and path.exists(): raise ValidationError('full baseline requires an empty destination')
    if mode!='full' and not path.exists(): raise ValidationError('increment needs an existing baseline state')
    with ViewSession(db,asof=asof,known_at=known_at,watermark=watermark) as source:
        first=source.first_month.isoformat();knowledge=iso(source.known_at);position=source.watermark
        # This tiny configuration relation is shared policy input, not revenue.
        allocation=source.query('SELECT * FROM @allocation')
    changed=[];counts={}
    with duckdb.connect(str(path)) as conn:
        from tl.stream import StreamReader
        conn.execute("SET memory_limit='4GB'");conn.execute('SET threads=4');conn.execute("SET TimeZone='UTC'")
        StreamReader(db).attach_snapshot(conn,asof=asof,known_at=knowledge,watermark=position)
        conn.execute('CREATE SCHEMA IF NOT EXISTS raw')
        for spec in sorted(Path('definitions/activities').glob('*.yaml')):
            kind=spec.stem
            if kind=='revenue_recognized': continue
            if mode=='full':
                conn.execute(f'CREATE TABLE raw.{kind} AS SELECT * FROM _visible WHERE activity=?',[kind])
            elif mode=='incremental':
                if conn.execute(f'SELECT count(*) FROM raw.{kind} r ANTI JOIN _visible n USING(activity_id)').fetchone()[0]:
                    raise ValidationError('incremental knowledge must be monotone')
                count=conn.execute(f'SELECT count(*) FROM _visible n ANTI JOIN raw.{kind} r USING(activity_id) WHERE activity=?',[kind]).fetchone()[0]
                if count:
                    # Raw source copies preserve the fourteen physical fields.
                    # No model reads the optional derived occurrence windows;
                    # computing them here would impose an unnecessary transform.
                    conn.execute(f'INSERT INTO raw.{kind} SELECT n.* FROM _visible n ANTI JOIN raw.{kind} r USING(activity_id) WHERE activity=?',[kind])
                    changed.append(kind);counts[kind]=count
        if mode=='full':
            conn.register('allocation_input',allocation)
            conn.execute('CREATE TABLE raw.allocation AS SELECT * FROM allocation_input')
    settings={'tokenledger_baseline':{'target':'local','outputs':{'local':{
        'type':'duckdb','path':str(path.resolve()),'schema':'main','threads':4,
        'settings':{'TimeZone':'UTC','memory_limit':'4GB','threads':4}}}}}
    (root/'profiles.yml').write_text(yaml.safe_dump(settings),encoding='utf-8')
    return dict(seconds=perf_counter()-tick,mode=mode,changed_sources=changed,inserted=counts,
                first_month=first,known_at=knowledge,watermark=position)


def graph_inventory(session,manifest,project):
    old=inventory(manifest,project)
    outputs={'output_'+name:session.output_sql(name) for name in registry(project)['outputs']}
    nodes={**session.graph.nodes,**outputs}
    def depth(name,trail=()):
        if name in trail: raise ValidationError('cyclic measured view graph')
        sql=nodes[name];deps=re.findall(r'@([a-z_]+)',sql)
        source_depth=2 if '_visible' in sql or '_snapshot' in sql else 0
        return 1+max([source_depth,*[depth(dep,(*trail,name)) for dep in deps]])
    files=[]
    for path in sorted([*Path('tl/views').rglob('*.py'),*Path('tl/views').rglob('*.sql'),*Path('tl/metrics/sql').rglob('*.sql')]):
        active=[line for line in path.read_text(encoding='utf-8').splitlines() if line.strip() and not line.lstrip().startswith(('#','--'))]
        files.append(dict(path=path.as_posix(),lines=len(active)))
    relations=session.conn.execute("SELECT database_name,schema_name,table_name,'table' FROM duckdb_tables() WHERE NOT internal UNION ALL SELECT database_name,schema_name,view_name,'view' FROM duckdb_views() WHERE NOT internal").fetchall()
    return dict(baseline=old['baseline'],baseline_compiled_sql_lines=old['compiled_sql_lines'],
        baseline_source_files=[v for v in old['files'] if v['path'].startswith(project+'/')],
        spine=dict(relations=relations,stored_derived_tables=0,query_nodes=len(nodes),registered_outputs=len(registry(project)['outputs']),
                   longest_logical_path=max(map(depth,nodes)),files=files,
                   graph={key:re.findall(r'@([a-z_]+)',sql) for key,sql in nodes.items()},
                   count_basis='catalog relations, named query nodes and source-code lines reported separately; CTEs are not deleted complexity'))


def compare(db,*,asof,known_at=None,watermark=None,version='v2',project='baseline',
            output_root,ledger,baseline_root=None,mode='full',order='baseline_first',executable=None,progress=None):
    if mode not in ('full','incremental','definition'): raise ValidationError('unsupported comparison mode')
    if order not in ('baseline_first','spine_first'): raise ValidationError('unsupported execution order')
    executable,bruntime=baseline_runtime(executable)
    pins=artifacts(project);revision=code_revision(pins);start=perf_counter()
    with ViewSession(db,asof=asof,known_at=known_at,watermark=watermark) as frozen:
        known_at=iso(frozen.known_at);watermark=frozen.watermark
    binding_seconds=perf_counter()-start
    root=Path(output_root)/uuid.uuid4().hex;root.mkdir(parents=True,exist_ok=False)
    baseline_root=Path(baseline_root) if baseline_root else root/'baseline'
    result={};observations=dict(common_binding_seconds=binding_seconds);contract=registry(project);built=None
    def spine():
        from tl.views.worker import run_worker
        if progress: progress(dict(side='spine',status='started'))
        result['spine'],observations['native_process']=run_worker('profile',dict(db=db,asof=asof,
            known_at=known_at,watermark=watermark,version=version,project=project,
            output_root=output_root,ledger=ledger),progress)
    def conventional():
        nonlocal built
        if progress: progress(dict(side='baseline',status='started'))
        projection=prepare_source(db,baseline_root,asof=asof,known_at=known_at,watermark=watermark,mode=mode)
        select=None
        if mode=='definition': select=['int_nrr_lenses+']
        if mode=='incremental':
            select=['source:raw.'+name+'+' for name in projection['changed_sources']]
            if not select: raise ValidationError('increment has no newly visible events')
        built=baseline.execute(baseline_root,project=project,asof=asof,first_month=projection['first_month'],
            nrr_floor_cents=10000000 if version=='v3' else 1000000,select=select,executable=executable)
        observations['baseline_projection']=projection
        observations['baseline_build_and_tests_seconds']=built['seconds']
    for call in ([conventional,spine] if order=='baseline_first' else [spine,conventional]): call()
    sroot=Path(result['spine']['run_directory']);smanifest=json.loads((sroot/'run.json').read_text())
    outputs={};results=[];records=[]
    with duckdb.connect(str(baseline_root/'baseline.duckdb'),read_only=True) as conn:
        conn.execute('SET threads=4');conn.execute("SET memory_limit='4GB'");conn.execute("SET TimeZone='UTC'")
        baseline_relations=conn.execute("SELECT schema_name,table_name,'table' FROM duckdb_tables() WHERE NOT internal UNION ALL SELECT schema_name,view_name,'view' FROM duckdb_views() WHERE NOT internal").fetchall()
        for name,spec in contract['outputs'].items():
            tick=perf_counter();right=conn.execute('SELECT * FROM main.'+name).to_arrow_table(65536);read_seconds=perf_counter()-tick
            tick=perf_counter();fp=table_fingerprint(conn,right,spec['key']);hash_seconds=perf_counter()-tick
            left=pq.read_table(sroot/(name+'.parquet')).drop(['receipt_id'])
            if set(left.column_names)!=set(spec['columns']) or set(right.column_names)!=set(spec['columns']):
                raise ValidationError('registered output columns differ: '+name)
            tick=perf_counter();matched=compare_tables(left,right,spec['key']);matching_seconds=perf_counter()-tick
            row_result=dict(output=name,**matched,baseline_population=fp,spine_receipt_id=smanifest['outputs'][name]['receipt_id'])
            record=dict(schema_version='tokenledger-views-comparison/v1',run_id=root.name,result=row_result,
                definition_version='comparison-views/v1;accounting/v1;nrr/'+version,inputs=smanifest['inputs'],
                query_hash=digest(dict(output=name,artifacts=pins)),git_sha=revision['git_sha'],execution_hash=digest(pins),
                asof=asof,known_at=smanifest['known_at'],watermark=smanifest['watermark'])
            record['receipt_id']=receipt_id(record);records.append(record);results.append({**row_result,'receipt_id':record['receipt_id']})
            tick=perf_counter();pq.write_table(right.append_column('receipt_id',pa.array([record['receipt_id']]*len(right),type=pa.string())),root/(name+'-baseline.parquet'));write_seconds=perf_counter()-tick
            outputs[name]=dict(receipt_id=record['receipt_id'],query_seconds=read_seconds,hash_seconds=hash_seconds,write_seconds=write_seconds,matching_seconds=matching_seconds)
    with ViewSession(db,asof=asof,known_at=smanifest['known_at'],watermark=smanifest['watermark']) as session:
        measured_inventory=graph_inventory(session,built['manifest'],project)
    measured_inventory['baseline']['relations']=baseline_relations
    (root/'dbt-artifacts').mkdir()
    for name in ('manifest.json','run_results.json'):
        shutil.copyfile(baseline_root/'target'/name,root/'dbt-artifacts'/name)
    shutil.copyfile(baseline_root/'dbt.log',root/'dbt-artifacts'/'build.log')
    observations.update(spine=smanifest['observations'],spine_outputs=smanifest['outputs'],baseline_outputs=outputs,total_seconds=perf_counter()-start)
    manifest=dict(schema_version='tokenledger-views-comparison/v1',run_id=root.name,project=project,definition_version=version,
        stream_path=str(Path(db).resolve()),inputs=smanifest['inputs'],asof=asof,known_at=smanifest['known_at'],watermark=smanifest['watermark'],
        **revision,execution_artifacts=pins,execution_hash=digest(pins),runtime=runtime(),baseline_runtime=bruntime,
        baseline_root=str(baseline_root.resolve()),spine_run_id=sroot.name,mode=mode,order=order,
        status='equivalent' if all(r['status']=='equivalent' for r in results) else 'different',results=results,
        settings=smanifest['settings'],inventory=measured_inventory,observations=observations,
        executed_baseline_nodes=[dict(node=r['unique_id'],status=r['status'],seconds=r['execution_time']) for r in built['run_results']['results']],
        bigquery=dict(status='not_run'))
    if artifacts(project)!=pins: raise ValidationError('execution artifacts changed during comparison')
    write_json(root/'run.json',manifest)
    observation=dict(schema_version='tokenledger-views-comparison-observation/v1',run_id=root.name,
        result=dict(manifest_sha256=hashlib.sha256((root/'run.json').read_bytes()).hexdigest(),observations=observations,inventory=measured_inventory),
        definition_version='comparison-views/v1',inputs=manifest['inputs'],query_hash=digest(pins),git_sha=revision['git_sha'],
        execution_hash=digest(pins),asof=asof,known_at=manifest['known_at'],watermark=manifest['watermark'])
    observation['receipt_id']=receipt_id(observation);records.append(observation)
    (root/'rows.jsonl').write_text(''.join(canonical(r)+'\n' for r in records),encoding='utf-8',newline='\n');append_receipts(ledger,records)
    return dict(status=manifest['status'],run_id=root.name,run_directory=str(root),results=results,
        observation_receipt_id=observation['receipt_id'],spine_run_id=sroot.name,bigquery=manifest['bigquery'])


def replay(ids,*,db=None,output_root,ledger,python=None):
    all_records=read_receipts(ledger);groups={};verified=[]
    for key in ids: groups.setdefault(all_records[key]['run_id'],[]).append(all_records[key])
    for run_id,selected in groups.items():
        root=Path(output_root)/run_id;manifest=json.loads((root/'run.json').read_text())
        observations=[r for r in all_records.values() if r.get('run_id')==run_id and r.get('schema_version')=='tokenledger-views-comparison-observation/v1']
        if len(observations)!=1 or hashlib.sha256((root/'run.json').read_bytes()).hexdigest()!=observations[0]['result']['manifest_sha256']:
            raise ValidationError('comparison observation differs')
        for row in selected:
            for key in ('run_id','inputs','git_sha','execution_hash','asof','known_at','watermark'):
                if row[key]!=manifest[key]: raise ValidationError('view comparison manifest differs: '+key)
            if row['schema_version']=='tokenledger-views-comparison-observation/v1':
                if row['result']['manifest_sha256']!=hashlib.sha256((root/'run.json').read_bytes()).hexdigest():
                    raise ValidationError('comparison observation differs')
                verified.append(dict(receipt_id=row['receipt_id'],verified=True,recomputed=False,result=row['result']))
        selected=[r for r in selected if r['schema_version']=='tokenledger-views-comparison/v1']
        if not selected: continue
        if digest(manifest['execution_artifacts'])!=manifest['execution_hash']: raise ValidationError('comparison execution hash differs')
        if python or artifacts(manifest['project'])!=manifest['execution_artifacts'] or runtime()!=manifest['runtime']:
            from tl.receipts.archive import historical_replay
            verified.extend(historical_replay([r['receipt_id'] for r in selected],manifest=manifest,db=db or manifest['stream_path'],
                output_root=output_root,ledger=ledger,python=python));continue
        replay_views([r['result']['spine_receipt_id'] for r in selected],db=db or manifest['stream_path'],output_root=output_root,ledger=ledger)
        with duckdb.connect() as conn:
            conn.execute('SET threads=4');conn.execute("SET memory_limit='4GB'")
            for row in selected:
                name=row['result']['output'];spec=registry(manifest['project'])['outputs'][name]
                if row['definition_version']!='comparison-views/v1;accounting/v1;nrr/'+manifest['definition_version'] or row['query_hash']!=digest(dict(output=name,artifacts=manifest['execution_artifacts'])):
                    raise ValidationError('comparison definition/query differs')
                right=pq.read_table(root/(name+'-baseline.parquet'))
                if len(right) and set(right['receipt_id'].unique().to_pylist())!={row['receipt_id']}:
                    raise ValidationError('baseline receipt substituted')
                right=right.drop(['receipt_id'])
                if table_fingerprint(conn,right,spec['key'])!=row['result']['baseline_population']: raise ValidationError('baseline population differs')
                left=pq.read_table(Path(output_root)/manifest['spine_run_id']/(name+'.parquet')).drop(['receipt_id'])
                result=dict(output=name,**compare_tables(left,right,spec['key']),baseline_population=row['result']['baseline_population'],spine_receipt_id=row['result']['spine_receipt_id'])
                if result!=row['result']: raise ValidationError('comparison reproduction differs')
                verified.append(dict(receipt_id=row['receipt_id'],verified=True,recomputed='spine_population; retained baseline verified',result=result))
    return verified
