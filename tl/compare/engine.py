"""Comparison runs with content-bound output populations and replay receipts."""
from pathlib import Path
from time import perf_counter
from datetime import datetime,timezone
import hashlib
import json
import uuid
import shutil
import re
import os

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from tl.compare import baseline
from tl.compare.matching import compare_tables,clean
from tl.compare.outputs import registry,spine_rows,spine_table
from tl.compare.session import ComparisonSession
from tl.metrics.definitions import Definition,FAMILIES
from tl.metrics.engine import runtime
from tl.receipts.metrics import (execution_artifacts,code_revision,input_manifest,digest,
                                 receipt_id,append_receipts,read_receipts,ordered_batches)
from tl.stream.events import canonical,ValidationError,iso


def artifacts(project='baseline'):
    found=execution_artifacts([Definition(name) for name in FAMILIES])
    paths=[*Path(project).rglob('*.sql'),*Path(project).rglob('*.yaml'),*Path(project).rglob('*.yml'),
           Path(project)/'requirements-lock.txt',Path(project)/'requirements-views-lock.txt',Path('definitions/accounting/v1.yaml'),
           Path('definitions/metrics/consumption_nrr/v3.yaml')]
    for path in paths:
        if not any(part in ('target','logs','dbt_packages') for part in path.parts):
            found[path.as_posix()]=hashlib.sha256(path.read_bytes().replace(b'\r\n',b'\n')).hexdigest()
    return dict(sorted(found.items()))


def rows_digest(rows,keys):
    h=hashlib.sha256()
    if not isinstance(rows,pa.Table): rows=pa.Table.from_pylist(rows)
    for batch in ordered_batches(rows,[(key,'ascending') for key in keys]):
        for row in batch.to_pylist(): h.update((canonical(clean(row))+'\n').encode())
    return h.hexdigest()


def inventory(manifest,project='baseline',session=None):
    nodes={key:value for key,value in manifest['nodes'].items() if value['resource_type']=='model'}
    sources=manifest['sources']
    def depth(key,trail=()):
        if key in trail:
            raise ValidationError('cyclic lineage')
        if key in sources:
            return 1
        node=nodes.get(key)
        return 1+max((depth(parent,(*trail,key)) for parent in node['depends_on']['nodes']),default=0) if node else 0
    files=[]
    for path in [*Path(project).rglob('*.sql'),*Path('scaffolds').glob('*.sql'),
                 *Path('tl/metrics/sql').rglob('*.sql'),*Path('tl').rglob('*.py'),
                 *Path(project).rglob('*.yaml'),*Path(project).rglob('*.yml'),
                 *Path('definitions').rglob('*.yaml')]:
        if any(part in ('target','logs') for part in path.parts):
            continue
        lines=path.read_text(encoding='utf-8').splitlines()
        active=[line for line in lines if line.strip() and not line.lstrip().startswith(('--','#'))]
        files.append(dict(path=path.as_posix(),nonblank_non_line_comment_lines=len(active),noncomment_utf8_bytes=len('\n'.join(active).encode())))
    graph={'stream.activity':[], '_snapshot':['stream.activity'], '_derived_recognition':['_snapshot'], '_journal':['_snapshot']}
    if session:
        for name,sql in session.sql.items():
            dependencies=set(re.findall(r'\breport\.([a-z_]+)',sql))
            dependencies={'report.'+dep for dep in dependencies}
            dependencies.update(re.findall(r'\b(_snapshot|_derived_recognition|_allocation|_calendar|_context|_recognition_policy)\b',sql))
            graph['_cache_'+name]=sorted(dependencies);graph['report.'+name]=['_cache_'+name]
        for name in FAMILIES:
            graph['query.'+name]=sorted(set(re.findall(r'\breport\.[a-z_]+|\b_snapshot\b',Definition(name).sql)))
    def spine_depth(node,trail=()):
        if node in trail: raise ValidationError('cyclic spine lineage')
        return 1+max((spine_depth(parent,(*trail,node)) for parent in graph.get(node,[])),default=0)
    return dict(baseline=dict(models=len(nodes),sources=len(sources),
        materializations={kind:sum(n['config']['materialized']==kind for n in nodes.values()) for kind in ('table','view','incremental','ephemeral')},
        longest_path=max(map(depth,nodes),default=0)+2,
        lineage_basis='dbt manifest path includes its raw source; add common stream and frozen snapshot steps',
        nodes=[dict(name=n['name'],path=n['original_file_path'],materialization=n['config']['materialized'],
                    dependencies=n['depends_on']['nodes']) for n in nodes.values()]),
        spine=dict(canonical_tables=1,scaffold_views=6,scaffold_caches=6,recognition_materializations=1,
                   journal_materializations=1,registered_queries=len(registry(project)['outputs']),
                   lineage_status='measured_from_executed_scaffold_dependencies',graph=graph,
                   longest_path=max(map(spine_depth,graph),default=0),
                   relations=[] if not session else session.rows("SELECT schema_name,table_name AS name,'table' AS kind FROM duckdb_tables() WHERE NOT internal UNION ALL SELECT schema_name,view_name AS name,'view' AS kind FROM duckdb_views() WHERE NOT internal")),files=files,
        compiled_sql_lines=sum(sum(bool(line.strip()) and not line.lstrip().startswith('--') for line in n.get('compiled_code','').splitlines()) for n in nodes.values()))


def run_compare(db,*,asof,known_at=None,watermark=None,project='baseline',version='v2',
                output_root=Path('metrics/out'),ledger=Path('ledger/metrics.jsonl'),baseline_root=None,
                baseline_select=None,mode='full'):
    if version not in ('v2','v3'):
        raise ValidationError('comparison NRR definition must be v2 or v3')
    contract=registry(project)
    pins=artifacts(project);revision=code_revision(pins)
    run_id=uuid.uuid4().hex;root=Path(output_root)/run_id;root.mkdir(parents=True,exist_ok=False)
    started=datetime.now(timezone.utc)
    outputs={};results=[];records=[]
    with ComparisonSession(db,asof=asof,known_at=known_at,watermark=watermark) as session:
        inputs=input_manifest(session.snapshot,root/'inputs.jsonl')
        pq.write_table(session.snapshot,root/'snapshot.parquet')
        allocation=session.conn.execute('SELECT * FROM _allocation').fetch_arrow_table()
        baseline_root=Path(baseline_root) if baseline_root else root/'baseline'
        if mode=='full':
            projection=dict(seconds=baseline.prepare(session.snapshot,baseline_root,allocation),mode='full_projection')
        elif mode=='definition':
            projection=dict(seconds=0,mode='no_source_change')
            baseline_select=['int_nrr_lenses+']
        else:
            projection=baseline.append_projection(session.snapshot,baseline_root)
            baseline_select=['source:raw.'+name+'+' for name in projection['changed_sources']]
            if not baseline_select: raise ValidationError('incremental comparison has no newly visible source events')
        built=baseline.execute(baseline_root,project=project,asof=asof,first_month=session.first_month.isoformat(),
                               nrr_floor_cents=10000000 if version=='v3' else 1000000,select=baseline_select)
        (root/'dbt-artifacts').mkdir()
        for filename in ('manifest.json','run_results.json'):
            shutil.copyfile(baseline_root/'target'/filename,root/'dbt-artifacts'/filename)
        shutil.copyfile(baseline_root/'dbt.log',root/'dbt-artifacts/build.log')
        manifest=dict(schema_version='tokenledger-comparison/v1',run_id=run_id,synthetic=True,
            target='duckdb',asof=asof,known_at=iso(session.known_at),watermark=session.watermark,
            stream_path=str(Path(db).resolve()),project=project,definition_version=version,inputs=inputs,
            **revision,execution_artifacts=pins,execution_hash=digest(pins),runtime=runtime(),started_at=iso(started),
            bigquery=dict(status='not_run',reason='No configured scratch project or native jobs.'),
            baseline_root=str(baseline_root.resolve()),mode=mode,
            observations=dict(common_snapshot_seconds=session.common_snapshot_seconds,
                              spine_build_seconds=session.build_seconds-session.common_snapshot_seconds,baseline_projection=projection,
                              baseline_build_and_tests_seconds=built['seconds']),
            spine_invariants=session.invariants,
            inventory=inventory(built['manifest'],project,session))
        for name,spec in contract['outputs'].items():
            tick=perf_counter();left=spine_table(session,name,spec,version);spine_seconds=perf_counter()-tick
            tick=perf_counter();right=baseline.table(baseline_root,name);baseline_seconds=perf_counter()-tick
            comparison=compare_tables(left,right,spec['key'])
            expected=set(spec['columns'])
            if set(left.column_names)!=expected or set(right.column_names)!=expected:
                raise ValidationError('output columns differ from frozen registry: '+name)
            queries=dict(spine=spec,baseline_model=name,artifacts=pins)
            result=dict(output=name,**comparison,spine_sha256=rows_digest(left,spec['key']),baseline_sha256=rows_digest(right,spec['key']))
            record=dict(schema_version='tokenledger-comparison/v1',run_id=run_id,result=result,
                definition_version='comparison/v1;accounting/v1;nrr/'+version,inputs=inputs,query_hash=digest(queries),
                git_sha=revision['git_sha'],execution_hash=digest(pins),asof=asof,known_at=manifest['known_at'],watermark=session.watermark)
            record['receipt_id']=receipt_id(record)
            for side,rows in [('spine',left),('baseline',right)]:
                # Every emitted result row links to its whole-query population receipt.
                table=rows.append_column('receipt_id',pa.array([record['receipt_id']]*len(rows),type=pa.string()))
                pq.write_table(table,root/f'{name}-{side}.parquet')
            outputs[name]=dict(receipt_id=record['receipt_id'],spine_seconds=spine_seconds,baseline_seconds=baseline_seconds)
            results.append({**result,'receipt_id':record['receipt_id']});records.append(record)
        manifest['outputs']=outputs
        manifest['inventory']=inventory(built['manifest'],project,session)
        manifest['executed_baseline_nodes']=[dict(node=row['unique_id'],status=row['status'],seconds=row['execution_time']) for row in built['run_results']['results']]
        manifest['status']='equivalent' if all(r['status']=='equivalent' for r in results) else 'different'
        manifest['finished_at']=iso(datetime.now(timezone.utc))
    if artifacts(project)!=pins:
        raise ValidationError('comparison execution artifacts changed during run')
    raw=canonical(manifest)+'\n'
    (root/'run.json').write_text(raw,encoding='utf-8',newline='\n')
    measurement=dict(schema_version='tokenledger-comparison-observation/v1',run_id=run_id,
        result=dict(output='observations',manifest_sha256=hashlib.sha256(raw.encode()).hexdigest(),
                    observations=manifest['observations'],inventory=manifest['inventory']),
        definition_version='comparison/v1',inputs=inputs,query_hash=digest(dict(method='perf_counter;dbt-manifest;registered-populations',artifacts=pins)),
        git_sha=revision['git_sha'],execution_hash=digest(pins),asof=asof,known_at=manifest['known_at'],watermark=manifest['watermark'])
    measurement['receipt_id']=receipt_id(measurement);records.append(measurement)
    (root/'rows.jsonl').write_text(''.join(canonical(row)+'\n' for row in results),encoding='utf-8',newline='\n')
    append_receipts(ledger,records)
    return dict(status=manifest['status'],run_id=run_id,run_directory=str(root),results=results,
                observation_receipt_id=measurement['receipt_id'],bigquery=manifest['bigquery'])


def replay(ids,*,db=None,output_root=Path('metrics/out'),ledger=Path('ledger/metrics.jsonl'),python=None):
    records=read_receipts(ledger);groups={}
    for key in ids:
        record=records[key];groups.setdefault(record['run_id'],[]).append(record)
    verified=[]
    for run_id,selected in groups.items():
        root=Path(output_root)/run_id;manifest=json.loads((root/'run.json').read_text(encoding='utf-8'))
        for record in selected:
            if record['schema_version']=='tokenledger-comparison-observation/v1':
                if hashlib.sha256((root/'run.json').read_bytes()).hexdigest()!=record['result']['manifest_sha256']:
                    raise ValidationError('comparison measurement manifest mismatch')
                verified.append(dict(receipt_id=record['receipt_id'],verified=True,recomputed=False,
                                     method='retained timing observations and inventory hash verification',result=record['result']))
        selected=[r for r in selected if r['schema_version']=='tokenledger-comparison/v1']
        if not selected: continue
        for record in selected:
            for key in ('run_id','git_sha','asof','known_at','watermark','inputs','execution_hash'):
                if record[key]!=manifest[key]: raise ValidationError('comparison manifest differs from receipt: '+key)
            if record['definition_version']!='comparison/v1;accounting/v1;nrr/'+manifest['definition_version']:
                raise ValidationError('comparison definition differs from receipt')
        if digest(manifest['execution_artifacts'])!=manifest['execution_hash']:
            raise ValidationError('comparison execution artifact hash mismatch')
        pins=artifacts(manifest['project'])
        if python or pins!=manifest['execution_artifacts'] or runtime()!=manifest['runtime']:
            if manifest['execution_hash'] in json.loads(os.environ.get('TL_REPLAY_STACK','[]')):
                raise ValidationError('comparison historical runtime or artifacts differ')
            from tl.receipts.archive import historical_replay
            verified.extend(historical_replay([r['receipt_id'] for r in selected],manifest=manifest,
                db=db or manifest['stream_path'],output_root=output_root,ledger=ledger,python=python))
            continue
        with ComparisonSession(db or manifest['stream_path'],asof=manifest['asof'],known_at=manifest['known_at'],watermark=manifest['watermark']) as session:
            if input_manifest(session.snapshot)!=manifest['inputs']:
                raise ValidationError('comparison source snapshot mismatch')
            for record in selected:
                name=record['result']['output'];spec=registry(manifest['project'])['outputs'][name]
                expected=dict(spine=spec,baseline_model=name,artifacts=pins)
                if (record['query_hash']!=digest(expected) or record['execution_hash']!=digest(pins)
                        or record['inputs']!=manifest['inputs'] or record['asof']!=manifest['asof']
                        or record['known_at']!=manifest['known_at'] or record['watermark']!=manifest['watermark']):
                    raise ValidationError('comparison receipt contract mismatch')
                rows=spine_table(session,name,spec,manifest['definition_version'])
                if rows_digest(rows,spec['key'])!=record['result']['spine_sha256']:
                    raise ValidationError('comparison spine output reproduction failed')
                for side in ('spine','baseline'):
                    path=root/f'{name}-{side}.parquet'
                    retained=pa.table({}) if not path.exists() else pq.read_table(path)
                    if len(retained):
                        if set(retained['receipt_id'].unique().to_pylist())!={record['receipt_id']}:
                            raise ValidationError('output row receipt mismatch')
                        retained=retained.drop(['receipt_id'])
                    if rows_digest(retained,spec['key'])!=record['result'][side+'_sha256']:
                        raise ValidationError('retained comparison output hash mismatch')
                verified.append(dict(receipt_id=record['receipt_id'],verified=True,recomputed='spine_population',
                                     baseline='retained_population_verified',result=record['result']))
    return verified


def record_observation(value,*,inputs,project,output_root,ledger):
    """Bind suite re-performance and migration observations using the shared ledger."""
    run_id=uuid.uuid4().hex;root=Path(output_root)/run_id;root.mkdir(parents=True,exist_ok=False)
    pins=artifacts(project);revision=code_revision(pins)
    manifest=dict(schema_version='tokenledger-comparison/v1',run_id=run_id,project=project,
                  runtime=runtime(),execution_artifacts=pins,execution_hash=digest(pins),observations=value,**revision)
    raw=(canonical(manifest)+'\n').encode();(root/'run.json').write_bytes(raw)
    record=dict(schema_version='tokenledger-comparison-observation/v1',run_id=run_id,
        result=dict(output='suite_observations',manifest_sha256=hashlib.sha256(raw).hexdigest(),observations=value),
        definition_version='comparison-suite/v1',inputs=inputs,query_hash=digest(dict(method='suite-observations',artifacts=pins)),
        git_sha=revision['git_sha'],execution_hash=digest(pins),asof='2026-07-31',known_at='2026-08-01T00:00:00Z',
        watermark=inputs['input_row_range'][1])
    record['receipt_id']=receipt_id(record);append_receipts(ledger,[record])
    return dict(receipt_id=record['receipt_id'],run_id=run_id,run_directory=str(root))
