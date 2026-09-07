"""CLI-first profile evidence with full population receipts and fresh replay."""
from datetime import datetime,timezone
from pathlib import Path
from time import perf_counter
import hashlib
import json
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

from tl.compare.engine import artifacts
from tl.compare.outputs import registry
from tl.compare.matching import compare_tables
from tl.metrics.engine import runtime
from tl.receipts.metrics import code_revision,digest,receipt_id,append_receipts,read_receipts
from tl.stream.events import canonical,iso,ValidationError
from tl.views.session import ViewSession
from tl.views.hashing import input_manifest,table_fingerprint


def profile(db,*,asof,known_at=None,watermark=None,names=(),version='v2',project='baseline',
            output_root,ledger,reference=None,explain=False,progress=None):
    pins=artifacts(project);revision=code_revision(pins)
    contract=registry(project);names=list(names) or list(contract['outputs'])
    if len(set(names))!=len(names) or set(names)-set(contract['outputs']):
        raise ValidationError('profile requires distinct registered outputs')
    root=Path(output_root)/uuid.uuid4().hex;root.mkdir(parents=True,exist_ok=False)
    run_id=root.name;started=perf_counter();records=[];outputs={}
    with ViewSession(db,asof=asof,known_at=known_at,watermark=watermark) as session:
        tick=perf_counter();inputs=input_manifest(session);input_seconds=perf_counter()-tick
        tick=perf_counter();session.validate_dependencies(names,defer=['token_errors','seat_errors','journal_errors']);validation_seconds=perf_counter()-tick
        manifest=dict(schema_version='tokenledger-views/v1',execution_version=session.execution_version,run_id=run_id,
            asof=asof,known_at=iso(session.known_at),watermark=session.watermark,definition_version=version,project=project,
            stream_path=str(Path(db).resolve()),inputs=inputs,runtime=runtime(),execution_artifacts=pins,
            execution_hash=digest(pins),**revision,started_at=iso(datetime.now(timezone.utc)),
            settings=dict(threads=4,memory_limit='4GB',timezone='UTC'),bigquery=dict(status='not_run'),
            observations=dict(setup_seconds=session.setup_seconds,input_hash_seconds=input_seconds,validation_seconds=validation_seconds),
            profiling=bool(explain),validation_results=session.validation_results)
        batch_tables={};batch_for={};batches=[]
        for label,(members,_) in session.batch_groups.items():
            requested=[name for name in names if name in members]
            if len(requested)>1 and not explain:
                for name in requested: batch_for[name]=(label,requested)
        manifest['observations']['batches']=batches
        for name in names:
            if name in batch_for and name not in batch_tables:
                label,grouped=batch_for[name]
                tick=perf_counter();batch_tables.update(session.outputs(grouped,version,project));seconds=perf_counter()-tick
                batch_observation=dict(family=label,outputs=grouped,seconds=seconds,execute_seconds=session.last_execute_seconds,
                    arrow_seconds=session.last_arrow_seconds,dependencies=session.last_dependencies,
                    query_sha256=hashlib.sha256(session.last_query.encode()).hexdigest(),file=label+'-batch.sql')
                (root/batch_observation['file']).write_text(session.last_query,encoding='utf-8');batches.append(batch_observation)
            if name in batch_tables:
                table=batch_tables.pop(name);query_seconds=None;execute_seconds=None;arrow_seconds=None
                sql,dependencies=session.graph.compile(session.output_sql(name,version,project))
                observed=next(b for b in batches if name in b['outputs'])
                executed_file=observed['file'];executed_hash=observed['query_sha256']
            else:
                tick=perf_counter();table=session.output(name,version,project);query_seconds=perf_counter()-tick
                execute_seconds=session.last_execute_seconds;arrow_seconds=session.last_arrow_seconds
                sql=session.last_query;dependencies=list(session.last_dependencies)
                (root/(name+'-executed.sql')).write_text(session.last_executed_query,encoding='utf-8')
                executed_file=name+'-executed.sql';executed_hash=hashlib.sha256(session.last_executed_query.encode()).hexdigest()
            if set(table.column_names)!=set(contract['outputs'][name]['columns']):
                raise ValidationError('output contract differs: '+name)
            keys=contract['outputs'][name]['key']
            tick=perf_counter();fingerprint=table_fingerprint(session.conn,table,keys);hash_seconds=perf_counter()-tick
            comparison=None
            if reference:
                path=Path(reference)/(name+'-baseline.parquet')
                expected=pq.read_table(path)
                if 'receipt_id' in expected.column_names: expected=expected.drop(['receipt_id'])
                comparison=compare_tables(table,expected,keys)
            result=dict(output=name,population=fingerprint,comparison=comparison)
            record=dict(schema_version='tokenledger-views/v1',run_id=run_id,result=result,
                definition_version='accounting/v1;nrr/'+version,inputs=inputs,query_hash=hashlib.sha256(sql.encode()).hexdigest(),
                git_sha=revision['git_sha'],execution_hash=digest(pins),asof=asof,known_at=manifest['known_at'],watermark=session.watermark)
            record['receipt_id']=receipt_id(record);records.append(record)
            tick=perf_counter()
            pq.write_table(table.append_column('receipt_id',pa.array([record['receipt_id']]*len(table),type=pa.string())),root/(name+'.parquet'))
            write_seconds=perf_counter()-tick
            (root/(name+'.sql')).write_text(sql,encoding='utf-8')
            if explain:
                session.conn.execute("PRAGMA enable_profiling='json'")
                session.conn.execute('SET profiling_output=?',[str((root/(name+'-profile.json')).resolve())])
                session.conn.execute(session.last_executed_query).to_arrow_table(65536)
                session.conn.execute('PRAGMA disable_profiling')
            outputs[name]=dict(receipt_id=record['receipt_id'],query_seconds=query_seconds,hash_seconds=hash_seconds,
                execute_seconds=execute_seconds,arrow_seconds=arrow_seconds,
                write_seconds=write_seconds,dependencies=dependencies,query_sha256=record['query_hash'],
                executed_sql_file=executed_file,executed_query_sha256=executed_hash)
            if progress: progress(dict(output=name,rows=len(table),query_seconds=query_seconds,
                status='uncompared' if comparison is None else comparison['status']))
        manifest['outputs']=outputs
        manifest['dependencies']={name:dict(sql_sha256=hashlib.sha256(sql.encode()).hexdigest()) for name,sql in session.graph.nodes.items()}
        manifest['relations']=session.conn.execute("SELECT database_name,schema_name,table_name,'table' FROM duckdb_tables() WHERE NOT internal UNION ALL SELECT database_name,schema_name,view_name,'view' FROM duckdb_views() WHERE NOT internal").fetchall()
        manifest['observations']['total_seconds']=perf_counter()-started
    if artifacts(project)!=pins: raise ValidationError('execution artifacts changed during profile')
    raw=canonical(manifest)+'\n';(root/'run.json').write_text(raw,encoding='utf-8',newline='\n')
    observation=dict(schema_version='tokenledger-views-observation/v1',run_id=run_id,
        result=dict(manifest_sha256=hashlib.sha256(raw.encode()).hexdigest(),observations=manifest['observations'],outputs=outputs),
        definition_version='duckdb-views/v1',inputs=inputs,query_hash=digest(pins),git_sha=revision['git_sha'],
        execution_hash=digest(pins),asof=asof,known_at=manifest['known_at'],watermark=manifest['watermark'])
    observation['receipt_id']=receipt_id(observation);records.append(observation)
    (root/'rows.jsonl').write_text(''.join(canonical(r)+'\n' for r in records),encoding='utf-8',newline='\n')
    append_receipts(ledger,records)
    return dict(status='different' if any(r['result'].get('comparison',{}).get('status')=='different' for r in records if r['result'].get('comparison')) else 'recorded',
                run_id=run_id,run_directory=str(root),receipt_ids=[r['receipt_id'] for r in records],observations=manifest['observations'])


def replay(ids,*,db=None,output_root,ledger,python=None):
    records=read_receipts(ledger);groups={};verified=[]
    for key in ids:
        row=records[key];groups.setdefault(row['run_id'],[]).append(row)
    for run_id,selected in groups.items():
        root=Path(output_root)/run_id;manifest=json.loads((root/'run.json').read_text(encoding='utf-8'))
        observations=[r for r in records.values() if r.get('run_id')==run_id and r.get('schema_version')=='tokenledger-views-observation/v1']
        if len(observations)!=1 or hashlib.sha256((root/'run.json').read_bytes()).hexdigest()!=observations[0]['result']['manifest_sha256']:
            raise ValidationError('view observation manifest mismatch')
        for record in selected:
            for key in ('run_id','git_sha','execution_hash','inputs','asof','known_at','watermark'):
                if record[key]!=manifest[key]: raise ValidationError('view receipt/manifest mismatch: '+key)
            if record['schema_version']=='tokenledger-views-observation/v1':
                if hashlib.sha256((root/'run.json').read_bytes()).hexdigest()!=record['result']['manifest_sha256']:
                    raise ValidationError('view observation manifest mismatch')
                verified.append(dict(receipt_id=record['receipt_id'],verified=True,recomputed=False,result=record['result']))
        selected=[r for r in selected if r['schema_version']=='tokenledger-views/v1']
        if not selected: continue
        if manifest.get('execution_version')!=ViewSession.execution_version:
            raise ValidationError('unregistered view execution version')
        if any(r['definition_version']!='accounting/v1;nrr/'+manifest['definition_version'] for r in selected):
            raise ValidationError('view definition differs from receipt')
        pins=artifacts(manifest['project'])
        if digest(manifest['execution_artifacts'])!=manifest['execution_hash']:
            raise ValidationError('view artifacts hash mismatch')
        if python or pins!=manifest['execution_artifacts'] or runtime()!=manifest['runtime']:
            from tl.receipts.archive import historical_replay
            verified.extend(historical_replay([r['receipt_id'] for r in selected],manifest=manifest,
                db=db or manifest['stream_path'],output_root=output_root,ledger=ledger,python=python));continue
        with ViewSession(db or manifest['stream_path'],asof=manifest['asof'],known_at=manifest['known_at'],watermark=manifest['watermark']) as session:
            if input_manifest(session)!=manifest['inputs']: raise ValidationError('view source snapshot mismatch')
            batch={};batch_for={}
            for members,_ in session.batch_groups.values():
                requested=[r['result']['output'] for r in selected if r['result']['output'] in members]
                if len(requested)>1:
                    for name in requested: batch_for[name]=requested
            for record in selected:
                name=record['result']['output']
                observed=manifest['outputs'][name]
                if hashlib.sha256((root/(name+'.sql')).read_text(encoding='utf-8').encode()).hexdigest()!=record['query_hash']:
                    raise ValidationError('retained logical query differs')
                executed=root/observed['executed_sql_file']
                if not executed.resolve().is_relative_to(root.resolve()) or hashlib.sha256(executed.read_text(encoding='utf-8').encode()).hexdigest()!=observed['executed_query_sha256']:
                    raise ValidationError('retained executed query differs')
                if name in batch_for and name not in batch:
                    batch.update(session.outputs(batch_for[name],manifest['definition_version'],manifest['project']))
                if name in batch:
                    table=batch.pop(name);sql=session.graph.compile(session.output_sql(name,manifest['definition_version'],manifest['project']))[0]
                else:
                    table=session.output(name,manifest['definition_version'],manifest['project']);sql=session.last_query
                if hashlib.sha256(sql.encode()).hexdigest()!=record['query_hash']:
                    raise ValidationError('view query changed')
                keys=registry(manifest['project'])['outputs'][name]['key']
                expected=record['result']['population']
                if table_fingerprint(session.conn,table,keys)!=expected: raise ValidationError('view population reproduction failed')
                retained=pq.read_table(root/(name+'.parquet'))
                if len(retained) and set(retained['receipt_id'].unique().to_pylist())!={record['receipt_id']}:
                    raise ValidationError('view retained receipt substitution')
                if table_fingerprint(session.conn,retained.drop(['receipt_id']),keys)!=expected:
                    raise ValidationError('view retained population mismatch')
                verified.append(dict(receipt_id=record['receipt_id'],verified=True,recomputed='spine_population',result=record['result']))
    return verified
