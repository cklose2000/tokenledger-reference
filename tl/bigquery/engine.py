"""Fresh BigQuery populations, source checks and shared content-addressed receipts."""
from pathlib import Path
import hashlib
import json
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

from tl.bigquery.client import Client
from tl.bigquery.export import read_export
from tl.bigquery.hashing import fingerprint
from tl.compare.matching import compare_tables
from tl.receipts.metrics import append_receipts, digest, read_receipts, receipt_id
from tl.stream import ValidationError
from tl.stream.events import canonical


def _save(path, value):
    Path(path).write_text(canonical(value)+'\n', encoding='utf-8', newline='\n')


def run(binding, directory, *, output_root, ledger, reference=None, client=None, dry_run=False):
    manifest=read_export(directory,binding);directory=Path(directory)
    expected_tables={};reference_identity=None
    if reference:
        from tl.bigquery.reference import verify
        expected_tables,reference_identity=verify(reference,manifest)
    client=client or Client(binding)
    job_start=len(client.jobs)
    if dry_run:
        estimates={name:client.estimate((directory/name).read_text(encoding='utf-8'))
                   for name in manifest['files'] if name.startswith(('queries/','checks/')) or name=='snapshot.sql'}
        return dict(status='dry_run',native_execution='not_run',estimates=estimates,binding_identity=binding.identity)
    root=Path(output_root)/uuid.uuid4().hex;root.mkdir(parents=True,exist_ok=False)
    run_id=root.name;records=[];results={}
    # Retain exact executable export beside results; no dependency on a user's
    # original export directory for replay.
    import shutil
    shutil.copytree(directory,root/'export')
    _save(root/'state.json',dict(status='running',run_id=run_id,binding_identity=binding.identity))
    try:
        table,_=client.query((directory/'snapshot.sql').read_text(encoding='utf-8'),label='source_snapshot')
        inputs=fingerprint(table,['activity_id'],source=True)
        if inputs!=manifest['inputs']: raise ValidationError('cloud source does not match the frozen validated stream')
        validation={}
        for domain in manifest['validation_domains']:
            table,job=client.query((directory/('checks/'+domain+'.sql')).read_text(encoding='utf-8'),label=domain)
            validation[domain]=table.to_pylist()
            if any(row['failures']!=0 for row in validation[domain]):
                raise ValidationError('native BigQuery reporting checks failed: '+domain)
        for name,spec in manifest['outputs'].items():
            sql=(directory/('queries/'+name+'.sql')).read_text(encoding='utf-8')
            table,job=client.query(sql,label=name)
            if set(table.column_names)!=set(spec['columns']): raise ValidationError('native output columns differ: '+name)
            population=fingerprint(table,spec['key'])
            comparison=None
            if reference:
                comparison=compare_tables(table,expected_tables[name],spec['key'])
            result=dict(output=name,population=population,comparison=comparison,job=job,reference=reference_identity)
            record=dict(schema_version='tokenledger-bigquery/v1',run_id=run_id,result=result,
                definition_version='accounting/v1;nrr/'+manifest['definition_version'],
                inputs=inputs,query_hash=hashlib.sha256(sql.encode()).hexdigest(),
                git_sha=manifest['git_sha'],execution_hash=manifest['execution_hash'],
                binding_identity=binding.identity,asof=manifest['asof'],known_at=manifest['known_at'],watermark=manifest['watermark'])
            record['receipt_id']=receipt_id(record);records.append(record)
            pq.write_table(table.append_column('receipt_id',pa.array([record['receipt_id']]*len(table),type=pa.string())),root/(name+'.parquet'))
            results[name]=dict(receipt_id=record['receipt_id'],comparison=comparison)
        # Detect a privileged source edit during execution. Later append positions
        # cannot enter this fixed watermark, but destructive admin changes can.
        table,_=client.query((directory/'snapshot.sql').read_text(encoding='utf-8'),label='source_recheck')
        if fingerprint(table,['activity_id'],source=True)!=inputs: raise ValidationError('source changed during native execution')
        status='different' if any(v['comparison'] and v['comparison']['status']!='equivalent' for v in results.values()) else 'recorded'
        report=dict(schema_version='tokenledger-bigquery-run/v1',run_id=run_id,status=status,
            export_sha256=digest(manifest),binding_identity=binding.identity,validation=validation,
            outputs=results,jobs=client.jobs[job_start:],iam_negative_tests='not_run',
            authority_mode=binding.authority_mode,reference=reference_identity,
            parity='not_run' if reference is None else ('different' if status=='different' else 'equivalent'))
        _save(root/'run.json',report)
        observation=dict(schema_version='tokenledger-bigquery-observation/v1',run_id=run_id,
            result=dict(manifest_sha256=digest(report),jobs=client.jobs[job_start:]),definition_version='bigquery/v1',
            inputs=inputs,query_hash=digest(manifest['files']),git_sha=manifest['git_sha'],
            execution_hash=manifest['execution_hash'],binding_identity=binding.identity,
            asof=manifest['asof'],known_at=manifest['known_at'],watermark=manifest['watermark'])
        observation['receipt_id']=receipt_id(observation);records.append(observation)
        _save(root/'state.json',dict(status=status,run_id=run_id))
        (root/'receipts.jsonl').write_text(''.join(canonical(r)+'\n' for r in records),encoding='utf-8',newline='\n')
        append_receipts(ledger,records)
        return dict(status=status,run_id=run_id,run_directory=str(root.resolve()),parity=report['parity'],
            receipt_ids=[r['receipt_id'] for r in records],jobs=client.jobs[job_start:])
    except BaseException as exc:
        _save(root/'state.json',dict(status='failed',run_id=run_id,error=str(exc),jobs=client.jobs[job_start:]))
        raise


def replay(binding, ids, *, output_root, ledger, client=None):
    records=read_receipts(ledger);verified=[]
    if any(key not in records for key in ids): raise ValidationError('unknown native BigQuery receipt')
    if any(records[key].get('schema_version') not in ('tokenledger-bigquery/v1','tokenledger-bigquery-observation/v1',
            'tokenledger-bigquery-suite-observation/v1') for key in ids):
        raise ValidationError('receipt is not a native BigQuery population or observation')
    if any(records[key].get('binding_identity')!=binding.identity for key in ids):
        raise ValidationError('receipt cloud binding differs')
    for key in ids:
        record=records[key];root=Path(output_root)/record['run_id']
        if record['schema_version']=='tokenledger-bigquery-suite-observation/v1':
            # Suite evidence has the standard artifact-root layout: metrics.jsonl
            # and parity.json beside runs/. Never execute SQL or load credentials
            # to verify an observation. A hash proves retained evidence, not a
            # second successful native trial or newly measured elapsed time.
            report=json.loads((Path(output_root).parent/'parity.json').read_text(encoding='utf-8'))
            if report.get('schema_version')!='tokenledger-bigquery-parity/v1' or report.get('run_id')!=record['run_id']:
                raise ValidationError('native parity report identity differs')
            if report.get('binding_identity')!=binding.identity:
                raise ValidationError('native parity report cloud binding differs')
            if record['result']['report_sha256']!=digest(report):
                raise ValidationError('native parity report changed')
            verified.append(dict(receipt_id=key,verified=True,recomputed=False,result=record['result']));continue
        report=json.loads((root/'run.json').read_text(encoding='utf-8'))
        if report.get('binding_identity')!=binding.identity or report.get('run_id')!=record['run_id']:
            raise ValidationError('native run manifest identity differs')
        observations=[r for r in records.values() if r.get('run_id')==record['run_id'] and r.get('schema_version')=='tokenledger-bigquery-observation/v1']
        if len(observations)!=1 or observations[0]['result']['manifest_sha256']!=digest(report):
            raise ValidationError('native run manifest changed')
        if record['schema_version']=='tokenledger-bigquery-observation/v1':
            verified.append(dict(receipt_id=key,verified=True,recomputed=False,result=record['result']));continue
        manifest=read_export(root/'export',binding)
        for field in ('inputs','git_sha','execution_hash','asof','known_at','watermark'):
            if record[field]!=manifest[field]: raise ValidationError('native receipt differs from export: '+field)
        client=client or Client(binding)
        source,_=client.query((root/'export/snapshot.sql').read_text(encoding='utf-8'),label='replay_source')
        if fingerprint(source,['activity_id'],source=True)!=record['inputs']: raise ValidationError('native receipt source changed')
        name=record['result']['output'];spec=manifest['outputs'][name]
        from tl.views.validation import required
        for domain in sorted(required([name])):
            check,_=client.query((root/('export/checks/'+domain+'.sql')).read_text(encoding='utf-8'),label=domain)
            if any(r['failures']!=0 for r in check.to_pylist()): raise ValidationError('native replay checks failed')
        sql=(root/('export/queries/'+name+'.sql')).read_text(encoding='utf-8')
        if hashlib.sha256(sql.encode()).hexdigest()!=record['query_hash']: raise ValidationError('native receipt query changed')
        actual,job=client.query(sql,label=name)
        retained=pq.read_table(root/(name+'.parquet'))
        if len(retained) and set(retained['receipt_id'].to_pylist())!={key}: raise ValidationError('native retained receipt substitution')
        expected=record['result']['population']
        if fingerprint(actual,spec['key'])!=expected or fingerprint(retained.drop(['receipt_id']),spec['key'])!=expected:
            raise ValidationError('native BigQuery population reproduction failed')
        verified.append(dict(receipt_id=key,verified=True,recomputed='bigquery_population',result=record['result'],replay_job=job))
    return verified
