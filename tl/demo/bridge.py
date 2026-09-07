"""An exact account bridge whose two populations reperform from the stream."""
from pathlib import Path
import hashlib,json,uuid

import pyarrow.parquet as pq

from tl.compare.engine import artifacts
from tl.metrics.engine import runtime
from tl.receipts.metrics import append_receipts,code_revision,digest,read_receipts,receipt_id
from tl.stream import StreamReader,ValidationError
from tl.stream.events import canonical

SCHEMA='tokenledger-demo-bridge/v1'


def hashed(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def calculate(old_id,new_id,*,db,output_root,ledger):
    records=read_receipts(ledger)
    parents=[records[key] for key in (old_id,new_id)]
    if any(p.get('schema_version')!='tokenledger-views/v1' or p['result']['output']!='close_accounts' for p in parents):
        raise ValidationError('demo bridge requires two native close-account populations')
    old,new=parents
    if old['asof']!=new['asof'] or old['definition_version']!=new['definition_version'] or old['execution_hash']!=new['execution_hash']:
        raise ValidationError('demo bridge requires identical reporting policy and execution')
    from tl.stream.events import timestamp
    if old['watermark']>=new['watermark'] or timestamp(old['known_at'])>=timestamp(new['known_at']):
        raise ValidationError('demo bridge needs increasing arrival cutoff and watermark')
    snapshots=[];populations=[]
    for parent in parents:
        table=StreamReader(db).snapshot(asof=parent['asof'],known_at=parent['known_at'],watermark=parent['watermark'])
        # Occurrence and next-event pointers are recomputed after cutoff filtering.
        snapshots.append({r['activity_id']:{k:v for k,v in r.items() if k not in ('activity_occurrence','activity_repeated_at')}
                          for r in table.to_pylist()})
        rows=pq.read_table(Path(output_root)/parent['run_id']/'close_accounts.parquet').to_pylist()
        populations.append({(str(r['month']),r['account']):int(r['amount_cents']) for r in rows})
    before,after=snapshots
    if not set(before)<=set(after) or any(row!=after[key] for key,row in before.items()):
        raise ValidationError('demo bridge source prefix changed')
    added=sorted(set(after)-set(before))
    if len(added)!=1 or after[added[0]]['activity']!='usage_invoiced':
        raise ValidationError('controlled demo requires exactly one newly visible invoice')
    rows=[]
    for key in sorted(set(populations[0])|set(populations[1])):
        a,b=[p.get(key,0) for p in populations]
        if a!=b: rows.append(dict(month=key[0],account=key[1],before_cents=a,after_cents=b,delta_cents=b-a))
    if not rows or sum(r['delta_cents'] for r in rows)!=0:
        raise ValidationError('late-invoice bridge must change the close and balance to the cent')
    return dict(asof=old['asof'],original_known_at=old['known_at'],current_known_at=new['known_at'],
        scope='Changed account balances only; omitted accounts are unchanged. Debit positive, credit negative.',
        rows=rows,responsible_activity_ids=added,
        causal_basis='Identical policy and economic cutoff; full source-prefix equality; exactly one newly visible invoice',
        original_receipt_id=old_id,current_receipt_id=new_id)


def create(old_id,new_id,*,db,output_root,ledger):
    from tl.views.engine import replay
    pins=artifacts();revision=code_revision(pins)
    replay([old_id,new_id],db=db,output_root=output_root,ledger=ledger)
    result=calculate(old_id,new_id,db=db,output_root=output_root,ledger=ledger)
    records=read_receipts(ledger);old,new=[records[k] for k in (old_id,new_id)]
    run_id=uuid.uuid4().hex;root=Path(output_root)/run_id;root.mkdir()
    manifest=dict(schema_version=SCHEMA,run_id=run_id,stream_path=str(Path(db).resolve()),
        execution_artifacts=pins,execution_hash=digest(pins),runtime=runtime(),
        definition_version='native-close-bridge/v1',query_hash=digest(pins),
        inputs=dict(original=old['inputs'],current=new['inputs']),asof=old['asof'],
        known_at=new['known_at'],watermark=new['watermark'],parents=[old_id,new_id],**revision)
    if artifacts()!=pins: raise ValidationError('demo bridge code changed during execution')
    (root/'run.json').write_text(canonical(manifest)+'\n',encoding='utf-8',newline='\n')
    record={k:manifest[k] for k in ('schema_version','run_id','definition_version','query_hash','inputs','asof','known_at','watermark','git_sha','execution_hash')}
    record.update(result=result,manifest_sha256=hashed(root/'run.json'))
    record['receipt_id']=receipt_id(record)
    (root/'bridge.json').write_text(canonical(dict(receipt_id=record['receipt_id'],**result))+'\n',encoding='utf-8',newline='\n')
    append_receipts(ledger,[record])
    return dict(receipt_id=record['receipt_id'],run_id=run_id,**result)


def replay(ids,*,db,output_root,ledger,python=None):
    from tl.views.engine import replay as replay_parents
    records=read_receipts(ledger);answers=[]
    for key in ids:
        record=records[key];root=Path(output_root)/record['run_id']
        if hashed(root/'run.json')!=record['manifest_sha256']: raise ValidationError('demo bridge manifest changed')
        manifest=json.loads((root/'run.json').read_bytes())
        for field in ('schema_version','run_id','definition_version','query_hash','inputs','asof','known_at','watermark','git_sha','execution_hash'):
            if manifest[field]!=record[field]: raise ValidationError('demo bridge receipt differs from manifest')
        if digest(manifest['execution_artifacts'])!=manifest['execution_hash']: raise ValidationError('demo bridge artifact identity changed')
        if python or artifacts()!=manifest['execution_artifacts'] or runtime()!=manifest['runtime']:
            from tl.receipts.archive import historical_replay
            answers.extend(historical_replay([key],manifest=manifest,db=db or manifest['stream_path'],output_root=output_root,ledger=ledger,python=python));continue
        source=db or manifest['stream_path']
        replay_parents(manifest['parents'],db=source,output_root=output_root,ledger=ledger)
        actual=calculate(*manifest['parents'],db=source,output_root=output_root,ledger=ledger)
        if actual!=record['result'] or json.loads((root/'bridge.json').read_bytes())!=dict(receipt_id=key,**actual):
            raise ValidationError('demo bridge reproduction differs')
        answers.append(dict(receipt_id=key,verified=True,recomputed=True,result=actual))
    return answers
