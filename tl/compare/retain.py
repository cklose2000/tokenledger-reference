"""Portable report evidence; business replay still requires its retained source."""
from pathlib import Path
import json
import shutil
from tl.receipts.metrics import read_receipts,append_receipts
from tl.stream.events import canonical,ValidationError


def retain(bundle,destination):
    bundle=Path(bundle);destination=Path(destination)
    if destination.exists(): raise ValidationError('report evidence destination must be new')
    value=json.loads(bundle.read_text(encoding='utf-8'))
    records=read_receipts(value['ledger']);selected=set()
    destination.mkdir(parents=True)
    for run in value['runs']:
        origin=Path(run['run_directory']);target=destination/'runs'/run['run_id'];target.mkdir(parents=True)
        for name in ('run.json','rows.jsonl'):
            shutil.copyfile(origin/name,target/name)
        (target/'dbt-artifacts').mkdir()
        for name in ('run_results.json','build.log'):
            shutil.copyfile(origin/'dbt-artifacts'/name,target/'dbt-artifacts'/name)
        run['run_directory']=target.as_posix()
        selected.add(run['observation_receipt_id']);selected.update(r['receipt_id'] for r in run['results'])
    if value.get('observation'):
        observation=value['observation'];target=destination/'runs'/observation['run_id'];target.mkdir(parents=True)
        shutil.copyfile(Path(observation['run_directory'])/'run.json',target/'run.json')
        observation['run_directory']=target.as_posix();selected.add(observation['receipt_id'])
    if value.get('bridges'):
        shutil.copyfile(value['bridges']['path'],destination/'bridges.json')
        value['bridges']['path']=(destination/'bridges.json').as_posix()
    append_receipts(destination/'metrics.jsonl',[records[key] for key in sorted(selected)])
    value['ledger']=(destination/'metrics.jsonl').as_posix()
    value['retention_scope']='Report observations, receipt records, SQL/test execution evidence. Source snapshots and full business populations remain in the original private benchmark artifact root.'
    target=destination/'comparison.json';target.write_text(canonical(value)+'\n',encoding='utf-8',newline='\n')
    return target
