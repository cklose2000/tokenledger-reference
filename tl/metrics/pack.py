"""Render only verified metric output rows; every displayed value retains its receipt."""

import csv
import hashlib
import json
from pathlib import Path

from tl.metrics.engine import read_run
from tl.receipts.metrics import read_receipts
from tl.stream.events import canonical


def pack_run(run_id,*,output_root=Path('metrics/out'),ledger=Path('ledger/metrics.jsonl'),pack_root=Path('docs/pack')):
    run=read_run(run_id,output_root=output_root,ledger=ledger)
    manifest=run['manifest']
    if manifest.get('schema_version')!='tokenledger-run/v1':
        from tl.stream import ValidationError
        raise ValidationError('derived reports render beside their run; pack requires a metric run')
    root=Path(pack_root)/manifest['asof']/run_id
    root.mkdir(parents=True,exist_ok=False)
    records=read_receipts(ledger)
    families=sorted({row['metric'] for row in run['rows']})
    for family in families:
        flattened=[]
        for row in run['rows']:
            if row['metric']!=family:
                continue
            for measure,value in (row['values'] or {'no_observations':None}).items():
                flattened.append(dict(dimensions=canonical(row['dimensions']),measure=measure,value=value,
                    unit=row['units'].get(measure,''),status=row['status'],definition=row['definition_version'],receipt_id=row['receipt_id']))
        with (root/f'{family}.csv').open('x',encoding='utf-8',newline='') as handle:
            writer=csv.DictWriter(handle,fieldnames=['dimensions','measure','value','unit','status','definition','receipt_id'])
            writer.writeheader()
            writer.writerows(flattened)
        lines=[f'# {family}', '', 'Synthetic reporting. Every value references a metric receipt.', '',
               '| Dimensions | Measure | Value | Unit | Status | Receipt |','|---|---|---:|---|---|---|']
        for row in flattened:
            cells=[row['dimensions'],row['measure'],'undefined' if row['value'] is None else str(row['value']),row['unit'],row['status'],row['receipt_id']]
            lines.append('| '+' | '.join(str(cell).replace('|','\\|').replace('\n',' ') for cell in cells)+' |')
        (root/f'{family}.md').write_text('\n'.join(lines)+'\n',encoding='utf-8',newline='\n')
    (root/'receipts.jsonl').write_text(''.join(canonical(records[row['receipt_id']])+'\n' for row in run['rows']),encoding='utf-8',newline='\n')
    (root/'run.json').write_bytes((Path(run['run_directory'])/'run.json').read_bytes())
    (root/'inputs.jsonl').write_bytes((Path(run['run_directory'])/'inputs.jsonl').read_bytes())
    readme=['# Synthetic disclosure pack','',f'Economic as-of: {manifest["asof"]}. Knowledge cutoff: {manifest["known_at"]}.',
            f'Run: `{run_id}`. Git revision: `{manifest["git_sha"]}`.', '',
            'This is a local synthetic demonstration. Null ratios are undefined. Definition and input hashes are in run.json.', '',
            'Receipt IDs cover each row and every value in that row. Inputs include the complete candidate snapshot, including exclusions.', '',
            *[f'- [{family}]({family}.md), [CSV]({family}.csv)' for family in families]]
    (root/'README.md').write_text('\n'.join(readme)+'\n',encoding='utf-8',newline='\n')
    files={path.name:hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(root.iterdir()) if path.is_file()}
    (root/'manifest.json').write_text(canonical(dict(schema_version='tokenledger-pack/v1',run_id=run_id,asof=manifest['asof'],files=files))+'\n',encoding='utf-8',newline='\n')
    return dict(path=str(root),run_id=run_id,files=len(files),manifest_sha256=hashlib.sha256((root/'manifest.json').read_bytes()).hexdigest())
