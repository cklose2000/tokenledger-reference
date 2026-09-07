"""Native packs render saved, re-performed populations through the shared ledger."""
from pathlib import Path
import csv
import hashlib
import json

import pyarrow.parquet as pq

from tl.metrics.definitions import FAMILIES
from tl.receipts.metrics import read_receipts
from tl.stream.events import canonical,ValidationError
from tl.views.engine import replay


def pack_run(run_id,*,output_root,ledger,pack_root):
    root=Path(output_root)/run_id
    manifest=json.loads((root/'run.json').read_text(encoding='utf-8'))
    if manifest.get('schema_version')!='tokenledger-views/v1': raise ValidationError('native pack requires a views-v1 metric run')
    names=[name for name in manifest['outputs'] if name in FAMILIES]
    if not names: raise ValidationError('native pack has no registered metric populations')
    ids=[manifest['outputs'][name]['receipt_id'] for name in names]
    # Rendering cannot bypass re-performance or replace saved population bytes.
    replay(ids,output_root=output_root,ledger=ledger)
    records=read_receipts(ledger)
    target=Path(pack_root)/manifest['asof']/run_id;target.mkdir(parents=True,exist_ok=False)
    for name in names:
        table=pq.read_table(root/(name+'.parquet'));columns=table.column_names
        lines=['# '+name,'','Synthetic data. Values follow the registered all-cuts population contract. Null means undefined. Every row links to its complete population receipt.','',
               '| '+' | '.join(columns)+' |','|'+'|'.join('---' for _ in columns)+'|']
        with (target/(name+'.csv')).open('x',encoding='utf-8',newline='') as handle:
            writer=csv.DictWriter(handle,fieldnames=columns);writer.writeheader()
            for row in table.to_pylist():
                writer.writerow(row)
                lines.append('| '+' | '.join(('undefined' if row[k] is None else str(row[k])).replace('|','\\|').replace('\n',' ') for k in columns)+' |')
        (target/(name+'.md')).write_text('\n'.join(lines)+'\n',encoding='utf-8',newline='\n')
    (target/'receipts.jsonl').write_text(''.join(canonical(records[key])+'\n' for key in ids),encoding='utf-8',newline='\n')
    (target/'run.json').write_bytes((root/'run.json').read_bytes())
    (target/'README.md').write_text('# Native synthetic disclosure pack\n\n'+
        f"Reporting date {manifest['asof']}; exclusive knowledge cutoff {manifest['known_at']}; watermark {manifest['watermark']}.\n\n"+
        'Seven-family calculations use independent columnar recognition of commercial evidence. No generated recognized-revenue assertion is an input. Each saved population was freshly reproduced before rendering.\n\n'+
        'The run manifest pins definition versions, code/runtime, complete source fingerprint and compiled query hashes. Source data and query populations remain in the isolated artifact root for `tl receipt`; this directory is a presentation artifact, not a self-contained source archive.\n\n'+
        '\n'.join(f'- [{name}]({name}.md), [CSV]({name}.csv)' for name in names)+'\n',encoding='utf-8',newline='\n')
    files={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(target.iterdir()) if p.is_file()}
    (target/'manifest.json').write_text(canonical(dict(schema_version='tokenledger-views-pack/v1',run_id=run_id,asof=manifest['asof'],files=files,receipt_ids=ids))+'\n',encoding='utf-8',newline='\n')
    return dict(path=str(target),run_id=run_id,files=len(files),manifest_sha256=hashlib.sha256((target/'manifest.json').read_bytes()).hexdigest())
