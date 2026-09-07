"""Verify the independent baseline population and its same-cutoff receipt."""
from pathlib import Path
import hashlib
import json

import duckdb
import pyarrow.parquet as pq

from tl.receipts.metrics import read_receipts
from tl.stream import ValidationError
from tl.views.hashing import table_fingerprint


def verify(directory,export_manifest):
    root=Path(directory)
    manifest=json.loads((root/'run.json').read_text(encoding='utf-8'))
    if manifest.get('schema_version')!='tokenledger-views-comparison/v1' or manifest.get('status')!='equivalent':
        raise ValidationError('reference requires an equivalent native comparison with independently built dbt outputs')
    for field in ('asof','known_at','watermark','definition_version','git_sha','execution_hash'):
        if manifest[field]!=export_manifest[field]: raise ValidationError('baseline reference cutoff/code differs: '+field)
    if manifest['inputs']!=export_manifest['local_native_inputs']:
        raise ValidationError('baseline reference source population differs')
    records=read_receipts(root/'rows.jsonl')
    observed=[r for r in records.values() if r['schema_version']=='tokenledger-views-comparison-observation/v1']
    if len(observed)!=1 or observed[0]['result']['manifest_sha256']!=hashlib.sha256((root/'run.json').read_bytes()).hexdigest():
        raise ValidationError('reference manifest does not match its observation receipt')
    run=json.loads((root/'dbt-artifacts/run_results.json').read_text(encoding='utf-8'))
    if not run['results'] or any(r['status'] not in ('success','pass') for r in run['results']):
        raise ValidationError('reference dbt build/tests did not all pass')
    populations={};identities={}
    with duckdb.connect() as conn:
        for name,spec in export_manifest['outputs'].items():
            selected=[r for r in records.values() if r['schema_version']=='tokenledger-views-comparison/v1' and r['result']['output']==name]
            if len(selected)!=1: raise ValidationError('reference population receipt missing or duplicated: '+name)
            receipt=selected[0]
            for field in ('inputs','asof','known_at','watermark','git_sha','execution_hash'):
                if receipt[field]!=manifest[field]: raise ValidationError('reference receipt differs: '+field)
            table=pq.read_table(root/(name+'-baseline.parquet'))
            if len(table) and set(table['receipt_id'].unique().to_pylist())!={receipt['receipt_id']}:
                raise ValidationError('reference receipt substitution')
            table=table.drop(['receipt_id'])
            if table_fingerprint(conn,table,spec['key'])!=receipt['result']['baseline_population']:
                raise ValidationError('reference baseline population changed: '+name)
            populations[name]=table;identities[name]=receipt['receipt_id']
    return populations,dict(run_id=manifest['run_id'],observation_receipt_id=observed[0]['receipt_id'],populations=identities,
                           implementation='independent dbt baseline, locally executed; no cloud baseline timing claim')
