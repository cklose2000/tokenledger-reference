"""Content-addressed metric receipts and exact input manifests, without authority claims."""

from datetime import date,datetime
from decimal import Decimal,localcontext
import hashlib
import json
import math
from pathlib import Path
import subprocess
import pyarrow.compute as pc

from tl.stream.events import ValidationError,canonical,iso


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def normalized(value):
    if isinstance(value,datetime):
        return iso(value)
    if isinstance(value,date):
        return value.isoformat()
    if isinstance(value,Decimal):
        return format(value,'f')
    if isinstance(value,float):
        if not math.isfinite(value):
            raise ValidationError('nonfinite metric value')
        with localcontext() as context:
            context.prec=400
            return format(Decimal(str(value)).quantize(Decimal('0.000000000001')),'f')
    return value


def ordered_batches(table, keys, size=8192):
    """Sort narrow row indexes, then gather bounded batches of the wide payload."""
    if not len(table):
        return
    indexes = pc.sort_indices(table.select([name for name, _ in keys]), sort_keys=keys)
    for offset in range(0, len(indexes), size):
        yield table.take(indexes.slice(offset, size))


def input_manifest(snapshot,destination=None):
    """Ordered per-event hashes pin the complete candidate snapshot, including exclusions."""
    whole=hashlib.sha256()
    count=0
    first=last=None
    low=high=None
    handle=Path(destination).open('xb') if destination else None
    try:
        for batch in ordered_batches(snapshot,[('activity_id','ascending')]):
            for original in batch.to_pylist():
                row={key:(iso(value) if isinstance(value,datetime) else format(value,'f') if isinstance(value,Decimal) else value)
                     for key,value in original.items()}
                row['feature_json']=json.loads(row['feature_json'])
                line=(canonical(dict(activity_id=row['activity_id'],sha256=digest(row)))+'\n').encode()
                whole.update(line)
                if handle:
                    handle.write(line)
                count+=1
                first=first or row['activity_id']
                last=row['activity_id']
                position=row['_stream_position']
                low=position if low is None else min(low,position)
                high=position if high is None else max(high,position)
    finally:
        if handle:
            handle.close()
    return dict(activity_id_min=first,activity_id_max=last,activity_count=count,
                input_row_range=[low,high],sha256=whole.hexdigest(),
                scope='complete knowledge-filtered candidate stream, including metadata and excluded activities')


def execution_artifacts(definitions):
    paths={Path('pyproject.toml'),*Path('tl').rglob('*.py'),*Path('tl/stream').glob('*.sql'),*Path('tl/views').rglob('*.sql'),
           *Path('scaffolds').glob('*.sql'),*Path('definitions/activities').glob('*.yaml'),
           *Path('definitions/allocation').glob('*.yaml'),*Path('definitions/generators').glob('*.yaml'),
           *Path('definitions/derivations').glob('*.yaml'),
           *Path('definitions/controls').glob('*.yaml'),
           Path('definitions/policies/recognition/v1.yaml'),Path('requirements/replay-1.2.1.txt')}
    for definition in definitions:
        paths.add(definition.artifact_path)
        paths.add(Path(definition.spec['formula']['sql']))
    return {path.as_posix():hashlib.sha256(path.read_bytes().replace(b'\r\n',b'\n')).hexdigest() for path in sorted(paths)}


def code_revision(artifacts):
    result=subprocess.run(['git','rev-parse','HEAD'],capture_output=True,text=True,check=True)
    sha=result.stdout.strip()
    modified=[]
    request=''.join(f'{sha}:{path}\n' for path in artifacts).encode()
    blobs=subprocess.run(['git','cat-file','--batch'],input=request,capture_output=True,check=True).stdout
    position=0
    for path,expected in artifacts.items():
        end=blobs.find(b'\n',position)
        header=blobs[position:end].split()
        if len(header)==3 and header[1]==b'blob':
            size=int(header[2])
            content=blobs[end+1:end+1+size]
            position=end+size+2
        else:
            content=None
            position=end+1
        if content is None or hashlib.sha256(content.replace(b'\r\n',b'\n')).hexdigest()!=expected:
            modified.append(path)
    return dict(git_sha=sha,execution_artifacts_match_git=not modified,modified_execution_artifacts=modified)


def metric_row(definition,raw):
    dimensions={}
    values={}
    units={}
    status=raw.get('status','defined')
    for key,value in raw.items():
        if key=='status':
            continue
        if key in definition.dimensions:
            if key.endswith('_cents'):
                dimensions[key.removesuffix('_cents')+'_usd']=None if value is None else format(Decimal(value)/100,'.2f')
            else:
                dimensions[key]=normalized(value)
        elif 'value_units' in definition.spec:
            if key not in definition.spec['value_units']:
                raise ValidationError('application measure has no registered unit')
            values[key]=normalized(value)
            units[key]=definition.spec['value_units'][key]
        else:
            name=key.removesuffix('_cents')+'_usd' if key.endswith('_cents') else key
            if key.endswith('_cents'):
                values[name]=None if value is None else format(Decimal(value)/100,'.2f')
                units[name]='USD'
            else:
                values[name]=normalized(value)
                units[name]=('USD/Mtok' if 'per_mtok' in name else 'USD/MW' if name.startswith('revenue_per_') else
                             'USD/paid seat' if name=='arpu' else 'USD/paid account' if name=='arpa' else
                             'MW' if name.endswith('_mw') else 'tokens' if name=='tokens' else
                             'ratio' if any(word in name for word in ('share','rate','margin','churn','nrr','coverage','utilization'))
                             and name not in ('churned_accounts',) else 'count')
    return dict(metric=definition.name,definition_version=definition.version,dimensions=dimensions,values=values,units=units,status=status)


def receipt_id(record):
    return 'mr-'+digest({key:value for key,value in record.items() if key!='receipt_id'})[:32]


from tl.receipts.journal import ledger_lock


def append_receipts(path,records):
    from tl.receipts.journal import append_batch
    append_batch(path,records)


def read_receipts(path):
    path=Path(path)
    if not path.is_file():
        raise ValidationError('metric receipt ledger does not exist')
    from tl.receipts.journal import pending_path
    if pending_path(path).exists():
        raise ValidationError('ledger recovery required; pending append is not a completed run')
    return parse_receipts(path.read_bytes())


def parse_receipts(content):
    if content and not content.endswith(b'\n'):
        raise ValidationError('incomplete metric ledger tail; preserve evidence for recovery')
    result={}
    for line in content.decode('utf-8').splitlines():
        try:
            record=json.loads(line)
        except ValueError as exc:
            raise ValidationError('invalid metric ledger tail; preserve evidence for recovery') from exc
        if not isinstance(record,dict) or not isinstance(record.get('receipt_id'),str):
            raise ValidationError('invalid metric receipt record')
        if receipt_id(record)!=record['receipt_id']:
            raise ValidationError('metric receipt content hash mismatch')
        if record['receipt_id'] in result:
            raise ValidationError('duplicate metric receipt id')
        result[record['receipt_id']]=record
    return result
