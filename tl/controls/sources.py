"""Pre-ingestion source evidence and exact ID/content reconciliation."""
from collections import Counter,defaultdict
from datetime import datetime,date
from decimal import Decimal
import hashlib
import json
from pathlib import Path

from tl.receipts.metrics import digest
from tl.stream import Activity,Stream,StreamReader,ValidationError
from tl.stream.events import Catalog,canonical,iso,timestamp,UTC

SCHEMA='tokenledger-source-capture/v1'
CORE=('activity_id','ts','customer','anonymous_customer_id','activity','feature_json','revenue_impact','link')


def canonical_event(event,catalog):
    normalized=catalog.validate(event)
    return dict(activity_id=event.activity_id,ts=iso(normalized['event_ts']),customer=event.customer,
        anonymous_customer_id=event.anonymous_customer_id,activity=event.activity,
        feature_json=normalized['feature_json'],revenue_impact=normalized['revenue_impact'],link=event.link)


def inventory(raw,catalog):
    observations=[]
    unique={}
    for number,line in enumerate(raw.decode('utf-8').splitlines(),1):
        entry=dict(observation=number,raw_sha256=hashlib.sha256(line.encode()).hexdigest())
        try:
            original=json.loads(line)
            event=canonical_event(Activity(**original['event']),catalog)
            arrival=timestamp(original['recorded_at'])
            if arrival<timestamp(event['ts']):
                raise ValidationError('simulated arrival precedes economic time')
            entry.update(activity_id=event['activity_id'],event=event,recorded_at=iso(arrival),
                         event_sha256=digest(event),status='accepted')
            key=event['activity_id']
            if key in unique:
                same=unique[key]['event_sha256']==entry['event_sha256'] and unique[key]['recorded_at']==entry['recorded_at']
                entry['status']='duplicate' if same else 'conflict'
            else:
                unique[key]=entry
        except (ValueError,TypeError,KeyError) as exc:
            entry.update(status='rejected',error=str(exc))
        observations.append(entry)
    if not observations:
        raise ValidationError('source capture requires observations')
    return observations


def capture(db,input_path,destination,*,source,start,end,catalog_path='definitions/activities'):
    if not source.startswith('sim:'):
        raise ValidationError('P3-C capture accepts explicitly synthetic sources only; personal binding is G0')
    if date.fromisoformat(start)>=date.fromisoformat(end):
        raise ValidationError('source coverage must be a nonempty half-open date interval')
    catalog=Catalog(Path(catalog_path))
    raw=Path(input_path).read_bytes()
    rows=inventory(raw,catalog)
    with StreamReader(db).connect() as conn:
        watermark=conn.execute('SELECT coalesce(max(_stream_position),0) FROM stream.activity').fetchone()[0]
        existing={r[0] for r in conn.execute('SELECT activity_id FROM stream.activity').fetchall()}
    if existing & {row.get('activity_id') for row in rows}:
        raise ValidationError('capture must precede ingestion of its source IDs; a stream recount is not source evidence')
    manifest=dict(schema_version=SCHEMA,source=source,synthetic=True,captured_at=iso(datetime.now(UTC)),
        coverage_start=start,coverage_end=end,coverage_basis='declared synthetic source population, not authenticated provider coverage',
        pre_ingestion_watermark=watermark,catalog_sha256=catalog.digest,
        source_sha256=hashlib.sha256(raw).hexdigest(),observations=rows)
    root=Path(destination);root.mkdir(parents=True,exist_ok=False)
    (root/'source.jsonl').write_bytes(raw)
    content=(canonical(manifest)+'\n').encode()
    (root/'manifest.json').write_bytes(content)
    return dict(status='captured',path=str(root/'manifest.json'),sha256=hashlib.sha256(content).hexdigest(),
                counts=dict(Counter(row['status'] for row in rows)))


def load_capture(path,catalog_path='definitions/activities'):
    path=Path(path)
    manifest=json.loads(path.read_bytes())
    if manifest.get('schema_version')!=SCHEMA or manifest.get('synthetic') is not True:
        raise ValidationError('unsupported source capture')
    if not manifest['source'].startswith('sim:') or date.fromisoformat(manifest['coverage_start'])>=date.fromisoformat(manifest['coverage_end']):
        raise ValidationError('invalid synthetic capture scope')
    catalog=Catalog(Path(catalog_path))
    raw=(path.parent/'source.jsonl').read_bytes()
    if (hashlib.sha256(raw).hexdigest()!=manifest['source_sha256'] or catalog.digest!=manifest['catalog_sha256']
            or inventory(raw,catalog)!=manifest['observations']):
        raise ValidationError('source capture content or registry hash mismatch')
    return manifest


def ingest(db,path,*,catalog_path='definitions/activities'):
    manifest=load_capture(path,catalog_path)
    if any(r['status']=='conflict' for r in manifest['observations']):
        raise ValidationError('conflicting source IDs; preserve capture and resolve before ingest')
    events=[(Activity(**row['event']),timestamp(row['recorded_at'])) for row in manifest['observations'] if row['status']=='accepted']
    result=Stream(db,catalog_path).append_simulation(events,source=manifest['source'],actor='synthetic-source-import')
    return dict(status='ingested_with_exceptions' if any(r['status']=='rejected' for r in manifest['observations']) else 'ingested',
                **result,rejected_observations=sum(r['status']=='rejected' for r in manifest['observations']))


def reconcile(table,manifests,*,start,end):
    actual={}
    for row in table.to_pylist():
        if start<=row['ts'].date().isoformat()<end:
            event={key:row[key] for key in CORE}
            event['ts']=iso(event['ts']);event['feature_json']=json.loads(event['feature_json'])
            event['revenue_impact']=None if event['revenue_impact'] is None else format(event['revenue_impact'],'.2f')
            actual[event['activity_id']]=(row,event)
    expected={};exceptions=[];duplicates=0
    if not manifests:
        exceptions.append(dict(kind='missing_source_population'))
    sources=set()
    for manifest in manifests:
        source=manifest['source']
        if source in sources:
            raise ValidationError('overlapping captures for a source require an explicit revision policy')
        sources.add(source)
        if manifest['coverage_start']>start or manifest['coverage_end']<end:
            exceptions.append(dict(kind='partial_source_coverage',source=source))
        for obs in manifest['observations']:
            if obs['status'] in ('rejected','conflict'):
                exceptions.append(dict(kind=obs['status'],source=source,observation=obs['observation'],activity_id=obs.get('activity_id')))
            elif start<=obs['event']['ts'][:10]<end:
                if obs['status']=='duplicate':
                    duplicates+=1
                    continue
                key=obs['activity_id']
                if key in expected:
                    raise ValidationError('source captures reuse an activity ID')
                expected[key]=(manifest,obs)
    for key,(manifest,obs) in expected.items():
        if key not in actual:
            exceptions.append(dict(kind='missing_activity',activity_id=key,source=manifest['source']))
        else:
            row,event=actual[key]
            if row['_source']!=manifest['source'] or digest(event)!=obs['event_sha256']:
                exceptions.append(dict(kind='altered_activity',activity_id=key,source=manifest['source']))
            if iso(row['_recorded_at'])!=obs['recorded_at']:
                exceptions.append(dict(kind='altered_arrival',activity_id=key))
            if row['_stream_position']<=manifest['pre_ingestion_watermark']:
                exceptions.append(dict(kind='not_pre_ingestion_evidence',activity_id=key))
    for key,(row,_) in actual.items():
        if key not in expected:
            exceptions.append(dict(kind='unexpected_activity',activity_id=key,source=row['_source']))
    totals=defaultdict(lambda:dict(expected_count=0,actual_count=0,expected_cents=0,actual_cents=0))
    for manifest,obs in expected.values():
        event=obs['event'];key=(manifest['source'],event['ts'][:7],event['activity'])
        totals[key]['expected_count']+=1
        totals[key]['expected_cents']+=int(Decimal(event['revenue_impact'] or '0')*100)
    for row,event in actual.values():
        key=(row['_source'],event['ts'][:7],event['activity'])
        totals[key]['actual_count']+=1
        totals[key]['actual_cents']+=int(Decimal(event['revenue_impact'] or '0')*100)
    return dict(passed=not exceptions,scope='synthetic pre-ingestion IDs and canonical content; signed recognized amounts',
        expected_events=len(expected),actual_events=len(actual),duplicate_observations=duplicates,exceptions=exceptions,
        periods=[dict(source=k[0],period=k[1],activity=k[2],**v) for k,v in sorted(totals.items())])
