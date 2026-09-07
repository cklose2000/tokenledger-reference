"""Admit strict usage projections. Conflicts quarantine, never revise in place."""

from datetime import datetime,timedelta
from dataclasses import replace
import hashlib,json,re,uuid
from pathlib import Path
import yaml

from tl.stream import Activity,ValidationError
from tl.stream.events import UTC,canonical,iso,timestamp
from tl.receipts.metrics import digest
from tl.reporting.application import private_path
from tl.usage.sources import TOKEN_FIELDS

SOURCE='codex-local-token-count/v1'
KINDS=dict(zip(TOKEN_FIELDS,('total','input','cache_read','cache_write','output','reasoning_output')))


def fingerprint(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def source_time(value):
    try:
        return timestamp(value)
    except (ValueError,TypeError):
        raise ValidationError('invalid source timestamp') from None


def packet(application, directory):
    directory=private_path(directory)
    meta_path=private_path(directory/'manifest.json')
    data_path=private_path(directory/'observations.jsonl')
    if meta_path.stat().st_size>65536 or data_path.stat().st_size>64*1024*1024:
        raise ValidationError('usage sample exceeds bounded input size')
    meta=json.loads(meta_path.read_bytes())
    if not isinstance(meta,dict):
        raise ValidationError('source manifest must be an object')
    content=data_path.read_bytes()
    policy=yaml.safe_load((application.catalog_root/'policies/codex-local/v1.yaml').read_bytes())
    if (meta.get('schema_version')!='tokenledger-source-readiness/v1'
            or meta.get('source')!='codex-local-token-count' or meta.get('parser_version')!='v1'
            or meta.get('client_version') not in policy['client_versions']):
        raise ValidationError('unregistered source/parser/client version; review a bounded adapter change')
    if hashlib.sha256(content).hexdigest()!=meta.get('observations_sha256'):
        raise ValidationError('source projection hash mismatch')
    if not re.fullmatch('[0-9a-f]{64}',str(meta.get('source_sha256',''))):
        raise ValidationError('source identity hash is missing')
    account=meta.get('account_alias')
    with application.reader().connect() as conn:
        registered=conn.execute("""SELECT count(*) FROM stream.activity
            WHERE activity='application_inventory' AND customer=?
            AND json_extract_string(feature_json,'$.provider')='openai'
            AND json_extract_string(feature_json,'$.surface')='codex-cli'
            AND json_extract_string(feature_json,'$.scope')='laptop'
            AND json_extract_string(feature_json,'$.ownership')='personally_owned'""",[account]).fetchone()[0]
    if not registered:
        raise ValidationError('source account is not a confirmed personal Codex laptop binding')
    collected=source_time(meta['collected_at'])
    rows={}
    for line in content.splitlines():
        row=json.loads(line)
        if not isinstance(row,dict) or set(row)!={'source_line','ts','session_id','counters'}:
            raise ValidationError('source projection contains undeclared fields')
        if (type(row['source_line']) is not int or not 0<row['source_line']<10**12
                or not isinstance(row['session_id'],str)
                or not re.fullmatch('[A-Za-z0-9._-]{1,128}',row['session_id'])):
            raise ValidationError('source observation identity is invalid')
        row['ts']=iso(source_time(row['ts']))
        if timestamp(row['ts'])>collected:
            raise ValidationError('source observation is later than its collection timestamp')
        counters=row['counters']
        if (not isinstance(counters,dict) or not counters
                or set(counters)-{'total_token_usage','last_token_usage'}):
            raise ValidationError('unregistered counter temporality')
        for counter_kind,values in counters.items():
            if not isinstance(values,dict) or set(values)-set(TOKEN_FIELDS):
                raise ValidationError('source counter contains undeclared fields')
            if any(v is not None and (type(v) is not int or not 0<=v<2**63) for v in values.values()):
                raise ValidationError('source counter quantity is invalid')
            for key in TOKEN_FIELDS:
                values.setdefault(key,None)
            if all(values[k] is not None for k in ('total_tokens','input_tokens','output_tokens')):
                if counter_kind=='total_token_usage' and values['total_tokens']!=values['input_tokens']+values['output_tokens']:
                    raise ValidationError('source total does not reconcile to input plus output')
            for part,whole in [('cached_input_tokens','input_tokens'),('reasoning_output_tokens','output_tokens')]:
                if values[part] is not None and values[whole] is not None and values[part]>values[whole]:
                    raise ValidationError('source subset quantity exceeds its parent category')
        key=(row['session_id'],row['source_line'])
        if key in rows and rows[key]!=row:
            raise ValidationError('conflicting source observation in sample')
        rows[key]=row
    if not rows or len(rows)>200000:
        raise ValidationError('empty or oversized usage sample')
    ordered=sorted(rows.values(),key=lambda r:(r['session_id'],r['source_line']))
    last={}
    for row in ordered:
        if row['session_id'] in last and row['ts']<last[row['session_id']]:
            raise ValidationError('source timestamps disagree with original line order')
        last[row['session_id']]=row['ts']
    return dict(schema_version='tokenledger-usage-packet/v1',source=SOURCE,
                client_version=meta['client_version'],account_alias=account,
                source_sha256=meta['source_sha256'],collected_at=iso(collected),observations=ordered)


def events_for(packet, evidence_hash):
    account=packet['account_alias']
    events=[]
    for row in packet['observations']:
        scope='codex:'+digest(dict(source=SOURCE,session=row['session_id']))[:32]
        for raw_kind,values in sorted(row['counters'].items()):
            temporality='cumulative' if raw_kind=='total_token_usage' else 'interval_total'
            for field,value in values.items():
                observation=f'{scope}:{row["source_line"]:012d}:{temporality}:{KINDS[field]}'
                features=dict(observation_id=observation,source=SOURCE,scope_id=scope,
                    model=None,token_kind=KINDS[field],quantity=value,temporality=temporality,
                    interval_start=row['ts'],interval_end=row['ts'],revision=0,supersedes=None,
                    evidence_sha256=evidence_hash)
                events.append(Activity('usage:'+digest(dict(account=account,observation=observation)),row['ts'],
                    'usage_observed',features,account,link='evidence:imports/'+evidence_hash+'/packet.json'))
    return events


def import_sample(application, directory, *, actor):
    application.validate()
    if not isinstance(actor,str) or not actor.strip() or actor!=actor.strip():
        raise ValidationError('an explicit actor is required')
    failure_id=uuid.uuid4().hex
    document=None
    try:
        document=packet(application,directory)
        content=(canonical(document)+'\n').encode()
        content_hash=hashlib.sha256(content).hexdigest()
        events=events_for(document,content_hash)
        with application.reader().connect() as conn:
            old=conn.execute("""SELECT activity_id,ts,customer,feature_json::VARCHAR,link
                FROM stream.activity WHERE activity='usage_observed' AND
                json_extract_string(feature_json,'$.source')=?""",[SOURCE]).fetchall()
        admitted={key:(iso(ts),account,json.loads(features),link) for key,ts,account,features,link in old}
        scope_accounts={f['scope_id']:account for _,account,f,_ in admitted.values()}
        ready=[]
        duplicates=0
        for event in events:
            if scope_accounts.get(event.feature_json['scope_id'],event.customer)!=event.customer:
                raise ValidationError('session already belongs to another account binding')
            if event.activity_id in admitted:
                ts,account,features,link=admitted[event.activity_id]
                comparable={k:v for k,v in features.items() if k!='evidence_sha256'}
                if ts!=iso(timestamp(event.ts)) or comparable!={k:v for k,v in event.feature_json.items() if k!='evidence_sha256'}:
                    raise ValidationError('source revision conflicts with admitted observation; quarantined without restating')
                # First admission owns provenance. A different operator retry must
                # not impersonate that actor or rewrite the original evidence.
                duplicates+=1
                continue
            ready.append(event)
        target=application.path('evidence/imports/'+content_hash+'/packet.json')
        target.parent.mkdir(parents=True,exist_ok=True)
        if target.exists():
            if target.read_bytes()!=content:
                raise ValidationError('retained source packet hash mismatch')
        else:
            with target.open('xb') as handle:
                handle.write(content)
        # The complete batch uses the common writer's transaction and identity checks.
        result=application.append(ready,source='codex-import-v1',actor=actor)
        result['duplicates']+=duplicates
        summary=dict(status='imported',packet_sha256=content_hash,**result,
            actor=actor,arrived_at=iso(datetime.now(UTC)),
            coverage='partial',scope='local session counters; account total and charges unknown')
        destination=application.path('evidence/import-runs/'+failure_id+'.json')
        destination.parent.mkdir(parents=True,exist_ok=True)
        destination.write_text(canonical(summary)+'\n',encoding='utf-8')
        return summary
    except (ValidationError,ValueError,KeyError,TypeError,OSError) as exc:
        destination=application.path('evidence/failures/'+failure_id+'.json')
        destination.parent.mkdir(parents=True,exist_ok=True)
        # Invalid source content/paths never become error or process-ledger text.
        reason=str(exc) if isinstance(exc,ValidationError) else 'invalid or unavailable bounded usage evidence'
        now=iso(datetime.now(UTC))
        failure=dict(status='quarantined',gate='usage-import/v1',reason=reason,
                     ts=now,candidate_promoted=False)
        # A fully validated but conflicting candidate is safe to retain privately.
        # Invalid arbitrary source content never enters this artifact.
        if document is not None:
            failure['candidate']=document
        data=(canonical(failure)+'\n').encode()
        destination.write_bytes(data)
        if document is not None:
            times=[timestamp(r['ts']) for r in document['observations']]
            evidence_hash=hashlib.sha256(data).hexdigest()
            event=Activity('usage-failure:'+failure_id,now,'usage_coverage',dict(source=SOURCE,
                scope_id='quarantine:'+failure_id,interval_start=iso(min(times)),
                interval_end=iso(max(times)+timedelta(microseconds=1)),coverage='partial',
                reason='Quarantined candidate: '+reason,
                next_action='Review the retained conflict; preserve the last admitted observations',
                evidence_sha256=evidence_hash),document['account_alias'],link='evidence:failures/'+failure_id+'.json')
            application.append([event],source='usage-failure-v1',actor=actor)
        raise ValidationError(reason+'; failure '+failure_id) from None


def verify_packets(application,snapshot):
    """Re-performance checks retained normalized evidence as well as stream hashes."""
    checked={}
    for row in snapshot.select(['activity_id','activity','feature_json','ts','customer','link']).to_pylist():
        if row['activity']=='usage_coverage' and (row['link'] or '').startswith('evidence:failures/'):
            features=json.loads(row['feature_json'])
            path=application.path('evidence/'+row['link'].removeprefix('evidence:'))
            if not path.is_file() or fingerprint(path)!=features['evidence_sha256']:
                raise ValidationError('retained failure evidence is missing or changed')
        if row['activity']!='usage_observed':
            continue
        features=json.loads(row['feature_json'])
        if features.get('source')!=SOURCE:
            continue
        key=features['evidence_sha256']
        if key not in checked:
            path=application.path('evidence/imports/'+key+'/packet.json')
            if not path.is_file() or fingerprint(path)!=key:
                raise ValidationError('retained usage evidence is missing or changed')
            document=json.loads(path.read_bytes())
            checked[key]={e.activity_id:e for e in events_for(document,key)}
        expected=checked[key].get(row['activity_id'])
        if (expected is None or expected.feature_json!=features or expected.customer!=row['customer']
                or expected.link!=row['link'] or iso(timestamp(expected.ts))!=iso(row['ts'])):
            raise ValidationError('usage activity differs from retained source evidence')
