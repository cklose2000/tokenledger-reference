"""Frozen private evaluations using the shared receipt and historical replay path."""
from pathlib import Path
from time import perf_counter
from datetime import datetime,timezone
import hashlib,json,re,uuid

from tl.learning.candidate import diagnose,VERSION,SPEC
from tl.metrics.engine import runtime
from tl.receipts.metrics import (append_receipts,code_revision,digest,input_manifest,
    read_receipts,receipt_id)
from tl.reporting.application import private_path
from tl.stream import ValidationError
from tl.stream.events import canonical
from tl.usage.importer import events_for

SCHEMA='tokenledger-learning/v1'
FILES={'packet.json','failure.json','prior-run.json','expected.json','partitions.json'}


def freeze(app,*,packet_hash,failure_id,prior_run_id,development_ordinals=()):
    """Freeze independent scalar expectations before candidate evaluation."""
    app.validate()
    if not re.fullmatch('[0-9a-f]{64}',packet_hash) or any(not re.fullmatch('[0-9a-f]{32}',v) for v in (failure_id,prior_run_id)):
        raise ValidationError('invalid retained learning evidence identity')
    source=app.path('evidence/imports/'+packet_hash+'/packet.json')
    failure=app.path('evidence/failures/'+failure_id+'.json')
    prior=app.outputs/prior_run_id/'run.json'
    if file_hash(source)!=packet_hash or not failure.is_file() or not prior.is_file():
        raise ValidationError('learning requires retained source, failure and prior report')
    packet=json.loads(source.read_bytes());expected=[];partitions={}
    for row in packet['observations']:
        key=f'{row["session_id"]}:{row["source_line"]}'
        partitions[key]='development' if row['source_line'] in development_ordinals or int(hashlib.sha256(key.encode()).hexdigest(),16)%3 else 'held_out'
        vals=[row['counters'].get('last_token_usage',{}).get(k) for k in ('total_tokens','input_tokens','output_tokens')]
        if None not in vals and vals[0]!=sum(vals[1:]):
            c=[row['counters'].get('total_token_usage',{}).get(k) for k in ('total_tokens','input_tokens','output_tokens')]
            expected.append(dict(session_id=row['session_id'],source_line=row['source_line'],
                reported_total=vals[0],expected_total=sum(vals[1:]),residual=vals[0]-sum(vals[1:]),
                cumulative_status='incomplete' if None in c else 'reconciled' if c[0]==sum(c[1:]) else 'mismatch'))
    root=app.path('evidence/learning/'+uuid.uuid4().hex);root.mkdir(parents=True)
    for name,path in [('packet.json',source),('failure.json',failure),('prior-run.json',prior)]:
        (root/name).write_bytes(path.read_bytes())
    for name,data in [('expected.json',expected),('partitions.json',partitions)]:
        (root/name).write_text(canonical(data)+'\n',encoding='utf-8')
    manifest=dict(schema_version='tokenledger-learning-study/v1',binding_id=app.binding.identity,
        files={p.name:file_hash(p) for p in root.iterdir()},prior_run_id=prior_run_id,
        frozen_at=datetime.now(timezone.utc).isoformat(),candidate_sha256=candidate_identity(),
        oracle='Independent scalar arithmetic; candidate uses DuckDB SQL',
        partition_policy='SHA-256(session:ordinal) modulo 3 with explicit discovery overrides',
        development_ordinals=list(development_ordinals),promotion='unavailable')
    (root/'study.json').write_text(canonical(manifest)+'\n',encoding='utf-8')
    return dict(status='frozen_not_evaluated',study_directory=str(root),study_sha256=file_hash(root/'study.json'))


def file_hash(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def candidate_identity():
    return digest({p.as_posix():hashlib.sha256(p.read_bytes().replace(b'\r\n',b'\n')).hexdigest()
                   for p in (Path('tl/learning/candidate.py'),SPEC)})


def artifacts(app):
    return {**app.artifacts([]),SPEC.as_posix():hashlib.sha256(SPEC.read_bytes().replace(b'\r\n',b'\n')).hexdigest()}


def study(app,directory,expected_hash=None):
    app.validate();directory=private_path(directory)
    if not directory.is_relative_to(app.path('evidence/learning')):
        raise ValidationError('learning evidence must belong to the bound private application')
    if expected_hash and file_hash(directory/'study.json')!=expected_hash:
        raise ValidationError('frozen learning study changed')
    manifest=json.loads((directory/'study.json').read_bytes())
    if (manifest.get('schema_version')!='tokenledger-learning-study/v1'
        or manifest.get('binding_id')!=app.binding.identity or set(manifest.get('files',{}))!=FILES):
        raise ValidationError('unsupported or mismatched frozen learning study')
    for name,expected in manifest['files'].items():
        path=private_path(directory/name)
        if not path.is_relative_to(directory) or file_hash(path)!=expected:
            raise ValidationError('frozen learning evidence changed')
    return directory,manifest


def compute(app,directory,*,expected_inputs=None):
    directory,frozen=study(app,directory)
    packet=json.loads((directory/'packet.json').read_bytes())
    prior=json.loads((directory/'prior-run.json').read_bytes())
    retained=app.path('evidence/imports/'+file_hash(directory/'packet.json')+'/packet.json')
    if not retained.is_file() or retained.read_bytes()!=(directory/'packet.json').read_bytes():
        raise ValidationError('learning input is not an unchanged admitted packet')
    from tl.metrics.engine import read_run
    original=read_run(frozen['prior_run_id'],output_root=app.outputs,ledger=app.ledger,application=app)
    if original['manifest']!=prior:
        raise ValidationError('frozen prior report differs from its receipt-backed original')
    if prior.get('application')!=app.name or prior.get('binding_id')!=app.binding.identity:
        raise ValidationError('prior report does not belong to the application')
    with app.session(asof=prior['asof'],known_at=prior['known_at'],watermark=prior['watermark'],
                     **prior['session_options']) as session:
        session.verify();inputs=input_manifest(session.snapshot)
        if expected_inputs is not None and inputs!=expected_inputs:
            raise ValidationError('learning source snapshot differs from receipt')
        actual={r['activity_id']:r for r in session.snapshot.select(
            ['activity_id','feature_json','customer','ts']).to_pylist()}
        for event in events_for(packet,file_hash(directory/'packet.json')):
            row=actual.get(event.activity_id)
            # U1 preserves first-admission evidence on overlapping imports.
            # session.verify() independently checks that original evidence.
            semantic=lambda f:{k:v for k,v in f.items() if k!='evidence_sha256'}
            if row is None or row['customer']!=event.customer or semantic(json.loads(row['feature_json']))!=semantic(event.feature_json):
                raise ValidationError('learning packet differs from admitted frozen observations')
    expected=json.loads((directory/'expected.json').read_bytes())
    partitions=json.loads((directory/'partitions.json').read_bytes())
    keys={(r['session_id'],r['source_line']) for r in packet['observations']}
    if set(partitions)!={f'{s}:{n}' for s,n in keys} or set(partitions.values())-{'development','held_out'}:
        raise ValidationError('frozen evaluation partitions differ from observations')
    findings=diagnose(packet)
    truth={(r['session_id'],r['source_line']):r for r in expected}
    located={(r['session_id'],r['source_line']):r for r in findings}
    if len(truth)!=len(expected) or len(located)!=len(findings) or not set(truth)<=keys:
        raise ValidationError('evaluation finding keys are duplicated or outside source evidence')
    scores={}
    for label in ('development','held_out'):
        population={k for k in keys if partitions[f'{k[0]}:{k[1]}']==label}
        wanted=set(truth)&population;found=set(located)&population
        incorrect=sum(any(located[k].get(field)!=value for field,value in truth[k].items()) for k in wanted&found)
        scores[label]=dict(observations=len(population),expected_findings=len(wanted),
            located_findings=len(found),false_positives=len(found-wanted),missed_findings=len(wanted-found),
            incorrect_residuals_or_context=incorrect)
    passed=all(not s[k] for s in scores.values() for k in ('false_positives','missed_findings','incorrect_residuals_or_context'))
    return dict(status='passed' if passed else 'failed',candidate=VERSION,candidate_sha256=candidate_identity(),
        scores=scores,findings=findings,baseline=dict(method='U1 session-level gap; no observation locator',
            observation_locators=0,unlocated_findings=len(truth),
            retained_session_flags=sum(r['metric']=='usage_gaps' and r['dimensions'].get('gap')=='last_counter_reconciliation'
                                      and r['dimensions'].get('account')==packet['account_alias'] for r in original['rows']),
            scope='Prior report window; candidate assesses the complete admitted packet, not account-wide usage'),
        authority='diagnostic only; counters, ingestion, coverage and definitions unchanged',
        manual_minutes=None,next_actual_run='not_run',promotion='unavailable_pending_human_approval'),inputs,prior


def evaluate(app,directory):
    directory,_=study(app,directory);pins=artifacts(app);revision=code_revision(pins)
    frozen_hash=file_hash(directory/'study.json')
    started=perf_counter();result,inputs,prior=compute(app,directory)
    study(app,directory,frozen_hash)
    if artifacts(app)!=pins:
        raise ValidationError('learning implementation changed during evaluation')
    elapsed=perf_counter()-started;run_id=uuid.uuid4().hex;root=app.outputs/run_id;root.mkdir()
    manifest=dict(schema_version=SCHEMA,run_id=run_id,application=app.name,binding_id=app.binding.identity,
        study_directory=str(directory),study_sha256=frozen_hash,
        candidate_sha256=candidate_identity(),execution_artifacts=pins,execution_hash=digest(pins),
        runtime=runtime(),asof=prior['asof'],known_at=prior['known_at'],watermark=prior['watermark'],
        inputs=inputs,definition_version=VERSION,query_hash=candidate_identity(),**revision)
    (root/'run.json').write_text(canonical(manifest)+'\n',encoding='utf-8')
    record=dict(schema_version=SCHEMA,run_id=run_id,result=result,inputs=inputs,
        definition_version=VERSION,query_hash=manifest['query_hash'],git_sha=revision['git_sha'],
        execution_hash=manifest['execution_hash'],asof=manifest['asof'],known_at=manifest['known_at'],
        watermark=manifest['watermark'],manifest_sha256=file_hash(root/'run.json'))
    record['receipt_id']=receipt_id(record)
    # Timings are observations, not a promise that elapsed time can reproduce.
    observation=dict(schema_version='tokenledger-learning-observation/v1',run_id=run_id,
        result=dict(evaluation_receipt_id=record['receipt_id'],seconds=elapsed),
        inputs=inputs,definition_version=VERSION,query_hash=record['query_hash'],
        git_sha=record['git_sha'],execution_hash=record['execution_hash'],
        asof=record['asof'],known_at=record['known_at'],watermark=record['watermark'],
        manifest_sha256=record['manifest_sha256'])
    observation['receipt_id']=receipt_id(observation)
    (root/'evaluation.json').write_text(canonical(dict(receipt_id=record['receipt_id'],**result))+'\n',encoding='utf-8')
    append_receipts(app.ledger,[record,observation])
    return dict(status='evaluated_'+result['status'],candidate_sha256=candidate_identity(),
        evaluation_receipt_id=record['receipt_id'],evaluation_sha256=file_hash(root/'evaluation.json'),
        evaluation_directory=str(root),timing_receipt_id=observation['receipt_id'],
        promotion='unavailable_pending_human_approval')


def replay(ids,*,application,output_root,ledger,python=None):
    if application is None:
        raise ValidationError('learning receipt requires its bound private application')
    app=application;records=read_receipts(ledger);answers=[]
    for key in ids:
        record=records[key];root=Path(output_root)/record['run_id']
        if file_hash(root/'run.json')!=record['manifest_sha256']:
            raise ValidationError('learning manifest changed')
        manifest=json.loads((root/'run.json').read_bytes())
        if manifest['application']!=app.name or manifest['binding_id']!=app.binding.identity:
            raise ValidationError('learning receipt application differs')
        for field in ('run_id','inputs','asof','known_at','watermark','query_hash','git_sha','execution_hash','definition_version'):
            if record[field]!=manifest[field]: raise ValidationError('learning receipt contract differs')
        study(app,manifest['study_directory'],manifest['study_sha256'])
        if python or manifest['execution_artifacts']!=artifacts(app) or manifest['runtime']!=runtime():
            from tl.receipts.archive import historical_replay
            answers.extend(historical_replay([key],manifest=manifest,db=app.db,output_root=output_root,
                ledger=ledger,python=python,application_config=app.config));continue
        if manifest['candidate_sha256']!=candidate_identity():
            raise ValidationError('learning candidate changed')
        observed=record['schema_version']=='tokenledger-learning-observation/v1'
        if not observed:
            if json.loads((root/'evaluation.json').read_bytes())!=dict(receipt_id=key,**record['result']):
                raise ValidationError('saved learning evaluation changed')
            actual,_,_=compute(app,manifest['study_directory'],expected_inputs=manifest['inputs'])
            if actual!=record['result']: raise ValidationError('learning evaluation differs from receipt')
        answers.append(dict(receipt_id=key,verified=True,recomputed=not observed,result=record['result']))
    return answers
