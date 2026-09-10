"""Bounded live admission rehearsal, never a human approval or learning outcome.

Every HTTP case is checked against a fresh authenticated BigQuery snapshot.
Only an explicitly disposable rehearsal deployment and test signer are allowed.
"""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timezone, timedelta
from pathlib import Path
import hashlib
import json
import subprocess
import uuid

import pyarrow.parquet as pq

from tl.bigquery.client import Client
from tl.bigquery.gateway_probe import identity_token, https_request
from tl.evolution import admission as a, diagnostic as d, evidence as e, transport
from tl.receipts.metrics import digest, code_revision, read_receipts
from tl.stream import ValidationError
from tl.stream.events import canonical, iso

MAX_REQUESTS = 64


def rehearsal_guard(config, test_key):
    key = Path(test_key).resolve()
    # Never accept the operational reviewer key or a live application. Rehearsal
    # trust is an explicit separate deployment, not an enrollment operation.
    if (key.name != 'rehearsal-test-signer' or 'rehearsal' not in config.binding.dataset
            or 'rehearsal' not in config.audience
            or any('rehearsal/' not in source for source in config.allowed_producers.values())
            or config.admission_policy != a.POLICY
            or config.binding.maximum_bytes_billed > 16 * 1024 * 1024):
        raise ValidationError('native rehearsal requires a disposable deployment and rehearsal-test-signer')
    public = ' '.join(key.with_name(key.name+'.pub').read_text().split()[:2])
    if config.learning_signers != 'chandler ' + public + '\n':
        raise ValidationError('disposable test signer differs from rehearsal trust')
    return key


def run(config, test_key, evaluation_id, pr, *, output_root, ledger):
    key = rehearsal_guard(config, test_key)
    replay = e.replay([evaluation_id], output_root=output_root, ledger=ledger)[0]
    evaluation = read_receipts(ledger)[evaluation_id]
    if evaluation['schema_version'] != e.SCHEMA or replay['result']['status'] != 'passed':
        raise ValidationError('a passed native diagnostic evaluation is required')
    if pr.get('private') is not True or not pr.get('pr_head') or not pr.get('changed_files'):
        raise ValidationError('retain the private PR inspection before rehearsal')
    pins=e.artifacts(); revision=code_revision(pins)
    if not revision['execution_artifacts_match_git']:
        raise ValidationError('freeze native rehearsal execution in Git')
    root=Path(output_root)/uuid.uuid4().hex; root.mkdir(parents=True)
    e.write(root/'scope.json',dict(status='engineering_rehearsal_only',gateway_identity=config.identity,
        max_http_requests=MAX_REQUESTS,actual_reviewer_approval=False,learning_outcome=False,
        signer_public_sha256=hashlib.sha256(config.learning_signers.encode()).hexdigest()))
    e.write(root/'pr.json',pr)
    roles={name:next(actor for actor,kinds in config.allowed_activities.items() if activity in kinds)
           for name,activity in dict(proposer='finding_submitted',verifier='finding_verified',
                                    reviewer='trial_authorized',runner='trial_started').items()}
    client=Client(config.binding); count=0; checks=[]; prefix={}

    def snapshot():
        observed=transport.snapshot(config,output_root=output_root,ledger=ledger,client=client)
        rows=pq.read_table(Path(observed['run_directory'])/'source.parquet').to_pylist()
        return {row['activity_id']:row for row in rows},observed

    def send(event, role, token_kind='valid'):
        nonlocal count
        count+=1
        if count>MAX_REQUESTS: raise ValidationError('rehearsal HTTP request ceiling exhausted')
        token=(None if token_kind=='none' else 'invalid-test-token' if token_kind=='forged'
               else identity_token(roles[role],config.audience if token_kind=='valid' else 'https://wrong-audience.invalid'))
        status,raw=https_request(config.audience,canonical(dict(events=[event])).encode(),token)
        # Do not persist tokens even if an unexpected service echoes a request.
        if token and token.encode() in raw: raw=raw.replace(token.encode(),b'[REDACTED]')
        return dict(http_status=status,response_text=raw.decode('utf-8',errors='replace'))

    def check(name, requests, accepted=(), *, concurrent=False, statuses=None, identity_denial=False):
        nonlocal prefix
        e.write(root/(name+'-requests.json'),[dict(event=v,role=r,token_kind=t) for v,r,t in requests])
        if concurrent:
            with ThreadPoolExecutor(max_workers=2) as pool:
                responses=list(pool.map(lambda args:send(*args),requests))
        else: responses=[send(*args) for args in requests]
        actual,observed=snapshot()
        unchanged=all(actual.get(k)==v for k,v in prefix.items())
        added=set(actual)-set(prefix)
        matched=(unchanged and added==set(accepted) and
                 (statuses is None or sorted(r['http_status'] for r in responses)==sorted(statuses)) and
                 (not identity_denial or len(responses)==1 and responses[0]['http_status'] in (401,403)))
        item=dict(case=name,passed=matched,responses=responses,new_activity_ids=sorted(added),
                  prior_rows_unchanged=unchanged,snapshot_receipt_id=observed['receipt_id'])
        e.write(root/(name+'-result.json'),item); checks.append(item); prefix=actual
        if not matched: raise ValidationError('native admission rehearsal failed: '+name)
        return responses

    suffix=uuid.uuid4().hex[:12]
    case='rehearsal-'+suffix
    def event(kind,body,tag,case_id=case):
        return transport.event(kind,case_id,body,suffix+'-'+tag)
    def one(name,v,role,accepted=True,status=200):
        return check(name,[(v,role,'valid')],[v['activity_id']] if accepted else [],statuses=[status])
    def finding(tag,case_id=case):
        return event('finding_submitted',dict(report_receipt_id=evaluation['result']['report_receipt_id'],
            requested_date='2026-07-01',description='Engineering rehearsal of retained NRR date selection.',
            evidence_sha256=digest(evaluation)),tag,case_id)
    def chain(case_id,tag,verdict='confirmed'):
        f=finding(tag+'-finding',case_id); one(tag+'-finding',f,'proposer')
        v=event('finding_verified',dict(finding_id=f['activity_id'],verdict=verdict,
            evidence_receipt_id=evaluation_id,rationale='Native evaluation and independent scalar oracle retained.'),tag+'-verified',case_id)
        one(tag+'-verified',v,'verifier')
        proposal=dict(verification_id=v['activity_id'],candidate=a.CANDIDATE,
            candidate_sha256=evaluation['candidate_sha256'],execution_sha256=evaluation['execution_hash'],
            cases_sha256=evaluation['cases_sha256'],pr_url=pr['pr_url'],pr_head=pr['pr_head'],changed_files=pr['changed_files'])
        p=event('candidate_proposed',proposal,tag+'-proposal',case_id)
        return f,v,p
    def evaluated(case_id,tag,p):
        one(tag+'-proposal',p,'proposer')
        v=event('candidate_evaluated',dict(proposal_id=p['activity_id'],verdict='passed',
            evaluation_receipt_id=evaluation_id,candidate_sha256=evaluation['candidate_sha256'],
            cases_sha256=evaluation['cases_sha256'],target='bigquery',
            evaluation_binding_identity=evaluation['binding_identity']),tag+'-evaluation',case_id)
        one(tag+'-evaluation',v,'verifier')
        now=datetime.now(timezone.utc)
        return dict(schema_version=a.PLAN,case_id=case_id,gateway_identity=config.identity,
            proposal_id=p['activity_id'],evaluation_id=v['activity_id'],candidate=a.CANDIDATE,
            candidate_sha256=evaluation['candidate_sha256'],execution_sha256=evaluation['execution_hash'],
            cases_sha256=evaluation['cases_sha256'],pr_head=pr['pr_head'],prepared_watermark=len(prefix),
            nonce=uuid.uuid4().hex,created_at=iso(now),expires_at=iso(now+timedelta(hours=2)),
            reviewer_principal=a.PRINCIPAL,signature_namespace=a.NAMESPACE,
            trust_sha256=hashlib.sha256(config.learning_signers.encode()).hexdigest(),
            limits=dict(runs=1,diagnostic_only=True,source_writes=False,policy_changes=False,
                        reporting_gate_changes=False,automatic_followup=False,ongoing_activation=False))
    def approval(plan,tag):
        path=root/(tag+'-test-plan.json'); e.write(path,plan)
        subprocess.run(['ssh-keygen','-Y','sign','-f',str(key),'-n',a.NAMESPACE,str(path)],
                       capture_output=True,check=True)
        return event('trial_authorized',dict(plan_json=path.read_text(),
            signature=path.with_name(path.name+'.sig').read_text()),tag,plan['case_id'])
    def inspect(tag,case_id=case):
        return event('inspection_requested',dict(report_receipt_id=evaluation['result']['report_receipt_id'],
                     population='nrr',requested_date='2026-07-31'),tag,case_id)
    def claim(auth,inspection,tag):
        return event('trial_started',dict(authorization_id=auth['activity_id'],inspection_id=inspection['activity_id'],
            candidate_sha256=evaluation['candidate_sha256'],execution_sha256=evaluation['execution_hash']),
            tag,auth['feature_json']['case_id'])

    try:
        prefix,initial=snapshot(); e.write(root/'initial.json',initial)
        f=finding('finding')
        for label,kind in [('no-identity','none'),('forged-identity','forged'),('wrong-audience','wrong')]:
            check(label,[(f,'proposer',kind)],identity_denial=True)
        forged=deepcopy(f); forged['_actor']=roles['reviewer']
        one('forged-provenance',forged,'proposer',False,422)
        invalid=deepcopy(f); invalid['feature_json']['record_sha256']='0'*64
        one('bad-envelope',invalid,'proposer',False,422)
        v=event('finding_verified',dict(finding_id=f['activity_id'],verdict='confirmed',
            evidence_receipt_id=evaluation_id,rationale='Rehearsal assertion.'),'verified')
        one('role-escalation',v,'proposer',False,403)
        one('future-reference',v,'verifier',False,422)
        one('finding',f,'proposer'); one('identical-retry',f,'proposer',False,200)
        conflict=deepcopy(f); body=a.strict_json(conflict['feature_json']['record_json']); body['description']='Conflicting retry.'
        conflict['feature_json']=a.envelope(case,body)
        one('conflicting-retry',conflict,'proposer',False,422)
        one('verification',v,'verifier')
        proposal=dict(verification_id=v['activity_id'],candidate=a.CANDIDATE,
            candidate_sha256=evaluation['candidate_sha256'],execution_sha256=evaluation['execution_hash'],
            cases_sha256=evaluation['cases_sha256'],pr_url=pr['pr_url'],pr_head=pr['pr_head'],changed_files=pr['changed_files'])
        bad=event('candidate_proposed',{**proposal,'changed_files':['definitions/metrics/consumption_nrr.yaml']},'bad-scope')
        one('policy-change-rejected',bad,'proposer',False,422)
        p=event('candidate_proposed',proposal,'proposal'); plan=evaluated(case,'main',p)
        wrong=deepcopy(plan);wrong['candidate_sha256']='0'*64
        one('signed-wrong-candidate',approval(wrong,'wrong-candidate'),'reviewer',False,422)
        expanded=deepcopy(plan);expanded['limits']['reporting_gate_changes']=True
        one('signed-expanded-authority',approval(expanded,'expanded'),'reviewer',False,422)
        expired=deepcopy(plan);expired['created_at']=iso(datetime.now(timezone.utc)-timedelta(days=2));expired['expires_at']=iso(datetime.now(timezone.utc)-timedelta(days=1))
        one('expired-approval',approval(expired,'expired'),'reviewer',False,422)
        auth=approval(plan,'approval')
        tampered=deepcopy(auth); b=a.strict_json(tampered['feature_json']['record_json']);b['plan_json']=b['plan_json'].replace(plan['nonce'],'0'*32)
        tampered['feature_json']=a.envelope(case,b)
        one('tampered-signed-bytes',tampered,'reviewer',False,422)
        one('valid-test-approval',auth,'reviewer')
        i=inspect('inspection');j=inspect('later-inspection')
        one('first-inspection',i,'proposer');one('later-inspection',j,'proposer')
        one('cannot-skip-first',claim(auth,j,'skip'),'runner',False,422)
        claims=[claim(auth,i,'claim-a'),claim(auth,i,'claim-b')]
        e.write(root/'concurrent-claims-requests.json',claims)
        with ThreadPoolExecutor(max_workers=2) as pool:
            responses=list(pool.map(lambda v:send(v,'runner'),claims))
        actual,observed=snapshot(); added=set(actual)-set(prefix)
        passed=(all(actual.get(k)==v for k,v in prefix.items()) and len(added)==1
                and added <= {v['activity_id'] for v in claims}
                and sorted(r['http_status'] for r in responses)==[200,422])
        item=dict(case='concurrent-claims',passed=passed,responses=responses,
                  new_activity_ids=sorted(added),snapshot_receipt_id=observed['receipt_id'])
        e.write(root/'concurrent-claims-result.json',item);checks.append(item);prefix=actual
        if not passed: raise ValidationError('native simultaneous claims did not elect exactly one runner')
        winner=next(iter(added))
        one('exhausted-claim',claim(auth,i,'claim-again'),'runner',False,422)
        # This completion is a test of admission, not a diagnostic measurement.
        completion=event('trial_completed',dict(trial_id=winner,outcome_receipt_id=evaluation_id,
            status='inconclusive',ongoing_activation=False),'completion')
        one('fixture-completion',completion,'runner');one('completion-retry',completion,'runner',False,200)
        unknown=deepcopy(completion);unknown['activity']='candidate_promoted'
        one('no-promotion-capability',unknown,'runner',False,403)
        other=case+'-revoked';_,_,p2=chain(other,'revoked');plan2=evaluated(other,'revoked',p2)
        auth2=approval(plan2,'revoked-approval');one('second-test-approval',auth2,'reviewer')
        i2=inspect('revoked-inspection',other);one('second-inspection',i2,'proposer')
        revoked=event('trial_revoked',dict(authorization_id=auth2['activity_id'],reason='Rehearsal halt.'),'revoke',other)
        one('revocation',revoked,'reviewer');one('revoked-run',claim(auth2,i2,'revoked-claim'),'runner',False,422)
        _,_,p3=chain(case+'-refuted','refuted','refuted')
        one('refuted-finding-blocks-candidate',p3,'proposer',False,422)
        result=dict(status='passed',checks=len(checks),http_requests=count,actual_reviewer_approval=False,
                    learning_outcome=False,ongoing_activation=False,interpretation='Disposable authenticated admission rehearsal only.')
    except Exception as exc:
        result=dict(status='failed',error_type=type(exc).__name__,error=str(exc),checks=len(checks),
                    http_requests=count,actual_reviewer_approval=False,learning_outcome=False,ongoing_activation=False)
    e.write(root/'checks.json',checks);e.write(root/'jobs.json',client.jobs)
    record=e.seal(root,dict(schema_version=e.OBSERVATION,run_id=root.name,result=result,
        definition_version='learning-admission-rehearsal/v1',inputs=dict(evaluation_receipt_id=evaluation_id),
        query_hash=digest(dict(gateway=config.identity,max_requests=MAX_REQUESTS)),
        git_sha=revision['git_sha'],execution_hash=digest(pins),binding_identity=config.identity),ledger)
    return dict(status=result['status'],receipt_id=record['receipt_id'],run_directory=str(root.resolve()),result=result)
