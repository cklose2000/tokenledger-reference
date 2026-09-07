import copy
import json
from pathlib import Path

from click.testing import CliRunner
import pytest

from tl.challenge.engine import prepare, read, verify, write
from tl.challenge.grade import differences, grade, score
from tl.cli import cli
from tl.metrics.engine import replay_receipts
from tl.stream import ValidationError


@pytest.fixture(scope='module')
def challenge(tmp_path_factory):
    root=tmp_path_factory.mktemp('challenge')/'fixture'
    shared={p:Path(p).read_bytes() for p in ('ledger/process.jsonl','ledger/metrics.jsonl')}
    result=prepare(root)
    assert all(Path(p).read_bytes()==content for p,content in shared.items())
    return root,result


def test_native_challenge_proof_and_meaningful_floor(challenge,tmp_path):
    root,result=challenge
    assert verify(root)['status']=='verified'
    expected=read(root/'expected.json');nrr=expected['tasks']['nrr_definition']
    assert nrr['changed_lenses']==['base_t12m']
    assert len(nrr['unchanged_lenses'])==3
    assert nrr['original']['base_t12m']['cohort_customers']==4
    assert nrr['current']['base_t12m']['cohort_customers']==2
    close=expected['tasks']['failed_close']
    assert close['before_status']=='held' and close['after_status']=='ready'
    assert close['before']['expected_cents']-close['before']['actual_cents']==1500
    assert close['after']['expected_cents']==close['after']['actual_cents']
    assert close['retry_inserted']==0
    card=grade(root,root/'expected.json',tmp_path/'score')
    assert card['status']=='pass' and card['token_usage'] is None
    replayed=replay_receipts([result['receipt_id'],card['receipt_id'],card['timing']['receipt_id']],
                            output_root=root/'runs',ledger=root/'metrics.jsonl')
    assert len(replayed)==3 and all(r['verified'] for r in replayed)
    assert sum(bool(r['recomputed']) for r in replayed)==2
    assert 'USD/Mtok' in (tmp_path/'score/scorecard.md').read_text()


@pytest.mark.parametrize('change', ['wrong_cent','wrong_ratio','missing_task','null','pass_string','wrong_fixture'])
def test_grader_rejects_agent_claims_and_wrong_values(challenge,change):
    root,_=challenge;expected=read(root/'expected.json');answer=copy.deepcopy(expected)
    if change=='wrong_cent': answer['tasks']['july_yield']['values']['net_revenue_cents']+=1
    elif change=='wrong_ratio': answer['tasks']['nrr_definition']['original']['base_t12m']['nrr']='999'
    elif change=='missing_task': del answer['tasks']['late_invoice']
    elif change=='null': answer['tasks']['july_yield']['values']['tokens']=None
    elif change=='pass_string': answer['tasks']['failed_close']='PASS'
    else: answer['fixture_id']='another-fixture'
    assert score(expected,answer)['status']=='fail'


def test_ratio_tolerance_never_applies_to_cents_or_nulls():
    assert not differences('1.000000000000','1.0000009','.nrr')
    assert differences('1.000000000000','1.000002','.nrr')
    assert differences(None,'0','.nrr')
    assert differences(100,'100','.net_revenue_cents')
    assert differences(1,True,'.tokens')
    assert differences('1','NaN','.nrr')
    assert not differences('USD/Mtok','USD/Mtok','.units.net_of_discounts_per_mtok')
    assert differences('USD/Mtok','ratio','.units.net_of_discounts_per_mtok')


def test_score_is_deterministic_and_cli_failure_nonzero(challenge,tmp_path):
    root,_=challenge;expected=read(root/'expected.json')
    assert score(expected,expected)==score(expected,copy.deepcopy(expected))
    bad=copy.deepcopy(expected);bad['tasks']['late_invoice']['delta']['tokens']=1
    answer=tmp_path/'bad.json';write(answer,bad)
    result=CliRunner().invoke(cli,['challenge','grade',str(root),'--answer',str(answer),'--output',str(tmp_path/'score'),'--json'])
    assert result.exit_code==1,result.output
    assert json.loads(result.output)['status']=='fail'


def test_same_answer_can_be_regraded_without_duplicate_receipts(challenge,tmp_path):
    from tl.receipts.metrics import read_receipts
    root,_=challenge
    first=grade(root,root/'expected.json',tmp_path/'first')
    before=read_receipts(root/'metrics.jsonl')
    second=grade(root,root/'expected.json',tmp_path/'second')
    after=read_receipts(root/'metrics.jsonl')
    assert first['receipt_id']==second['receipt_id']
    assert all(after[key]==value for key,value in before.items())
    assert (tmp_path/'second/scorecard.md').is_file()
    assert all(r['verified'] for r in replay_receipts(
        [second['receipt_id'],second['timing']['receipt_id']],
        output_root=root/'runs',ledger=root/'metrics.jsonl'))


def test_grader_cannot_claim_fixture_code_for_a_different_implementation(challenge,tmp_path,monkeypatch):
    root,_=challenge;before=(root/'metrics.jsonl').read_bytes()
    import tl.challenge.grade as grading
    # Source replay is checked elsewhere. Isolate the scoring-code boundary.
    monkeypatch.setattr(grading,'verify',lambda root: None)
    monkeypatch.setattr('tl.compare.engine.artifacts',lambda: {})
    with pytest.raises(ValidationError,match='original executable'):
        grading.grade(root,root/'expected.json',tmp_path/'score')
    assert (root/'metrics.jsonl').read_bytes()==before


def test_failed_close_command_preserves_publication_and_recovery_is_idempotent(challenge):
    root,_=challenge;pointer=read(root/'challenge.json');manifest=read(root/'runs'/pointer['run_id']/'run.json')
    before=(root/'close/publication.json').read_bytes()
    result=CliRunner().invoke(cli,['challenge','close',str(root),'--watermark',str(manifest['failed_source']['failed']['watermark']),'--json'])
    assert result.exit_code==2,result.output
    assert json.loads(result.output)['publication']=='held'
    retry=CliRunner().invoke(cli,['challenge','close',str(root),'--recover','--json'])
    assert retry.exit_code==0,retry.output
    assert json.loads(retry.output)['ingestion']['inserted']==0
    assert (root/'close/publication.json').read_bytes()==before


@pytest.mark.parametrize('path',['expected.json','close/capture/source.jsonl','close/publication.json'])
def test_challenge_tamper_fails_closed(challenge,path):
    root,_=challenge;target=root/path;saved=target.read_bytes()
    try:
        if path.endswith('source.jsonl'): target.write_bytes(saved+b'{}\n')
        else:
            value=json.loads(saved);value['tamper']=True;write(target,value)
        with pytest.raises(ValidationError): verify(root)
    finally: target.write_bytes(saved)


def test_prepare_refuses_existing_evidence(challenge):
    root,_=challenge
    with pytest.raises(ValidationError,match='new or empty'):prepare(root)
