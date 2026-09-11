"""The synthetic walkthrough exercises the real loop and cannot masquerade as authority.

These tests run without credentials or cloud calls. A passing walkthrough is a
rehearsal observation; it establishes no trial result, approval or promotion.
"""
from dataclasses import replace
import json
import shutil

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from tl.bigquery.gateway import GatewayConfig, serve
from tl.cli import cli
from tl.evolution import walkthrough as w
from tl.evolution.admission import validate
from tl.stream import ValidationError


@pytest.fixture(scope='module')
def rehearsal(tmp_path_factory):
    root = tmp_path_factory.mktemp('walkthrough') / 'run'
    return w.run(root), root


def test_walkthrough_records_the_complete_loop_and_labels_it_synthetic(rehearsal):
    result, root = rehearsal
    assert result['status'] == 'rehearsed' and result['synthetic'] is True and result['native_execution'] is False
    assert [e['activity'] for e in result['events']] == [
        'finding_submitted', 'finding_verified', 'finding_submitted', 'finding_verified', 'candidate_proposed',
        'candidate_evaluated', 'trial_authorized', 'inspection_requested', 'inspection_requested', 'trial_started',
        'trial_completed']
    roles = {e['activity']: e['role'] for e in result['events']}
    assert roles['finding_submitted'] == 'proposer' and roles['finding_verified'] == 'verifier'
    assert roles['trial_authorized'] == 'reviewer' and roles['trial_completed'] == 'runner'
    assert result['baseline_inspection'] == dict(requested_date='2026-07-01', report_date='2026-07-31',
                                                 report_rows=4, selected_rows=0, explanation='none')
    assert result['evaluation']['cases'] == 11 and result['evaluation']['target'] == 'duckdb'
    assert result['outcome']['finding']['status'] == 'date_grain_mismatch'
    assert result['outcome']['finding']['suggested_date'] == '2026-07-31'
    assert result['outcome_status'] == 'improved' and 'Not a measured operating gain' in result['outcome']['interpretation']
    assert result['outcome']['actual_native_trial']['status'] == 'inconclusive'
    assert result['ongoing_activation'] is False and result['promotion'] is False
    assert 'Not Chandler' in result['approval_authority']
    assert result['business_separation']['learning_rows_in_business_stream'] == 0
    assert result['original_report_reproduced'] is True
    text = (root / 'README.md').read_text(encoding='utf-8')
    assert 'synthetic rehearsal' in text and 'inconclusive' in text and 'replay-walkthrough' in text
    assert 'SYNTHETIC-WALKTHROUGH-SIGNER' in (root / 'signer/synthetic-signer.pub').read_text()


def test_every_specified_refusal_happened_without_a_write(rehearsal):
    result, _ = rehearsal
    labels = {r['label'] for r in result['refusals']}
    assert labels >= {'finding author verifying its own finding', 'verification with a tampered content hash',
                      'candidate proposed from a refuted finding',
                      'evaluation claiming native BigQuery execution on the synthetic gateway',
                      'signed plan that expands authority to ongoing activation', 'signed bytes altered after signing',
                      'proposer submitting the approval', 'claiming a later, more favorable inspection',
                      'second trial under the consumed authority', 'revoking authority after completion',
                      'unregistered promotion activity'}
    assert all(r['refused'] and r['stream_rows_before'] == r['stream_rows_after'] for r in result['refusals'])
    refuted = [e for e in result['events'] if e['case_id'] == w.REFUTED_CASE]
    assert [e['activity'] for e in refuted] == ['finding_submitted', 'finding_verified']


def test_replay_rebuilds_state_reverifies_signature_and_reproduces_report(rehearsal):
    result, root = rehearsal
    replayed = w.replay(root)
    assert replayed['status'] == 'verified' and replayed['signature_reverified'] and replayed['state_rebuilt']
    kinds = {r['receipt_id']: r.get('recomputed') for r in replayed['results']}
    assert kinds[result['evaluation_receipt_id']] == 'diagnostic_from_retained_population'
    assert kinds[result['report_receipt_id']] == 'spine_population'
    assert kinds[result['outcome_receipt_id']] is False
    runner = CliRunner()
    for receipt in (result['evaluation_receipt_id'], result['outcome_receipt_id'], result['walkthrough_receipt_id']):
        answer = runner.invoke(cli, ['--artifact-root', str(root), 'receipt', receipt, '--json'])
        assert answer.exit_code == 0 and json.loads(answer.output)['verified'] is True
    answer = runner.invoke(cli, ['learning', 'replay-walkthrough', str(root), '--json'])
    assert answer.exit_code == 0 and json.loads(answer.output)['status'] == 'verified'


@pytest.mark.parametrize('tamper', ['plan', 'stream', 'oracle', 'ledger_outcome'])
def test_any_changed_byte_fails_replay(rehearsal, tmp_path, tamper):
    result, root = rehearsal
    copy = tmp_path / 'copy'
    shutil.copytree(root, copy)
    if tamper == 'plan':
        path = copy / 'plan.json'
        path.write_text(path.read_text(encoding='utf-8').replace('"runs":1', '"runs":2'), encoding='utf-8')
    elif tamper == 'stream':
        path = copy / 'learning-stream.parquet'
        table = pq.read_table(path); rows = table.to_pylist()
        envelope = json.loads(rows[-1]['feature_json']); record = json.loads(envelope['record_json'])
        record['status'] = 'inconclusive'
        envelope['record_json'] = json.dumps(record, sort_keys=True, separators=(',', ':'))
        rows[-1]['feature_json'] = json.dumps(envelope, sort_keys=True, separators=(',', ':'))
        pq.write_table(pa.Table.from_pylist(rows).select(table.column_names), path)
    elif tamper == 'oracle':
        run = next(p for p in (copy / 'runs').iterdir() if (p / 'oracle.json').exists())
        path = run / 'oracle.json'
        path.write_text(path.read_text(encoding='utf-8').replace('date_grain_mismatch', 'no_matching_date'), encoding='utf-8')
    else:
        path = copy / 'metrics.jsonl'
        path.write_text(path.read_text(encoding='utf-8').replace('"status":"improved"', '"status":"inconclusive"'), encoding='utf-8')
    with pytest.raises(ValidationError):
        w.replay(copy)


def test_walkthrough_never_overwrites_and_reports_inconclusive_when_no_opportunity(tmp_path):
    result = w.run(tmp_path / 'second', next_inspection_date='2026-07-31')
    assert result['outcome_status'] == 'inconclusive' and result['outcome']['finding']['status'] == 'selected'
    assert result['outcome']['finding']['suggested_date'] is None
    with pytest.raises(ValidationError, match='never overwritten'):
        w.run(tmp_path / 'second')
    with pytest.raises(ValidationError, match='ISO date'):
        w.run(tmp_path / 'third', next_inspection_date='July 1')


def test_synthetic_configuration_is_confined(rehearsal, tmp_path):
    _, root = rehearsal
    config = GatewayConfig.read(root / 'gateway.json')
    assert config.synthetic_rehearsal is True and 'synthetic_rehearsal' in config.as_dict()
    with pytest.raises(ValidationError, match='never served'):
        serve(config)
    answer = CliRunner().invoke(cli, ['learning', '--gateway-config', str(root / 'gateway.json'), 'view-sql', str(tmp_path / 'view.sql')])
    assert answer.exit_code != 0 and 'confined to tl learning walkthrough' in str(answer.exception)
    assert not (tmp_path / 'view.sql').exists()
    # A real-looking project, producer or origin cannot carry the synthetic flag.
    with pytest.raises(ValidationError, match='synthetic rehearsal requires'):
        replace(config, binding=replace(config.binding, project='tokenledger-fixture',
                                        query_principal='fixture-reader@tokenledger-fixture.iam.gserviceaccount.com',
                                        writer_principal='fixture-writer@tokenledger-fixture.iam.gserviceaccount.com'),
                write_stream=config.write_stream.replace('tokenledger-walkthrough', 'tokenledger-fixture'))
    with pytest.raises(ValidationError, match='synthetic rehearsal requires'):
        replace(config, audience='https://append-fixture.run.app')
    with pytest.raises(ValidationError, match='synthetic rehearsal requires'):
        replace(config, synthetic_rehearsal=False)
    # Without the flag the identical configuration keeps its original semantics and a different identity.
    plain = replace(config, synthetic_rehearsal=None)
    assert 'synthetic_rehearsal' not in plain.as_dict() and plain.identity != config.identity


def test_synthetic_gateway_records_duckdb_only_and_real_gateways_still_require_bigquery(rehearsal):
    _, root = rehearsal
    config = GatewayConfig.read(root / 'gateway.json')
    rows = pq.read_table(root / 'learning-stream.parquet').to_pylist()
    state = validate(rows, [], config)
    evaluation = next(value for _, row, value in state.values() if row['activity'] == 'candidate_evaluated')
    assert evaluation['target'] == 'duckdb'
    # The same retained history is not admissible to a non-synthetic gateway with identical trust.
    real = replace(config, synthetic_rehearsal=None)
    with pytest.raises(ValidationError, match='native candidate'):
        validate(rows, [], real)
