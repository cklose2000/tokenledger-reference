from pathlib import Path
import json

import duckdb
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from tl.cli import cli
from tl.demo.bridge import calculate
from tl.demo.yield_bridge import ratio, summarize
from tl.metrics.engine import replay_receipts
from tl.receipts.metrics import read_receipts
from tl.stream import ValidationError


@pytest.fixture(scope='module')
def demonstration(tmp_path_factory):
    root=tmp_path_factory.mktemp('native-demo')/'run'
    shared=Path('ledger/metrics.jsonl').read_bytes()
    process=Path('ledger/process.jsonl').read_bytes()
    result=CliRunner().invoke(cli,['demo','--directory',str(root),'--json'])
    assert result.exit_code==0,result.output
    # Progress is stderr in the real CLI; Click's test result combines streams.
    answer=json.loads(next(line for line in result.output.splitlines() if line.startswith('{')))
    assert Path('ledger/metrics.jsonl').read_bytes()==shared
    assert Path('ledger/process.jsonl').read_bytes()==process
    return root,answer


def test_demo_native_july_bridge_and_frozen_original(demonstration):
    root,result=demonstration
    assert result['status']=='demonstrated' and result['original_reproduced']
    assert len(result['original_receipt_ids'])==8
    assert result['bridge']['responsible_activity_ids']==['demo:late-july:invoice']
    assert {(r['month'],r['account'],r['delta_cents']) for r in result['bridge']['rows']}=={
        ('2026-07-01','1100',300000),('2026-07-01','4000',-300000)}
    with duckdb.connect(str(root/'world.duckdb'),read_only=True) as conn:
        assert conn.execute("SELECT schema_name,table_name FROM duckdb_tables() WHERE NOT internal").fetchall()==[('stream','activity')]
        assert len(conn.execute("DESCRIBE stream.activity").fetchall())==14
    records=read_receipts(root/'metrics.jsonl')
    for key in result['original_receipt_ids']:
        manifest=json.loads((root/'runs'/records[key]['run_id']/'run.json').read_bytes())
        assert manifest['execution_artifacts_match_git'] or manifest['modified_execution_artifacts']
        assert not [r for r in manifest['relations'] if r[-1]=='table' and r[0]!='source_db']
    business=result['yield_bridge']
    assert business['delta']['net_revenue_cents']==300000
    assert business['delta']['tokens']==0 and business['original']['tokens']>0
    assert business['delta']['net_of_discounts_and_fees_per_mtok']==ratio(300000,business['original']['tokens'])
    assert business['responsible_activity_ids']==['demo:late-july:invoice']
    assert business['original_receipt_id'] in result['original_receipt_ids']
    ids=[*result['original_receipt_ids'],result['bridge']['receipt_id'],business['receipt_id'],result['observation_receipt_id']]
    replayed=replay_receipts(ids,db=root/'world.duckdb',output_root=root/'runs',ledger=root/'metrics.jsonl')
    assert len(replayed)==11 and all(r['verified'] for r in replayed)
    assert next(r for r in replayed if r['receipt_id']==result['bridge']['receipt_id'])['recomputed']
    assert not next(r for r in replayed if r['receipt_id']==result['observation_receipt_id'])['recomputed']
    pack=Path(result['pack']['path'])
    assert len(list(pack.glob('*.csv')))==7
    assert next(r for r in replayed if r['receipt_id']==business['receipt_id'])['result']['delta']==business['delta']


def test_demo_refuses_overwrite_and_repeated_runs_are_isolated(demonstration,tmp_path):
    root,first=demonstration
    before=(root/'demo.json').read_bytes()
    refused=CliRunner().invoke(cli,['demo','--directory',str(root),'--json'])
    assert refused.exit_code!=0 and (root/'demo.json').read_bytes()==before
    second=CliRunner().invoke(cli,['demo','--directory',str(tmp_path/'second'),'--customers','7','--json'])
    assert second.exit_code==0,second.output
    result=json.loads(next(line for line in second.output.splitlines() if line.startswith('{')))
    assert not set(first['original_receipt_ids'])&set(result['original_receipt_ids'])


def test_demo_bridge_rejects_saved_population_or_bridge_tamper(demonstration):
    root,result=demonstration
    bridge=result['bridge'];records=read_receipts(root/'metrics.jsonl')
    parent=records[bridge['current_receipt_id']]
    paths=[root/'runs'/parent['run_id']/'close_accounts.parquet',root/'runs'/bridge['run_id']/'bridge.json']
    for path in paths:
        saved=path.read_bytes()
        try:
            if path.suffix=='.parquet':
                table=pq.read_table(path);rows=table.to_pylist();rows[0]['amount_cents']+=1
                import pyarrow as pa
                pq.write_table(pa.Table.from_pylist(rows,schema=table.schema),path)
            else:
                value=json.loads(saved);value['rows'][0]['delta_cents']+=1
                path.write_text(json.dumps(value),encoding='utf-8')
            with pytest.raises(ValidationError):
                replay_receipts([bridge['receipt_id']],db=root/'world.duckdb',output_root=root/'runs',ledger=root/'metrics.jsonl')
        finally: path.write_bytes(saved)


def test_demo_bridge_refuses_definition_or_cutoff_change(demonstration):
    root,result=demonstration;bridge=result['bridge']
    with pytest.raises(ValidationError,match='increasing'):
        calculate(bridge['current_receipt_id'],bridge['original_receipt_id'],db=root/'world.duckdb',output_root=root/'runs',ledger=root/'metrics.jsonl')


def test_yield_aggregation_weights_tokens_and_keeps_fee_basis():
    rows=[dict(month='2026-07-01',tokens=1000000,net_revenue_cents=200,channel_fee_cents=10,revenue_before_channel_fees_cents=210),
          dict(month='2026-07-01',tokens=9000000,net_revenue_cents=900,channel_fee_cents=90,revenue_before_channel_fees_cents=990),
          dict(month='2026-06-01',tokens=1,net_revenue_cents=10000,channel_fee_cents=0,revenue_before_channel_fees_cents=10000)]
    summary=summarize(rows,'2026-07-01')
    assert summary['tokens']==10000000
    assert summary['net_of_discounts_and_fees_per_mtok']=='1.100000000000'
    assert summary['net_of_discounts_per_mtok']=='1.200000000000'
    assert summarize([],'2026-07-01')['net_of_discounts_per_mtok'] is None
    rows[0]['channel_fee_cents']+=1
    with pytest.raises(ValidationError,match='basis'): summarize(rows,'2026-07-01')


def test_yield_bridge_rejects_altered_kpi_or_summary(demonstration):
    root,result=demonstration;business=result['yield_bridge'];records=read_receipts(root/'metrics.jsonl')
    parent=records[business['current_receipt_id']]
    paths=[root/'runs'/parent['run_id']/'net_rev_per_mtok.parquet',root/'runs'/business['run_id']/'yield.json']
    for path in paths:
        saved=path.read_bytes()
        try:
            if path.suffix=='.parquet':
                import pyarrow as pa
                table=pq.read_table(path);rows=table.to_pylist();rows[0]['tokens']+=1
                pq.write_table(pa.Table.from_pylist(rows,schema=table.schema),path)
            else:
                value=json.loads(saved);value['delta']['net_revenue_cents']+=1
                path.write_text(json.dumps(value),encoding='utf-8')
            with pytest.raises(ValidationError):
                replay_receipts([business['receipt_id']],db=root/'world.duckdb',output_root=root/'runs',ledger=root/'metrics.jsonl')
        finally: path.write_bytes(saved)


@pytest.mark.parametrize('schema',['tokenledger-demo-bridge/v1','tokenledger-demo-yield/v1',
                                  'tokenledger-challenge/v1','tokenledger-challenge-score/v1'])
def test_derived_native_replay_retains_registered_output_contract(schema):
    import hashlib,subprocess
    from tl.receipts.archive import git_code
    sha=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
    path='baseline/outputs.yaml'
    content=subprocess.check_output(['git','show',sha+':'+path]).replace(b'\r\n',b'\n')
    manifest=dict(schema_version=schema,git_sha=sha,execution_artifacts={path:hashlib.sha256(content).hexdigest()})
    assert git_code(manifest)=={path:content}
    with pytest.raises(ValidationError,match='unregistered'):
        git_code({**manifest,'execution_artifacts':{'private/operating-plan.md':'0'*64}})
