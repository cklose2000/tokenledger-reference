"""One command, one new synthetic source, native reports and a frozen original."""
from pathlib import Path
from time import perf_counter
import json,uuid

from tl.generate import Config,SyntheticWorld
from tl.metrics.definitions import FAMILIES
from tl.receipts.metrics import append_receipts,read_receipts,receipt_id
from tl.stream import Activity,Stream,ValidationError
from tl.stream.events import canonical
from tl.views.engine import profile
from tl.views.pack import pack_run
from tl.demo.bridge import create
from tl.demo.yield_bridge import create as create_yield

ASOF='2026-07-31'
ORIGINAL='2026-10-01T00:00:00Z'
CURRENT='2026-10-03T00:00:00Z'


def run(destination=None,*,customers=28,progress=None):
    started=perf_counter()
    root=Path(destination or Path('data/demo')/uuid.uuid4().hex).resolve()
    if root.exists() and any(root.iterdir()): raise ValidationError('demo requires a new or empty directory; existing evidence is never overwritten')
    root.mkdir(parents=True,exist_ok=True)
    db=root/'world.duckdb';outputs=root/'runs';ledger=root/'metrics.jsonl'
    def step(message):
        if progress: progress(message)
    step('Generate an isolated synthetic business through the validated writer.')
    stream=Stream(db);stream.init()
    generated=stream.generate(SyntheticWorld(Config(customers=customers)),workers=1)
    batch='demo:late-july'
    token=Activity(batch+':tokens','2026-07-31T23:59:59Z','tokens_processed',dict(
        usage_batch_id=batch,model='synthetic-g5',model_release_date='2026-02-01',token_type='input',
        tokens=1000000000,list_price_per_mtok=3.0,effective_price_per_mtok=3.0,channel='direct',workload='api'),'customer:0000001')
    stream.append_simulation([(token,'2026-07-31T23:59:59Z')],source='sim:demo-v1',actor='synthetic-demo')
    names=[*FAMILIES,'close_accounts']
    step('Report July with native views and receipts. All generated late arrivals are already known.')
    original=profile(db,asof=ASOF,known_at=ORIGINAL,names=names,output_root=outputs,ledger=ledger)
    step('Append the missing July invoice, then report the same July with later knowledge.')
    invoice=Activity(batch+':invoice','2026-07-31T23:59:59Z','usage_invoiced',dict(
        invoice_id=batch,usage_batch_id=batch,contract_id=None,period_start='2026-07-01',period_end='2026-08-01',
        gross_usd='3000.00',discount_usd='0.00',net_usd='3000.00',channel_fee_usd='0.00',
        commit_applied_usd='0.00',cash_due_usd='3000.00',channel='direct'),'customer:0000001')
    injected=stream.append_simulation([(invoice,'2026-10-02T12:00:00Z')],source='sim:demo-v1',actor='synthetic-demo')
    current=profile(db,asof=ASOF,known_at=CURRENT,names=names,output_root=outputs,ledger=ledger)
    records=read_receipts(ledger)
    account_id=lambda run:next(k for k in run['receipt_ids'] if records[k]['schema_version']=='tokenledger-views/v1' and records[k]['result']['output']=='close_accounts')
    yield_id=lambda run:next(k for k in run['receipt_ids'] if records[k]['schema_version']=='tokenledger-views/v1' and records[k]['result']['output']=='net_rev_per_mtok')
    step('Reperform both close populations and derive a balanced, receipt-backed account bridge.')
    bridge=create(account_id(original),account_id(current),db=db,output_root=outputs,ledger=ledger)
    step('Reperform recognized usage revenue per million tokens and explain its change with the same invoice.')
    business=create_yield(yield_id(original),yield_id(current),bridge['receipt_id'],db=db,output_root=outputs,ledger=ledger)
    step('Reproduce every original KPI population after the append; render the original pack from those saved rows.')
    pack=pack_run(original['run_id'],output_root=outputs,ledger=ledger,pack_root=root/'pack')
    # create() reperformed the original close; pack_run() reperformed its seven KPI populations.
    verified_ids=[k for k in original['receipt_ids'] if records[k]['schema_version']=='tokenledger-views/v1']
    manifest=dict(schema_version='tokenledger-demo/v1',asof=ASOF,synthetic=True,execution='views-v1',
        database=str(db),artifact_root=str(root),generation=generated,injection=injected,
        original_run_id=original['run_id'],current_run_id=current['run_id'],
        original_receipt_ids=verified_ids,bridge_receipt_id=bridge['receipt_id'],yield_bridge_receipt_id=business['receipt_id'],pack=pack,
        observed_workflow_seconds=perf_counter()-started,
        timing_scope='Generation through all reports, bridge re-performance and original pack re-performance/rendering; excludes final summary serialization and process startup',
        target_seconds=60,target_basis='Measured target, not a correctness assertion or large-workload benchmark',
        bigquery='not_run',matched_agent_benchmark='not_run')
    parent=records[account_id(current)]
    observation=dict(schema_version='tokenledger-demo-observation/v1',run_id=uuid.uuid4().hex,result=manifest,
        **{k:parent[k] for k in ('inputs','definition_version','query_hash','git_sha','execution_hash','asof','known_at','watermark')})
    observation['receipt_id']=receipt_id(observation);append_receipts(ledger,[observation])
    manifest['receipt_id']=observation['receipt_id']
    (root/'demo.json').write_text(canonical(manifest)+'\n',encoding='utf-8',newline='\n')
    lines=['# July recognized usage revenue per million tokens','',
        business['revenue_basis']+'.','',business['denominator']+'.','',
        '| Measure | Original | Currently known | Delta | Unit | Receipt |',
        '|---|---:|---:|---:|---|---|']
    for field,unit in business['units'].items():
        lines.append('| '+field+' | '+' | '.join(str(business[k][field]) for k in ('original','current','delta'))+' | '+unit+' | '+business['receipt_id']+' |')
    lines.extend(['',business['aggregation']+'. '+business['rounding']+'.','',
        'The tokens were already present. One newly admitted invoice increases recognized revenue. Debit and credit entries explain that single revenue change; they are not two changes in revenue.','',
        'This directory owns its source database, saved populations, compiled queries, receipts and original disclosure pack. No credentials or private application data were used.','',
        'The original July report is a deliberately delayed close, known through October 1. The synthetic missing invoice arrives October 2. That choice isolates its effect from the generator\'s ordinary late arrivals.','',
        '## Account bridge','',bridge['scope'],'',
        '| Month | Account | Before cents | After cents | Delta cents | Receipt |',
        '|---|---|---:|---:|---:|---|'])
    for row in bridge['rows']:
        lines.append('| '+' | '.join(str(row[k]) for k in ('month','account','before_cents','after_cents','delta_cents'))+' | '+bridge['receipt_id']+' |')
    lines.extend(['',f"Responsible activity: `{invoice.activity_id}`. {bridge['causal_basis']}.",'',
        'All original KPI populations and the original close were freshly reproduced after the late append. Timing is an observation, not a reproducible financial result.','',
        f"Workflow observation: {manifest['observed_workflow_seconds']:.3f} seconds; receipt `{observation['receipt_id']}`.",'',
        'Replay from the repository with the retained source and artifact root:', '', '```powershell',
        f"tl --db '{db}' --artifact-root '{root}' receipt {business['receipt_id']} --json",
        f"tl --db '{db}' --artifact-root '{root}' receipt {business['original_receipt_id']} --json",'```','',
        'The original disclosure pack is under `pack/`. All reporting populations, including the current July, are under `runs/`. This small functional demo supplies no new architecture speed ranking, agent-efficiency claim or BigQuery result.','',
        '## Next: the feedback loop inside reporting','',
        'Reports like this one can be inspected wrongly. `tl learning walkthrough` rehearses the governed learning loop on a fresh synthetic July NRR report: a wrong-date inspection becomes a recorded finding, an independent verification, a bounded diagnostic candidate evaluated against frozen cases, a signed one-run approval boundary, the first next inspection and its recorded outcome, with the original report reproduced afterwards. The walkthrough is synthetic and credential-free; the one real native trial completed inconclusive with no promotion.'])
    (root/'README.md').write_text('\n'.join(lines)+'\n',encoding='utf-8',newline='\n')
    return dict(status='demonstrated',artifact_root=str(root),bridge=bridge,yield_bridge=business,
        original_receipt_ids=verified_ids,original_reproduced=True,pack=pack,
        observed_workflow_seconds=manifest['observed_workflow_seconds'],observation_receipt_id=observation['receipt_id'])


def replay_observations(ids,*,ledger):
    records=read_receipts(ledger)
    return [dict(receipt_id=k,verified=True,recomputed=False,result=records[k]['result']) for k in ids]
