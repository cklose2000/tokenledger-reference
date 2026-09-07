"""Private presentation reads only saved rows re-performed by the shared engine."""

import csv,hashlib
from pathlib import Path

from tl.metrics.engine import read_run,replay_receipts,run_metrics
from tl.stream.events import canonical,ValidationError,timestamp
from tl.usage.window import window


def report(application, *, start,end,known_at,timezone='UTC',gaps_only=False):
    bounds=window(start,end,timezone)
    cutoff=timestamp(known_at)
    asof=max(timestamp(bounds['end_utc']).date(),cutoff.date()).isoformat()
    result=run_metrics(application.db,asof=asof,known_at=known_at,
        names=['usage_gaps'] if gaps_only else ['usage_tokens','usage_gaps'],
        output_root=application.outputs,ledger=application.ledger,application=application,
        session_options=dict(start=start,end=end,timezone=timezone))
    rendered=render(application,result['run_id'])
    return dict(status='partial_report',run_id=result['run_id'],report_directory=str(rendered),rows=result['rows'])


def render(application,run_id):
    run=read_run(run_id,output_root=application.outputs,ledger=application.ledger,application=application)
    manifest=run['manifest']
    if not manifest.get('report_window') or set(manifest['queries'])-{'usage_tokens','usage_gaps'}:
        raise ValidationError('private report requires a registered usage window and report population')
    replay_receipts([r['receipt_id'] for r in run['rows']],db=application.db,
        output_root=application.outputs,ledger=application.ledger,application=application)
    root=application.path('outputs/'+run_id+'/report')
    root.mkdir(exist_ok=False)
    bounds=manifest['report_window']
    lines=['# Personal token usage','',
        f"Window: {bounds['start']} inclusive to {bounds['end']} exclusive, {bounds['timezone']}.",
        f"UTC bounds: {bounds['start_utc']} to {bounds['end_utc']}. Knowledge cutoff: {manifest['known_at']}.",'',
        'These are locally observed cumulative counter changes within each session. The opening balance,',
        'cross-boundary intervals and unobserved tails are excluded. Missing or decreasing adjacent counters',
        'make the category unknown. Cache and reasoning are separate overlapping categories. Last counters',
        'are retained as evidence and never added to cumulative growth. No cross-session/account total,',
        'provider revenue, model attribution, subscription bill or estimated cost is inferred.','',
        'Every result below comes from saved metric rows and has been reproduced from its receipt.','',
        '| Account / source / session | Category | Observed tokens | Status | Receipt |',
        '|---|---|---:|---|---|']
    def safe(value):
        return str(value).replace('|','\\|').replace('\n',' ').replace('\r',' ')
    for r in run['rows']:
        if r['metric']!='usage_tokens':
            continue
        d,v=r['dimensions'],r['values']
        cells=[f"{d.get('account','unknown')} / {d.get('source','unknown')} / {d.get('scope_id','unknown')}",
               d.get('token_kind','unknown'),'unknown' if v.get('observed_tokens') is None else v['observed_tokens'],
               r['status'],r['receipt_id']]
        lines.append('| '+' | '.join(map(safe,cells))+' |')
    lines+=['','## Coverage and next actions','','| Account | Surface / missing scope | Status | Next action | Receipt |','|---|---|---|---|---|']
    for r in run['rows']:
        if r['metric']=='usage_gaps':
            d=r['dimensions']
            lines.append('| '+' | '.join(map(safe,[d.get('account','unknown'),d.get('surface','unknown')+' / '+d.get('gap','unknown'),
                r['status'],d.get('next_action','Confirm inventory and source coverage'),r['receipt_id']]))+' |')
    (root/'report.md').write_text('\n'.join(lines)+'\n',encoding='utf-8',newline='\n')
    with (root/'metrics.csv').open('x',encoding='utf-8',newline='') as handle:
        writer=csv.DictWriter(handle,fieldnames=['metric','definition','dimensions','measure','value','unit','status','receipt_id'])
        writer.writeheader()
        for r in run['rows']:
            for measure,value in (r['values'] or {'coverage_status':None}).items():
                writer.writerow(dict(metric=r['metric'],definition=r['definition_version'],dimensions=canonical(r['dimensions']),
                    measure=measure,value=value,unit=r['units'].get(measure,''),status=r['status'],receipt_id=r['receipt_id']))
    for name in ('run.json','inputs.jsonl'):
        source=Path(run['run_directory'])/name
        (root/name).write_bytes(source.read_bytes())
    from tl.receipts.metrics import read_receipts
    records=read_receipts(application.ledger)
    (root/'receipts.jsonl').write_text(''.join(canonical(records[r['receipt_id']])+'\n' for r in run['rows']),encoding='utf-8')
    (root/'rows.jsonl').write_text(''.join(canonical(r)+'\n' for r in run['rows']),encoding='utf-8')
    hashes={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(root.iterdir())}
    (root/'manifest.json').write_text(canonical(dict(schema_version='tokenledger-private-report/v1',run_id=run_id,
        synthetic=False,files=hashes,receipt_ids=[r['receipt_id'] for r in run['rows']]))+'\n',encoding='utf-8')
    return root
