"""Report retained receipts without manufacturing a tenfold conclusion."""
from pathlib import Path
import hashlib
import json
import statistics

from tl.receipts.metrics import read_receipts
from tl.stream import ValidationError


def render(bundle,destination):
    bundle=Path(bundle);spec=json.loads(bundle.read_text(encoding='utf-8'))
    local_ledger=bundle.parent/'metrics.jsonl'
    records=read_receipts(local_ledger if local_ledger.exists() else Path(spec['ledger']))
    def run_root(run):
        local=bundle.parent/'runs'/run['run_id']
        return local if local.exists() else Path(run['run_directory'])
    if 'observation_receipt_id' in spec:
        expected=records[spec['observation_receipt_id']]['result']
        if expected!={k:v for k,v in spec.items() if k!='observation_receipt_id'}: raise ValidationError('suite evidence changed')
    manifests=[]
    for run in spec['runs']:
        path=run_root(run)/'run.json';m=json.loads(path.read_text())
        observation=records[run['observation_receipt_id']]
        if hashlib.sha256(path.read_bytes()).hexdigest()!=observation['result']['manifest_sha256']: raise ValidationError('comparison observation changed')
        for row in run['results']:
            if records[row['receipt_id']]['result']!={k:v for k,v in row.items() if k!='receipt_id'}: raise ValidationError('population evidence changed')
        manifests.append((run,m))
    first=manifests[0][1];inv=first['inventory']
    def summary(values): return f'{statistics.median(values):.3f} ({min(values):.3f}–{max(values):.3f})' if values else 'not_run'
    def spine_sql(m):
        o=m['observations'];s=o['spine']
        return s['setup_seconds']+s['validation_seconds']+s.get('kpi_batch',{}).get('seconds',0)+sum(b['seconds'] for b in s.get('batches',[]))+sum(v['query_seconds'] or 0 for v in o['spine_outputs'].values())
    def baseline_sql(m):
        o=m['observations']
        return o['baseline_projection']['seconds']+o['baseline_build_and_tests_seconds']+sum(v['query_seconds'] for v in o['baseline_outputs'].values())
    def native_total(m):
        o=m['observations']
        return o.get('native_process',{}).get('wall_seconds',o['spine']['total_seconds'])
    lines=['# DuckDB execution: native views and a fresh baseline','',
        'Synthetic data. Both paths independently recognize commercial events, then reconcile every registered journal, close and KPI population. '
        'The native path retains the fourteen-column activity stream and stores no derived reporting tables. Query-local CTE buffers, joins, sorts and spills remain real work.','',
        f"Runtime: DuckDB {first['runtime']['duckdb']} on both sides; four threads, 4 GB engine memory limits, UTC. The baseline retains its dbt models and dependency selection. Its source projections also use the native frozen reader. '{spec['os_cache']}'.",'',
        'The accepted [Phase 3b report](collapse-report.md) remains unchanged. This is a new execution experiment, not a revision of those historical observations. BigQuery execution and performance remain **not_run**.','',
        f"Workload: {spec['workload']['customers']} synthetic customers across {spec['workload']['months']} months; scale gate {spec['workload']['scale_gate']}. Suite observation `{spec.get('observation_receipt_id','pending')}`.",'',
        '| Workload / trial | Reporting / knowledge | Order | Equivalence | Observation receipt |',
        '|---|---|---|---|---|']
    for run,m in manifests:
        lines.append(f"| {run['workload']} / {run['trial']} | {m['asof']} / {m['known_at']} | {m['order']} | {run['status']} | `{run['observation_receipt_id']}` |")
    if spec.get('process_policy'):
        lines+=['',spec['process_policy'],'',
            '| Trial | Entire comparison process seconds | Comparison worker peak RSS MiB |',
            '|---|---:|---:|']
        for run,_ in manifests:
            p=run['process_observation'];peak=p.get('peak_rss_bytes')
            lines.append(f"| {run['workload']} / {run['trial']} | {p['wall_seconds']:.3f} | {'unavailable' if peak is None else f'{peak/1048576:.1f}'} |")
        lines+=['','This process clock includes both implementations, fingerprints, comparison and publication. '
            'It is not either architecture\'s query time. Peak RSS is the comparison worker\'s OS high-water mark; the separate dbt child is excluded.']
    if any('native_process' in m['observations'] for _,m in manifests):
        lines+=['','| Trial | Native process peak RSS MiB | Baseline projection process peak RSS MiB |',
            '|---|---:|---:|']
        for run,m in manifests:
            o=m['observations'];a=o['native_process'];b=o['baseline_projection']['process_observation']
            fmt=lambda p:'unavailable' if 'peak_rss_bytes' not in p else f"{p['peak_rss_bytes']/1048576:.1f}"
            lines.append(f"| {run['workload']} / {run['trial']} | {fmt(a)} | {fmt(b)} |")
        lines+=['','Each timed side exits before the next starts. Native evidence time and baseline projection time include their process startup. The separate dbt child RSS remains unmeasured.']
    lines+=['','Currency and integer counts match exactly; floating measures use absolute tolerance 1e-6. Missing rows, duplicate keys and NULL availability states are compared. Source activity IDs remain attached to accounting differences.','',
        '| Workload | Repetitions | Native execution seconds | Baseline projection + build/tests + output seconds | Native evidence seconds |',
        '|---|---:|---:|---:|---:|']
    for label in dict.fromkeys(r['workload'] for r,_ in manifests):
        selected=[m for r,m in manifests if r['workload']==label and r['trial']!='warmup']
        lines.append(f"| {label} | {len(selected)} | {summary([spine_sql(m) for m in selected])} | {summary([baseline_sql(m) for m in selected])} | {summary([native_total(m)-spine_sql(m) for m in selected])} |")
    lines+=['','Values show median (minimum to maximum); warmups remain above but are excluded from summaries. '
        'Native execution includes accounting validation and Arrow result transfer. Baseline includes its own dbt tests. '
        'Evidence time covers fresh whole-stream input hashing, output fingerprints and artifact writing. Comparison matching and baseline artifact hashing are retained separately in each run. '
        'No elapsed-time replay is claimed. These are sequential laptop observations, not cloud rankings.','',
        '| Inventory | Baseline | Native spine |','|---|---:|---:|',
        f"| Catalog relations including each path's source bindings | {len(inv['baseline']['relations'])} | {len(inv['spine']['relations'])} |",
        f"| dbt models / named query nodes including requested outputs | {inv['baseline']['models']} | {inv['spine']['query_nodes']} |",
        f"| Registered output populations | {inv['spine']['registered_outputs']} | {inv['spine']['registered_outputs']} |",
        f"| Longest logical path | {inv['baseline']['longest_path']} | {inv['spine']['longest_logical_path']} |",
        f"| Stored derived tables on native path | n/a | {inv['spine']['stored_derived_tables']} |",'',
        'A named CTE is not a stored relation, and moving SQL into a query does not erase its complexity. '
        'The manifest enumerates each query node, dependency, catalog relation and authored file. This report makes no tenfold total-complexity claim. '
        f"Inventory receipt `{spec['runs'][0]['observation_receipt_id']}`.",'',
        '| Authored footprint | Nonblank, non-line-comment lines |','|---|---:|',
        f"| Baseline SQL / macro / model configuration files | {sum(r['nonblank_non_line_comment_lines'] for r in inv['baseline_source_files'])} |",
        f"| Native execution Python and SQL plus shared metric SQL | {sum(r['lines'] for r in inv['spine']['files'])} |",'',
        'These are file inventories, not language-equivalent complexity scores. Shared writer, definitions, validation and receipt machinery remain additional common code.','',
        '## Definition changes and late evidence','',
        '| Trial | NRR only: query + validation seconds | NRR only: complete receipted request seconds | Receipt |',
        '|---|---:|---:|---|']
    for run in spec.get('nrr_only',[]):
        m=json.loads((run_root(run)/'run.json').read_text());o=m['observations']
        rec=records[run['receipt_ids'][-1]]
        if hashlib.sha256((run_root(run)/'run.json').read_bytes()).hexdigest()!=rec['result']['manifest_sha256']: raise ValidationError('NRR observation changed')
        seconds=o['setup_seconds']+o['validation_seconds']+sum(b['seconds'] for b in o.get('batches',[]))+sum(v['query_seconds'] or 0 for v in m['outputs'].values())
        total=run.get('process_observation',{}).get('wall_seconds',o['total_seconds'])
        lines.append(f"| {run['trial']} | {seconds:.3f} | {total:.3f} | `{run['receipt_ids'][-1]}` |")
    lines+=['','The NRR request includes the headline and complete membership population. Its query dependency list excludes token, seat, compute, journal and dense customer-month execution. '
        'Full thirteen-output equivalence is also rerun after changing the version. The baseline receives its normal `int_nrr_lenses+` selection; neither path needs duplicated floor literals. '
        'The released v2 and v3 YAMLs make this a version selection, not evidence of how many files an arbitrary estate must edit.','',
        '| Restatement work | Baseline executed models | Native stored objects refreshed |','|---|---:|---:|']
    for label in ('late_june','one_day','nrr_floor'):
        chosen=next((m for r,m in manifests if r['workload']==label and r['trial']=='1'),None)
        if chosen: lines.append(f"| {label} | {sum(n['node'].startswith('model.') for n in chosen['executed_baseline_nodes'])} | 0 |")
    lines+=['','Zero stored-object refreshes does not mean zero restatement work: native queries and evidence verification execute again. '
        'Baseline raw-source appends, selected model rebuilds, tests and reads are all included in its clocks.','',
        '## Reproduction and release gates','',f"Replay evidence: `{spec.get('replay',{})}`.",'',
        'The full replay scans the source and independently reruns each native population. The baseline comparison side verifies its retained content; it does not claim fresh dbt execution during receipt replay. '
        'The comparison runs themselves execute dbt afresh. Independent QA, original-pack replay, source preservation and the full regression suite remain release gates. '
        'Native BigQuery jobs, costs and agent-efficiency claims are separate unrun gates.','']
    destination=Path(destination);destination.parent.mkdir(parents=True,exist_ok=True)
    destination.write_text('\n'.join(lines),encoding='utf-8',newline='\n')
    return dict(path=str(destination),sha256=hashlib.sha256(destination.read_bytes()).hexdigest())


def retain(bundle,destination):
    """Portable observation bundle; original paths remain signed and untouched."""
    import shutil
    from tl.receipts.metrics import append_receipts
    bundle=Path(bundle);destination=Path(destination)
    if destination.exists(): raise ValidationError('retention destination must be new')
    spec=json.loads(bundle.read_text(encoding='utf-8'));records=read_receipts(spec['ledger'])
    destination.mkdir(parents=True);runs={}
    for run in spec['runs']:
        origin=Path(run['run_directory']);runs[run['run_id']]=origin
        native=json.loads((origin/'run.json').read_text())['spine_run_id'];runs[native]=origin.parent/native
    for run in spec.get('nrr_only',[]): runs[run['run_id']]=Path(run['run_directory'])
    for key,origin in runs.items():
        target=destination/'runs'/key;target.mkdir(parents=True)
        for name in ('run.json','rows.jsonl'): shutil.copyfile(origin/name,target/name)
        if (origin/'dbt-artifacts').exists():
            (target/'dbt-artifacts').mkdir()
            for name in ('run_results.json','build.log'): shutil.copyfile(origin/'dbt-artifacts'/name,target/'dbt-artifacts'/name)
    selected=[r for r in records.values() if r['run_id'] in runs or r['receipt_id']==spec.get('observation_receipt_id')]
    append_receipts(destination/'metrics.jsonl',selected)
    shutil.copyfile(bundle,destination/'comparison.json')
    (destination/'README.md').write_text('Report observations, exact manifests, receipts and dbt execution evidence. Source and full business populations remain in the original ignored benchmark directory. Run `tl compare --render <this-directory>/comparison.json --report <destination.md> --json` from the repository to verify and render. Historical absolute paths are content-bound evidence; the renderer resolves this portable observation bundle by run ID. Hashes are not authenticated signatures.\n',encoding='utf-8')
    return destination/'comparison.json'
