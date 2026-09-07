"""Render only retained comparison observations, with receipt references."""
from pathlib import Path
from datetime import datetime
import hashlib
import json
import statistics
from tl.stream.events import ValidationError,canonical
from tl.receipts.metrics import read_receipts


def summary(values):
    if not values:
        return 'not_run'
    return f'{statistics.median(values):.3f} ({min(values):.3f} to {max(values):.3f})'


def timing_scope(manifest):
    observed=manifest['observations']
    elapsed=(datetime.fromisoformat(manifest['finished_at'].replace('Z','+00:00'))-
             datetime.fromisoformat(manifest['started_at'].replace('Z','+00:00'))).total_seconds()
    queries=sum(row['spine_seconds']+row['baseline_seconds'] for row in manifest['outputs'].values())
    measured=(observed['common_snapshot_seconds']+observed['spine_build_seconds']+
              observed['baseline_projection']['seconds']+observed['baseline_build_and_tests_seconds']+queries)
    return dict(elapsed=elapsed,snapshot=observed['common_snapshot_seconds'],queries=queries,
                residual=elapsed-measured)


def render(bundle,destination):
    bundle=Path(bundle);spec=json.loads(bundle.read_text(encoding='utf-8'))
    records=read_receipts(Path(spec['ledger']))
    if spec.get('observation'):
        observation=spec['observation'];record=records[observation['receipt_id']]
        raw=(Path(observation['run_directory'])/'run.json').read_bytes()
        if hashlib.sha256(raw).hexdigest()!=record['result']['manifest_sha256']:
            raise ValidationError('suite observations changed')
        for key,value in record['result']['observations'].items():
            if key=='bridges':
                if hashlib.sha256(Path(spec['bridges']['path']).read_bytes()).hexdigest()!=value['sha256']:
                    raise ValidationError('bridge observations changed')
            elif spec[key]!=value: raise ValidationError('suite result differs from observation receipt')
    runs=spec['runs']
    for run in runs:
        for row in run['results']:
            record=records[row['receipt_id']]
            if record['result']!={k:v for k,v in row.items() if k!='receipt_id'}:
                raise ValidationError('comparison report result differs from receipt')
        observation=records[run['observation_receipt_id']]
        manifest_bytes=(Path(run['run_directory'])/'run.json').read_bytes()
        if hashlib.sha256(manifest_bytes).hexdigest()!=observation['result']['manifest_sha256']:
            raise ValidationError('comparison report observations changed')
    first=json.loads((Path(runs[0]['run_directory'])/'run.json').read_text(encoding='utf-8'))
    inv=first['inventory']
    baseline_total=inv['baseline']['models']+inv['baseline']['sources']
    spine_total=len([r for r in inv['spine']['relations'] if r['name']!='_snapshot'])
    lines=['# Architecture comparison: local observations','',
           'Synthetic data. Independent Python recognition and dbt SQL recognition consume the same knowledge-filtered stream. '
           'Generated recognition events are excluded from both calculations. This is a scoped accounting experiment, not an audit opinion.','',
           'BigQuery correctness, timing, bytes, slot-ms and cost: **not_run**. Local seconds do not establish cloud performance.','',
           f"Workload: {spec['workload']['customers']} synthetic customers, {spec['workload']['months']} months, seed {spec['workload']['seed']}. Scale gate: {spec['workload']['scale_gate']}. Suite evidence: `{spec.get('observation',{}).get('receipt_id','pending')}`.",'',
           '| Workload | Period | Knowledge cutoff | Status | Output differences | Observation receipt |',
           '|---|---|---|---|---:|---|']
    for run in runs:
        manifest=json.loads((Path(run['run_directory'])/'run.json').read_text(encoding='utf-8'))
        differences=sum(row['different_rows']+row['duplicate_spine']+row['duplicate_baseline'] for row in run['results'])
        lines.append(f"| {run['workload']} / {run['trial']} | {manifest['asof']} | {manifest['known_at']} | {run['status']} | {differences} | `{run['observation_receipt_id']}` |")
    lines+=['','The output registry includes journal lines, recognized lines, account close and rollforwards, all seven KPI families with every allowed commercial cut, and NRR/customer cohort membership. Missing, extra, duplicate and NULL states are checked before aggregation.','',
            '| Inventory | Baseline | Spine |','|---|---:|---:|',
            f"| dbt models / scaffold views | {inv['baseline']['models']} | {inv['spine']['scaffold_views']} |",
            f"| Raw baseline projections / canonical source | {inv['baseline']['sources']} | {inv['spine']['canonical_tables']} |",
            f"| Scaffold caches | n/a | {inv['spine']['scaffold_caches']} |",
            f"| Recognition and journal populations | included in models | {inv['spine']['recognition_materializations']+inv['spine']['journal_materializations']} |",
            f"| Longest source-to-KPI path | {inv['baseline']['longest_path']} | {inv['spine']['longest_path']} |",'',
            f"Inventory receipt: `{runs[0]['observation_receipt_id']}`. Helper relations, authored/compiled SQL, Python support and dependency lists are retained in the manifest. Views, caches, raw projections and logical queries have separate categories; the view count alone is not a total-object reduction claim.", '',
            '| Workload | Measured repetitions | Spine build median seconds (range) | Baseline projection/build/tests median seconds (range) |','|---|---:|---|---|']
    position=lines.index('| Workload | Measured repetitions | Spine build median seconds (range) | Baseline projection/build/tests median seconds (range) |')
    size_lines=[f"Relations beyond the common stream/snapshot: baseline {baseline_total}, spine {spine_total}; observed relation ratio {baseline_total/spine_total:.3f}. This includes ephemeral SQL bindings to Python/Arrow inputs on the spine. It excludes retained report files and counts those source populations separately. This result does not establish an order-of-magnitude reduction.",'',
                '| Authored footprint | Nonblank/non-line-comment lines | UTF-8 bytes after that filter |','|---|---:|---:|']
    groups={
      'Baseline model SQL':lambda p:p.startswith('baseline/models/') and p.endswith('.sql'),
      'Baseline macros':lambda p:p.startswith('baseline/macros/'),
      'Spine scaffold and metric SQL (including retained versions)':lambda p:p.startswith(('scaffolds/','tl/metrics/sql/')),
      'Spine accounting and session Python':lambda p:p in ('tl/compare/accounting.py','tl/compare/session.py'),
      'Other Python support, shared writer/receipts and comparison tooling':lambda p:p.startswith('tl/') and p.endswith('.py') and p not in ('tl/compare/accounting.py','tl/compare/session.py'),
      'Baseline YAML and shared output registry':lambda p:p.startswith('baseline/') and p.endswith(('.yaml','.yml')),
      'Domain definition YAML (including retained versions)':lambda p:p.startswith('definitions/')}
    for name,predicate in groups.items():
        files=[f for f in inv['files'] if predicate(f['path'])]
        size_lines.append(f"| {name} | {sum(f['nonblank_non_line_comment_lines'] for f in files)} | {sum(f['noncomment_utf8_bytes'] for f in files)} |")
    size_lines+=['',f"Compiled baseline model SQL: {inv['compiled_sql_lines']} nonblank/non-line-comment lines. Formatting affects line counts; bytes and the full file inventory are retained. Shared or retained support code is not described as exclusively executed by either implementation. Footprint receipt: `{runs[0]['observation_receipt_id']}`.",'']
    referenced_sources={d for node in inv['baseline']['nodes'] for d in node['dependencies'] if d.startswith('source.')}
    size_lines += [f"The baseline manifest references {len(referenced_sources)} of its {inv['baseline']['sources']} source projections in model dependencies. The inventory counts every created projection, including projections not consumed by the registered reports. Lineage measures relation/query dependency nodes; it does not expand Python function internals into fictitious warehouse objects. These details are bound by the same inventory receipt.", '']
    baseline_tests=[node for node in first['executed_baseline_nodes'] if node['node'].startswith('test.')]
    size_lines += [f"First retained run: {len(baseline_tests)} dbt tests executed, with statuses {sorted({node['status'] for node in baseline_tests})}. Evidence: `{runs[0]['observation_receipt_id']}`. Named spine invariant results are retained in that run manifest. Builder regression is recorded separately in the phase status and verification artifacts. Instrumented line/branch coverage: **not_run**; test counts are not coverage percentages.", '']
    lines[position:position]=size_lines
    for workload in sorted({r['workload'] for r in runs}):
        measured=[r for r in runs if r['workload']==workload and r['trial']!='warmup']
        left=[];right=[]
        for run in measured:
            m=json.loads((Path(run['run_directory'])/'run.json').read_text(encoding='utf-8'))['observations']
            left.append(m['spine_build_seconds']);right.append(m['baseline_projection']['seconds']+m['baseline_build_and_tests_seconds'])
        lines.append(f'| {workload} | {len(measured)} | {summary(left)} | {summary(right)} |')
    lines+=['','| Per-run timing scope | Measured repetitions | Recorded elapsed seconds (median and range) | Common snapshot seconds | Output-query seconds, both sides | Residual harness seconds |',
            '|---|---:|---|---|---|---|']
    for workload in sorted({r['workload'] for r in runs}):
        measured=[r for r in runs if r['workload']==workload and r['trial']!='warmup']
        values=[timing_scope(json.loads((Path(r['run_directory'])/'run.json').read_text(encoding='utf-8'))) for r in measured]
        cells=' | '.join(summary([v[key] for v in values]) for key in ('elapsed','snapshot','queries','residual'))
        lines.append(f'| {workload} | {len(measured)} | {cells} |')
    lines+=['',
            'Timing cells derive from the observation receipts listed for each run above. Recorded elapsed spans run start through the final output comparison; it excludes baseline cloning between runs, suite-level replay, migration rehearsal and final report writing. Residual harness time is elapsed minus the separately timed snapshot, both builds/projection/tests and output queries. It includes input/output hashing, receipt preparation, file serialization, comparison and uninstrumented setup; those components were not individually timed. Each residual is calculated per run; column medians need not sum to the elapsed median. This residual is not an architecture build time.', '',
            'The retained source/configuration pins specify four DuckDB threads on both paths and four concurrent dbt models. The baseline dbt build caps DuckDB memory at 4 GB; baseline source projection and the spine use DuckDB defaults. OS page caching and other laptop activity are uncontrolled. These are observations of the implemented local profiles, not a normalized hardware comparison. Inventory and footprint describe the frozen measured source revision, not later report-rendering edits.']
    lines+=['','| Change work, final measured repetition | Baseline model nodes executed | Baseline tests executed | Spine caches rebuilt | Definition files edited during run |','|---|---:|---:|---:|---|']
    work_notes=[]
    for workload in ('late_june','one_day','nrr_floor'):
        chosen=[r for r in runs if r['workload']==workload and r['trial']!='warmup']
        if not chosen: continue
        run=chosen[-1];m=json.loads((Path(run['run_directory'])/'run.json').read_text())
        executed=m['executed_baseline_nodes']
        lines.append(f"| {workload} | {sum(r['node'].startswith('model.') for r in executed)} | {sum(r['node'].startswith('test.') for r in executed)} | {m['inventory']['spine']['scaffold_caches']} | 0 on both sides; runtime selection is retained |")
        source_note='No source rows are appended for this definition selection.' if workload=='nrr_floor' else 'The canonical source is appended, never updated.'
        work_notes.extend(['',f"{workload} evidence: `{run['observation_receipt_id']}`. Source projection changes: `{m['observations']['baseline_projection'].get('inserted',{})}`. Affected baseline table models rebuild in full; local DuckDB models have no partition rewrite claim. {source_note}"])
    lines+=work_notes
    if spec.get('bridges'):
        bridges=json.loads(Path(spec['bridges']['path']).read_text(encoding='utf-8'))
        lines+=['','| Before/after bridge | Newly visible upstream activities | Changed registered populations | Changed registered rows |',
                '|---|---:|---:|---:|']
        for name,change in sorted(bridges.items()):
            values=list(change['outputs'].values())
            lines.append(f"| {name} | {len(change['newly_visible_upstream_activity_ids'])} | {sum(v['different_rows']>0 for v in values)} | {sum(v['different_rows'] for v in values)} |")
        lines+=['',f"Bridge counts are retained under suite receipt `{spec['observation']['receipt_id']}` and the hashed bridge artifact. These are changes between two knowledge/definition states, not differences between architectures. Registered row counts span distinct report populations and are not unique customer or event counts. Full changes and conservative upstream candidates are retained; the candidates do not establish minimal causal attribution.", '',
                'NRR delta in percentage points is 100 * (after ratio - before ratio).', '',
                '| Headline NRR change | Before ratio | After ratio | Delta percentage points | Before cohort customers | After cohort customers | Before / after population receipts |',
                '|---|---:|---:|---:|---:|---:|---|']
        for name,change in sorted(bridges.items()):
            nrr=change['outputs']['consumption_nrr']
            for difference in nrr['differences']:
                before,after=difference['spine'],difference['baseline']
                if before and after and before['lens']=='base_t12m':
                    value=lambda x:'NULL' if x is None else f'{x:.9f}'
                    delta=None if before['nrr'] is None or after['nrr'] is None else 100*(after['nrr']-before['nrr'])
                    lines.append(f"| {name} | {value(before['nrr'])} | {value(after['nrr'])} | {value(delta)} | {before['cohort_customers']} | {after['cohort_customers']} | `{nrr['before_receipt']}` / `{nrr['after_receipt']}` |")
    if spec.get('replay'):
        r=spec['replay'];obs=spec['observation']['receipt_id']
        if spec.get('bridges'):
            from tl.metrics.definitions import FAMILIES
            run_id=bridges['day']['after_run']
            replayed=next(run for run in runs if run['run_id']==run_id)
            kpi_rows=sum(row['spine_rows'] for row in replayed['results'] if row['output'] in FAMILIES)
            complete=r['verified_populations']==r['attempted_populations']==len(replayed['results'])
            lines+=['',f"KPI replay scope: {kpi_rows} eligible rows across the seven metric families. Exhaustive population verification complete: {complete}. Journals, close rows and membership are counted separately from these KPI rows in the total below. Scope is bound by the population receipts in run `{run_id}` and suite receipt `{obs}`."]
        lines+=['',f"Exhaustive spine replay: {r['verified_populations']}/{r['attempted_populations']} registered populations, {r['rows']} rows reproduced. This includes journals, close outputs, KPIs and membership. Evidence: `{obs}`. Baseline retained-byte verification is separate from recomputation; no unsupported baseline replay percentage is assigned.",'',
                f"Local switch and rollback: {len(spec['migrations'])} original/restated token-yield rehearsals. The endpoint hashes and comparison receipts are retained under suite observation `{obs}`. No production retirement occurred."]
    lines+=['','These are local observations, including an explicit warmup where recorded. Common snapshot selection is recorded separately and excluded from both build spans. Baseline projection and dbt tests are included; spine accounting invariants and cache builds are included. Output queries are separately timed; remaining harness work is reported only as an elapsed-time residual. OS page caching remains uncontrolled.','',
            'Restatement appends source evidence and executes dependency work. The baseline uses source-aware selection; the spine currently rebuilds all six caches. Neither path is described as zero work. The baseline NRR floor is a shared parameter; the spine selects immutable v3 instead of v2. Both execute affected queries.','',
            'Row receipts bind a complete registered output population. `tl receipt` recomputes the spine population and verifies the retained baseline bytes; it does not call that a fresh baseline rebuild. Separate comparison runs execute dbt and its tests. Timing receipts verify original observations and do not pretend to reproduce elapsed seconds.','',
            'Independent QA remains with Grok. Personal G0/U1/L1 follows the accepted local comparison. The matched-agent benchmark, `tl demo`, agent challenge, Codespaces, terminal recording and native BigQuery acceptance remain separate subsequent gates.','',
            f"Source evidence bundle: `{bundle.as_posix()}`. Bundle SHA-256: `{hashlib.sha256(bundle.read_bytes()).hexdigest()}`.",'']
    destination=Path(destination);destination.parent.mkdir(parents=True,exist_ok=True)
    destination.write_text('\n'.join(lines),encoding='utf-8',newline='\n')
    return dict(path=str(destination),sha256=hashlib.sha256(destination.read_bytes()).hexdigest())
