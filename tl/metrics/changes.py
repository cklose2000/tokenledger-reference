"""Receipt-backed comparisons of retained runs, with bounded causal re-performance.

Derived reports share the recoverable metric ledger, but have a separate run
schema. They retain dependency run IDs, not a second source of metric truth.
"""

from collections import defaultdict
from decimal import Decimal
from itertools import combinations
from pathlib import Path
import hashlib
import json
import os
import uuid

import yaml

from tl.metrics.definitions import Definition
from tl.receipts.metrics import (append_receipts, code_revision, digest, execution_artifacts,
                                input_manifest, metric_row, read_receipts, receipt_id)
from tl.scaffolds.invariants import verify
from tl.scaffolds.session import ReportSession
from tl.stream import StreamReader, ValidationError
from tl.stream.events import canonical, timestamp

POLICY = Path('definitions/derivations/v1.yaml')
SCHEMA = 'tokenledger-derived/v1'


def identity(row):
    return canonical([row['metric'], row['dimensions']])


def population(rows):
    result = {identity(row): row for row in rows}
    if len(result) != len(rows):
        raise ValidationError('duplicate comparison row identity')
    return result


def payload(row):
    return None if row is None else {k: row[k] for k in ('values', 'units', 'status')}


def delta(old, new, unit):
    if old is None or new is None:
        return dict(value=None, unit='percentage points' if unit=='ratio' else unit,
                    status='unavailable_endpoint')
    value = Decimal(str(new))-Decimal(str(old))
    if unit == 'ratio':
        value *= 100
    return dict(value=format(value, 'f'), unit='percentage points' if unit=='ratio' else unit,
                status='defined')


def snapshot(manifest, db):
    table = StreamReader(db or manifest['stream_path']).snapshot(asof=manifest['asof'],
        known_at=manifest['known_at'], watermark=manifest['watermark'], include_provenance=True)
    if input_manifest(table) != manifest['inputs']:
        raise ValidationError('comparison input stream differs from recorded run')
    return table


def physical_rows(table):
    # Temporal window columns can legitimately change after a late arrival.
    return {row['activity_id']: {k:v for k,v in row.items()
            if k not in ('activity_occurrence','activity_repeated_at')} for row in table.to_pylist()}


def candidates(old, new, db):
    left, right = physical_rows(snapshot(old, db)), physical_rows(snapshot(new, db))
    if not left.keys() <= right.keys() or any(right[k] != v for k,v in left.items()):
        raise ValidationError('restatement requires an immutable source and an expanding knowledge snapshot')
    return [right[k] for k in sorted(right.keys()-left.keys())]


def grouped(events):
    groups = defaultdict(list)
    for event in events:
        features = json.loads(event['feature_json'])
        document = (features.get('invoice_id') if event['activity']=='usage_invoiced'
                    else features.get('source_document') if event['activity']=='revenue_recognized' else None)
        key = canonical(['document',event['_source'],event['customer'],document]) if document else canonical(['event',event['activity_id']])
        groups[key].append(event['activity_id'])
    return [sorted(ids) for _,ids in sorted(groups.items())]


def computed(manifest, db, excluded):
    with ReportSession(db or manifest['stream_path'],asof=manifest['asof'],known_at=manifest['known_at'],
                       watermark=manifest['watermark'],_exclude_activity_ids=excluded) as session:
        verify(session)
        rows = []
        for name, query in manifest['queries'].items():
            definition = Definition(name,query['definition_version'],query['cuts'])
            for raw in definition.execute(session) or [dict(month=manifest['asof'],status='no_observations')]:
                rows.append(metric_row(definition,raw))
        return population(rows)


def proofs(old, new, events, db, changed):
    ids = [event['activity_id'] for event in events]
    groups = grouped(events)
    fallback = dict(status='unresolved_candidates', candidate_activity_ids=ids,
                    responsible_activity_ids=[], reason='candidate population exceeds bounded subset evaluation')
    if not events:
        return {key:dict(status='definition_change',candidate_activity_ids=[],responsible_activity_ids=[]) for key in changed}
    if old['execution_hash'] != new['execution_hash']:
        return {key:{**fallback,'reason':'execution changed; split the data and definition/code bridges'} for key in changed}
    definitions = [Definition(n,q['definition_version'],q['cuts']) for n,q in new['queries'].items()]
    if execution_artifacts(definitions) != new['execution_artifacts']:
        return {key:{**fallback,'reason':'causal evaluation requires the recorded current execution artifacts'} for key in changed}
    bound = yaml.safe_load(POLICY.read_text())['restatement']['bound']
    if len(groups)>bound:
        return {key:fallback.copy() for key in changed}
    result = {}
    invalid = []
    # Evaluate every subset in cardinality order. Document groups keep invoice
    # and recognition events together; minimality is at that explicit grain.
    for size in range(len(groups)+1):
        for selected in combinations(range(len(groups)),size):
            if len(result)==len(changed):
                break
            included = sorted(key for i in selected for key in groups[i])
            try:
                rows = computed(new,db,sorted(set(ids)-set(included)))
            except ValidationError as exc:
                invalid.append(dict(included=included,error=str(exc)))
                continue
            for key, expected in changed.items():
                if key not in result and payload(rows.get(key))==payload(expected):
                    result[key] = dict(status='proven_minimum_group_set',responsible_activity_ids=included,
                        candidate_activity_ids=ids,group_activity_ids=[groups[i] for i in selected],
                        proof_scope='minimum-cardinality sufficient valid source-document groups; not individual-event necessity')
        if len(result)==len(changed):
            break
    for key in changed:
        result.setdefault(key,{**fallback,'reason':'no valid sufficient counterfactual found'})
        result[key]['invalid_counterfactuals'] = invalid
        result[key]['evaluated_group_population'] = groups
    return result


def bridge_results(old_run, new_run, *, db=None):
    old, new = old_run['manifest'], new_run['manifest']
    if old['asof'] != new['asof']:
        raise ValidationError('restatement requires the same economic asof')
    if timestamp(new['known_at']) < timestamp(old['known_at']) or new['watermark'] < old['watermark']:
        raise ValidationError('restatement knowledge cutoff and watermark must not move backwards')
    if set(old['queries']) != set(new['queries']) or any(old['queries'][n]['cuts'] != new['queries'][n]['cuts'] for n in old['queries']):
        raise ValidationError('restatement requires matching metric families and cuts')
    events = candidates(old,new,db)
    a,b = population(old_run['rows']),population(new_run['rows'])
    keys = sorted(a.keys()|b.keys())
    changed = {k:b.get(k) for k in keys if payload(a.get(k)) != payload(b.get(k))}
    causes = proofs(old,new,events,db,changed) if changed else {}
    results = []
    counts = dict(added=0,removed=0,changed=0,unchanged=0)
    for key in keys:
        left,right = a.get(key),b.get(key)
        if key not in changed:
            counts['unchanged'] += 1
            continue
        status = 'added' if left is None else 'removed' if right is None else 'changed'
        counts[status] += 1
        source = right or left
        lv,rv = (left or {}).get('values',{}),(right or {}).get('values',{})
        lu,ru = (left or {}).get('units',{}),(right or {}).get('units',{})
        deltas = {}
        for measure in sorted(lv.keys()|rv.keys()):
            if measure in lu and measure in ru and lu[measure]!=ru[measure]:
                deltas[measure] = dict(value=None,unit=None,status='unit_changed')
            else:
                deltas[measure] = delta(lv.get(measure),rv.get(measure),ru.get(measure,lu.get(measure)))
        results.append(dict(kind='restatement_row',metric=source['metric'],dimensions=source['dimensions'],
            change=status,old=left,new=right,deltas=deltas,attribution=causes[key]))
    results.insert(0,dict(kind='restatement_summary',asof=old['asof'],counts=counts,
        original_run=old_run['run_id'],current_run=new_run['run_id'],
        old_definitions={n:q['definition_version'] for n,q in old['queries'].items()},
        new_definitions={n:q['definition_version'] for n,q in new['queries'].items()},
        newly_visible_activity_ids=[event['activity_id'] for event in events],
        method='data_and_execution' if events and old['execution_hash']!=new['execution_hash'] else 'data' if events else 'definition_or_execution'))
    return results


def sensitivity_results(run):
    policy = yaml.safe_load(POLICY.read_text())['sensitivity']
    rows = run['rows']
    if any(row['metric']!='consumption_nrr' for row in rows):
        raise ValidationError('sensitivity requires an NRR-only run')
    lenses = {row['dimensions']['lens']:row for row in rows}
    if len(lenses)!=len(rows) or set(lenses)!=set(policy['lenses']):
        raise ValidationError('sensitivity requires exactly the four registered NRR lenses')
    headline = lenses[policy['headline']]
    return [dict(kind='nrr_sensitivity',lens=lens,source=lenses[lens],headline_receipt_id=headline['receipt_id'],
        nrr_delta=delta(headline['values']['nrr'],lenses[lens]['values']['nrr'],'ratio'),
        diagnostic_scope='observed snapshot counts and amounts; history availability is not source-completeness assurance')
        for lens in policy['lenses']]


def derivation_artifacts(runs):
    return execution_artifacts([Definition(n,q['definition_version'],q['cuts'])
        for run in runs for n,q in run['manifest']['queries'].items()])


def persist(kind, runs, results, *, output_root, ledger, artifacts, context=None, attachments=None):
    from tl.metrics.engine import runtime
    from tl.receipts.archive import retain_artifacts
    if artifacts != derivation_artifacts(runs):
        raise ValidationError('execution artifacts changed during derivation')
    revision = code_revision(artifacts)
    run_id = uuid.uuid4().hex
    root = Path(output_root)/run_id
    root.mkdir(parents=True,exist_ok=False)
    policy_path=Path('definitions/controls/v1.yaml') if kind=='controls_evidence' else POLICY
    policy_hash = hashlib.sha256(policy_path.read_bytes().replace(b'\r\n',b'\n')).hexdigest()
    manifest = dict(schema_version=SCHEMA,kind=kind,run_id=run_id,dependencies=[run['run_id'] for run in runs],
        definition_version='v1',definition_sha256=policy_hash,execution_artifacts=artifacts,execution_hash=digest(artifacts),
        query_hash=digest(dict(kind=kind,policy_sha256=policy_hash,execution_hash=digest(artifacts))),
        inputs=[run['manifest']['inputs'] for run in runs],asof=runs[-1]['manifest']['asof'],
        known_at=runs[-1]['manifest']['known_at'],watermark=runs[-1]['manifest']['watermark'],
        stream_path=runs[-1]['manifest']['stream_path'],runtime=runtime(),row_count=len(results),**revision)
    retain_artifacts(artifacts,root/'artifacts')
    if context is not None:
        manifest['context']=context
        manifest['attachments']={name:hashlib.sha256(raw).hexdigest() for name,raw in (attachments or {}).items()}
        objects=root/'objects';objects.mkdir()
        for raw in (attachments or {}).values():
            path=objects/hashlib.sha256(raw).hexdigest()
            if not path.exists():
                path.write_bytes(raw)
    raw = (canonical(manifest)+'\n').encode()
    (root/'run.json').write_bytes(raw)
    records=[]
    for result in results:
        record = {key:manifest[key] for key in ('run_id','definition_version','definition_sha256','query_hash','inputs',
                  'git_sha','execution_hash','asof','known_at','watermark')}
        record.update(schema_version=SCHEMA,run_manifest_sha256=hashlib.sha256(raw).hexdigest(),result=result)
        record['receipt_id']=receipt_id(record)
        records.append(record)
    rows = [{**record['result'],'receipt_id':record['receipt_id']} for record in records]
    (root/'rows.jsonl').write_text(''.join(canonical(row)+'\n' for row in rows),encoding='utf-8',newline='\n')
    if kind!='controls_evidence':
        render(root,rows,manifest)
    if artifacts != derivation_artifacts(runs):
        raise ValidationError('execution artifacts changed during derivation')
    append_receipts(ledger,records)
    return dict(run_id=run_id,run_directory=str(root),manifest=manifest,rows=rows)


def render(root, rows, manifest):
    import csv
    flattened=[]
    for row in rows:
        if row['kind']=='nrr_sensitivity':
            source=row['source']
            flattened.append(dict(identity=row['lens'],status=source['status'],values=canonical(source['values']),
                deltas=canonical(row['nrr_delta']),receipt_id=row['receipt_id']))
        else:
            flattened.append(dict(identity=canonical(row.get('dimensions',{'summary':True})),
                status=row.get('change','summary'),values=canonical(row.get('counts',dict(old=row.get('old'),new=row.get('new')))),
                deltas=canonical(row.get('deltas',{})),receipt_id=row['receipt_id']))
    with (root/'report.csv').open('x',encoding='utf-8',newline='') as handle:
        writer=csv.DictWriter(handle,fieldnames=['identity','status','values','deltas','receipt_id'])
        writer.writeheader(); writer.writerows(flattened)
    lines=[f'# {manifest["kind"]}', '', 'Synthetic. Delta ratios use percentage points; money uses USD. Null means unavailable.',
           'Counts and amounts remain observed-snapshot diagnostics when history is unavailable.', '',
           '| Row | Status | Values | Deltas | Receipt |','|---|---|---|---|---|']
    for row in flattened:
        lines.append('| '+' | '.join(str(v).replace('|','\\|').replace('\n',' ') for v in row.values())+' |')
    (root/'report.md').write_text('\n'.join(lines)+'\n',encoding='utf-8',newline='\n')


def restate(original_run, current_run, *, db=None, output_root=Path('metrics/out'), ledger=Path('ledger/metrics.jsonl')):
    from tl.metrics.engine import read_run, replay_receipts
    runs=[read_run(key,output_root=output_root,ledger=ledger) for key in (original_run,current_run)]
    if any(run['manifest']['schema_version']!='tokenledger-run/v1' for run in runs):
        raise ValidationError('restatement requires metric runs, not another derived report')
    artifacts=derivation_artifacts(runs)
    # Re-perform both entire populations before deriving a disclosure bridge.
    replay_receipts(list(dict.fromkeys(row['receipt_id'] for run in runs for row in run['rows'])),
                    db=db,output_root=output_root,ledger=ledger)
    results=bridge_results(*runs,db=db)
    return persist('restatement',runs,results,output_root=output_root,ledger=ledger,artifacts=artifacts)


def sensitivity(run_id, *, db=None, output_root=Path('metrics/out'), ledger=Path('ledger/metrics.jsonl')):
    from tl.metrics.engine import read_run,replay_receipts
    run=read_run(run_id,output_root=output_root,ledger=ledger)
    if run['manifest']['schema_version']!='tokenledger-run/v1':
        raise ValidationError('sensitivity requires a metric run')
    artifacts=derivation_artifacts([run])
    replay_receipts([row['receipt_id'] for row in run['rows']],db=db,output_root=output_root,ledger=ledger)
    return persist('nrr_sensitivity',[run],sensitivity_results(run),output_root=output_root,ledger=ledger,artifacts=artifacts)


def read_derived(run_id, *, output_root, ledger):
    root=Path(output_root)/run_id
    raw=(root/'run.json').read_bytes()
    manifest=json.loads(raw)
    rows=[json.loads(line) for line in (root/'rows.jsonl').read_text(encoding='utf-8').splitlines()]
    records=read_receipts(ledger)
    if manifest.get('schema_version')!=SCHEMA or manifest['run_id']!=run_id or len(rows)!=manifest['row_count'] or len({r['receipt_id'] for r in rows})!=len(rows):
        raise ValidationError('derived run population or identity mismatch')
    for row in rows:
        record=records.get(row['receipt_id'],{})
        expected={k:manifest[k] for k in ('run_id','definition_version','definition_sha256','query_hash','inputs','git_sha',
                                         'execution_hash','asof','known_at','watermark')}
        expected.update(schema_version=SCHEMA,run_manifest_sha256=hashlib.sha256(raw).hexdigest(),
                        result={k:v for k,v in row.items() if k!='receipt_id'})
        if any(record.get(k)!=v for k,v in expected.items()):
            raise ValidationError('derived result or contract differs from its receipt')
    return dict(run_id=run_id,run_directory=str(root),manifest=manifest,rows=rows)


def replay_derived(ids, *, db=None, output_root, ledger, python=None):
    from tl.metrics.engine import read_run,replay_receipts,runtime
    records=read_receipts(ledger)
    run_id=records[ids[0]]['run_id']
    run=read_derived(run_id,output_root=output_root,ledger=ledger)
    manifest=run['manifest']
    if not set(ids)<={r['receipt_id'] for r in run['rows']}:
        raise ValidationError('derived receipt is not in its run')
    matches=all(Path(p).is_file() and hashlib.sha256(Path(p).read_bytes().replace(b'\r\n',b'\n')).hexdigest()==h
                for p,h in manifest['execution_artifacts'].items())
    if python or runtime()!=manifest['runtime'] or not matches:
        if manifest['execution_hash'] in json.loads(os.environ.get('TL_REPLAY_STACK','[]')):
            raise ValidationError('historical derived runtime or artifacts differ')
        from tl.receipts.archive import historical_replay
        return historical_replay(ids,manifest=manifest,db=db or manifest['stream_path'],
                                 output_root=output_root,ledger=ledger,python=python)
    runs=[read_run(key,output_root=output_root,ledger=ledger) for key in manifest['dependencies']]
    if any(r['manifest']['schema_version']!= 'tokenledger-run/v1' for r in runs):
        raise ValidationError('derived reports must depend directly on metric runs')
    replay_receipts(list(dict.fromkeys(row['receipt_id'] for dep in runs for row in dep['rows'])),
                    db=db,output_root=output_root,ledger=ledger)
    if manifest['kind']=='restatement' and len(runs)==2:
        expected=bridge_results(*runs,db=db)
    elif manifest['kind']=='nrr_sensitivity' and len(runs)==1:
        expected=sensitivity_results(runs[0])
    elif manifest['kind']=='controls_evidence' and len(runs)==1:
        from tl.controls.evidence import reproduce
        expected=reproduce(run,runs[0],db=db)
    else:
        raise ValidationError('unsupported derivation contract')
    if expected != [{k:v for k,v in row.items() if k!='receipt_id'} for row in run['rows']]:
        raise ValidationError('derived receipt reproduction failed')
    return [dict(receipt_id=key,verified=True,result=records[key]['result']) for key in ids]
