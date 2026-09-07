"""Immutable request-list bundles. Collection is distinct from control testing."""
from datetime import date,datetime,timedelta,timezone
import hashlib
import json
from pathlib import Path
import re
import shutil
from tempfile import TemporaryDirectory
import uuid
import yaml

from tl.controls import sources
from tl.controls.matrix import check_definitions
from tl.controls.reconciliation import bridge
from tl.metrics.changes import derivation_artifacts,persist,snapshot
from tl.metrics.engine import read_run,replay_receipts
from tl.metrics.pack import pack_run
from tl.receipts.archive import load_pack
from tl.receipts.metrics import read_receipts
from tl.scaffolds.session import ReportSession
from tl.stream import ValidationError
from tl.stream.events import canonical
from tl.stream.events import timestamp


def encoded(value):
    return (canonical(value)+'\n').encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def period_bounds(period):
    match=re.fullmatch(r'(\d{4})Q([1-4])',period)
    if not match:
        raise ValidationError('period must be YYYYQ1 through YYYYQ4')
    year,quarter=map(int,match.groups());month=(quarter-1)*3+1
    start=date(year,month,1);end=date(year+1,1,1) if quarter==4 else date(year,month+3,1)
    return start.isoformat(),end.isoformat()


def pack_check(files,run):
    with TemporaryDirectory(prefix='tl-evidence-pack-') as temp:
        root=Path(temp);saved=root/'saved';saved.mkdir()
        for key,raw in files.items():
            name=key.removeprefix('pack/')
            if Path(name).name!=name or name in ('.','..'):
                raise ValidationError('unsafe pack artifact name')
            (saved/name).write_bytes(raw)
        loaded=load_pack(saved,root/'loaded')
        if loaded['manifest']!=run['manifest'] or loaded['rows']!=run['rows']:
            raise ValidationError('pack differs from selected receipt population')
        result=pack_run(run['run_id'],output_root=root/'loaded/runs',ledger=root/'loaded/receipts.jsonl',pack_root=root/'rendered')
        regenerated=Path(result['path'])
        expected={p.name:p.read_bytes() for p in regenerated.iterdir()}
        actual={p.name:p.read_bytes() for p in saved.iterdir()}
        if expected!=actual:
            raise ValidationError('pack presentation differs from canonical receipt rendering')
    return dict(passed=True,receipt_ids=[r['receipt_id'] for r in run['rows']],
                value_slots=sum(len(r['values']) for r in run['rows']),
                null_slots=sum(v is None for r in run['rows'] for v in r['values'].values()))


def evaluate(run,context,attachments,*,db=None):
    start,end=period_bounds(context['period'])
    manifest=run['manifest']
    if manifest['asof']!=(date.fromisoformat(end)-timedelta(days=1)).isoformat():
        raise ValidationError('evidence period must end at the selected metric asof')
    table=snapshot(manifest,db)
    captures=[]
    with TemporaryDirectory(prefix='tl-evidence-source-') as temp:
        for name,raw in attachments.items():
            if re.fullmatch(r'capture/\d+/manifest.json',name):
                folder=Path(temp)/name.split('/')[1];folder.mkdir()
                (folder/'manifest.json').write_bytes(raw)
                (folder/'source.jsonl').write_bytes(attachments[name.replace('manifest.json','source.jsonl')])
                captures.append(sources.load_capture(folder/'manifest.json'))
    checks={}
    checks['KPI-01']=sources.reconcile(table,captures,start=start,end=end)
    if not captures:
        checks['KPI-01'].update(passed=None,status='not_run')
    if 'erp.csv' in attachments and 'erp-manifest.json' in attachments:
        with ReportSession(db or manifest['stream_path'],asof=manifest['asof'],known_at=manifest['known_at'],watermark=manifest['watermark']) as session:
            checks['KPI-06']=bridge(session,attachments['erp.csv'],json.loads(attachments['erp-manifest.json']),start=start,end=end)
    if 'pack/manifest.json' in attachments:
        checks['KPI-08']=pack_check({k:v for k,v in attachments.items() if k.startswith('pack/')},run)
        checks['KPI-04']=dict(passed=True,verified_receipt_ids=[r['receipt_id'] for r in run['rows']],
            row_count=len(run['rows']),value_slots=sum(len(r['values']) for r in run['rows']),
            scope='complete selected disclosure population, including explicit nulls; technical re-performance')
    visible=set(table['activity_id'].to_pylist())
    late=[dict(activity_id=obs['activity_id'],source=capture['source'],recorded_at=obs['recorded_at'],visible=obs['activity_id'] in visible)
          for capture in captures for obs in capture['observations'] if obs['status']=='accepted'
          and start<=obs['event']['ts'][:10]<end and timestamp(obs['recorded_at'])>=timestamp(end+'T00:00:00Z')]
    checks['KPI-05']=dict(passed=True,asof=manifest['asof'],known_at=manifest['known_at'],watermark=manifest['watermark'],
        late_arrivals=sorted(late,key=lambda row:(row['source'],row['activity_id'])),source_capture_available=bool(captures),
        scope='frozen snapshot replay and late arrivals in retained source captures only; independent close review remains not_run')
    history=json.loads(attachments['definition-change-history.json'])
    checks['KPI-03']=dict(passed=history['status']=='passed',status=history['status'],violations=history['violations'],
                        scope='retained git comparison, not independent approval; bytes are retained evidence, not a new git-history verification')
    externals=context.get('external',{})
    for name,description in externals.items():
        if description.get('period')!=context['period'] or description.get('git_sha')!=manifest['git_sha']:
            checks['external:'+name]=dict(passed=False,reason='stale or mismatched period/revision')
    return [dict(kind='control_test',control=key,result=value) for key,value in sorted(checks.items())]


def reproduce(derived,run,*,db=None):
    manifest=derived['manifest'];attachments={}
    for name,expected in manifest['attachments'].items():
        if not re.fullmatch('[0-9a-f]{64}',expected):
            raise ValidationError('invalid evidence object hash')
        raw=(Path(derived['run_directory'])/'objects'/expected).read_bytes()
        if sha(raw)!=expected:
            raise ValidationError('evidence object hash mismatch')
        attachments[name]=raw
    return evaluate(run,manifest['context'],attachments,db=db)


def collect(period,*,run_id=None,db=None,pack=None,source_manifests=(),erp=None,erp_manifest=None,external=None,
            output_root=Path('metrics/out'),ledger=Path('ledger/metrics.jsonl'),evidence_root=Path('docs/evidence')):
    period_bounds(period)
    matrix_raw=Path('controls/matrix.yaml').read_bytes();matrix=yaml.safe_load(matrix_raw)
    attachments={'matrix.yaml':matrix_raw,'definition-change-history.json':encoded(check_definitions())}
    context=dict(period=period,captured_at=datetime.now(timezone.utc).isoformat(),
                 scope='synthetic local evidence; operating effectiveness and audit opinion not asserted',external={})
    required={name for c in matrix['controls'] for name in c['evidence_artifact']}
    if external:
        for name,descriptor in json.loads(Path(external).read_bytes()).items():
            if name not in required:
                raise ValidationError('unknown matrix artifact')
            raw=Path(descriptor['path']).read_bytes()
            if sha(raw)!=descriptor['sha256']:
                raise ValidationError('external evidence hash mismatch')
            attachments['external/'+name]=raw
            context['external'][name]={k:v for k,v in descriptor.items() if k!='path'}
    for index,path in enumerate(source_manifests):
        sources.load_capture(path)
        attachments[f'capture/{index}/manifest.json']=Path(path).read_bytes()
        attachments[f'capture/{index}/source.jsonl']=(Path(path).parent/'source.jsonl').read_bytes()
    if erp:
        attachments['erp.csv']=Path(erp).read_bytes()
    if erp_manifest:
        attachments['erp-manifest.json']=Path(erp_manifest).read_bytes()
    if bool(erp)!=bool(erp_manifest):
        raise ValidationError('ERP requires both export bytes and origin manifest')
    if pack:
        attachments.update({'pack/'+p.name:p.read_bytes() for p in Path(pack).iterdir() if p.is_file()})
    rows=[];records={};run=None
    if run_id:
        run=read_run(run_id,output_root=output_root,ledger=ledger)
        if run['manifest']['schema_version']!='tokenledger-run/v1':
            raise ValidationError('evidence requires a metric run')
        artifacts=derivation_artifacts([run])
        replay_receipts([r['receipt_id'] for r in run['rows']],db=db,output_root=output_root,ledger=ledger)
        # Links are projections of existing canonical receipts, never a second ledger.
        records=read_receipts(ledger)
        related=[record for record in records.values() if record.get('schema_version')=='tokenledger-derived/v1'
                 and record.get('result',{}).get('kind')=='restatement_summary'
                 and run_id in (record['result'].get('original_run'),record['result'].get('current_run'))]
        if related:
            related_runs={record['run_id'] for record in related}
            attachments['restatements.jsonl']=b''.join(encoded(record) for record in records.values() if record['run_id'] in related_runs)
        notes=[]
        for name,query in run['manifest']['queries'].items():
            from tl.metrics.definitions import Definition
            notes.append(name+': '+str(Definition(name,query['definition_version']).spec['disclosure_note']))
        attachments['disclosure-notes.md']=('\n\n'.join(notes)+'\n').encode()
        if Path('ledger/process.jsonl').is_file():
            attachments['process-receipts.jsonl']=Path('ledger/process.jsonl').read_bytes()
        results=evaluate(run,context,attachments,db=db)
        derived=persist('controls_evidence',[run],results,output_root=output_root,ledger=ledger,artifacts=artifacts,
                        context=context,attachments=attachments)
        rows=derived['rows'];bundle_id=derived['run_id'];records=read_receipts(ledger)
    else:
        bundle_id=uuid.uuid4().hex
    root=Path(evidence_root)/period/bundle_id;root.mkdir(parents=True,exist_ok=False)
    objects=root/'objects';objects.mkdir()
    def put(raw):
        key=sha(raw);path=objects/key
        if not path.exists():
            path.write_bytes(raw)
        return dict(path='objects/'+key,sha256=key)
    raw_objects={name:put(raw) for name,raw in attachments.items()}
    by_control={row['control']:row for row in rows}
    generated={}
    def artifact(name,raw,control,origin):
        row=by_control.get(control,{})
        tested=row.get('result',{})
        test_status='not_run' if tested.get('status')=='not_run' or tested.get('passed') is None else 'passed' if tested['passed'] else 'failed'
        generated[name]=dict(**put(raw),origin=origin,capture_method='tl evidence',captured_at=context['captured_at'],
                             coverage_period=period,collection_status='collected',test_status=test_status,
                             receipt_id=row.get('receipt_id'),scope=context['scope'])
    for control,row in by_control.items():
        result=row['result'];receipt=encoded(records[row['receipt_id']])
        if control=='KPI-01':
            if source_manifests:
                artifact('source-manifests.json',encoded({k:v for k,v in raw_objects.items() if k.startswith('capture/')}),control,'pre-ingestion captures')
            artifact('source-reconciliation.json',encoded(result),control,'snapshot compared with captured observations')
            artifact('source-reconciliation.receipt.json',receipt,control,'shared metric receipt ledger')
        if control=='KPI-04':
            artifact('disclosed-population.json',encoded(result['verified_receipt_ids']),control,'selected pack population')
            artifact('receipt-reperformance.jsonl',b''.join(encoded(dict(receipt_id=r,verified=True)) for r in result['verified_receipt_ids']),control,'exhaustive shared-engine replay')
            artifact('reperformance-summary.receipt.json',receipt,control,'shared metric receipt ledger')
        if control=='KPI-05':
            artifact('close-cutoff-manifest.json',encoded(run['manifest']),control,'retained metric run')
            artifact('cutoff.receipt.json',receipt,control,'shared metric receipt ledger')
            if result['source_capture_available']:
                artifact('late-arrival-report.json',encoded(result),control,'retained pre-ingestion observations compared with frozen visibility')
        if control=='KPI-06':
            import csv,io
            handle=io.StringIO(newline='');writer=csv.DictWriter(handle,fieldnames=['period','account','source','net_cents','fee_addback_cents','erp_cents','delta_cents','activity_ids'])
            writer.writeheader();writer.writerows(result['rows'])
            artifact('gaap-revenue-bridge.csv',handle.getvalue().encode(),control,'distinct recognition entries plus once-per-invoice fees')
            artifact('erp-export-manifest.json',attachments['erp-manifest.json'],control,'separately authored synthetic ERP capture')
            artifact('gaap-tieout.receipt.json',receipt,control,'shared metric receipt ledger')
        if control=='KPI-08':
            artifact('pack-manifest.json',attachments['pack/manifest.json'],control,'retained selected pack')
            artifact('pack-hash-verification.json',encoded(result),control,'canonical regeneration and byte comparison')
            artifact('disclosure-value-map.json',encoded(run['rows']),control,'verified receipt-bearing rows')
    if run:
        artifact('definition-change-history.json',attachments['definition-change-history.json'],'KPI-03','git checkpoint comparison')
        artifact('definition-hashes.json',encoded(run['manifest']['queries']),'KPI-03','retained metric run')
        artifact('disclosure-notes.md',attachments['disclosure-notes.md'],'KPI-03','selected versioned definitions')
        for name in ('restatements.jsonl','process-receipts.jsonl'):
            if name in attachments:
                generated[name]=dict(**put(attachments[name]),origin='retained canonical ledger projection',collection_status='collected',
                                     test_status='not_run',reason='collected receipt links or actor claims require release/close review')
    controls=[]
    for control in matrix['controls']:
        items={}
        for name in control['evidence_artifact']:
            if name in generated:
                item=generated[name]
                if 'external:'+name in by_control:
                    item={**item,'test_status':'failed','reason':'conflicting external evidence has stale scope/revision'}
            elif 'external/'+name in raw_objects:
                stale='external:'+name in by_control
                item=dict(**raw_objects['external/'+name],origin=context['external'][name],collection_status='collected',
                          test_status='failed' if stale else 'not_run',reason='stale scope/revision' if stale else 'declared bytes require independent verification')
            else:
                item=dict(collection_status='missing',test_status='not_run',reason='required evidence not supplied or automated')
            items[name]=item
        tests=[v['test_status'] for v in items.values()]
        status='failed' if 'failed' in tests else 'not_run' if 'not_run' in tests else 'passed'
        controls.append(dict(id=control['id'],test_status=status,operating_effectiveness='not_assessed',artifacts=items))
    status='failed' if any(c['test_status']=='failed' for c in controls) else 'incomplete' if any(c['test_status']!='passed' for c in controls) else 'passed'
    if run:
        for name in ('run.json','rows.jsonl'):
            shutil.copyfile(Path(derived['run_directory'])/name,root/name)
        (root/'receipts.jsonl').write_bytes(b''.join(encoded(records[r['receipt_id']]) for r in rows))
    manifest=dict(schema_version='tokenledger-evidence/v1',run_id=bundle_id,period=period,status=status,scope=context['scope'],
        metric_run_id=run_id,captured_at=context['captured_at'],matrix_version=matrix['version'],matrix_sha256=sha(matrix_raw),controls=controls,
        captured_objects=raw_objects,files={p.relative_to(root).as_posix():sha(p.read_bytes()) for p in root.rglob('*') if p.is_file()})
    raw=encoded(manifest);(root/'manifest.json').write_bytes(raw)
    return dict(status=status,exit_code=1 if status=='failed' else 2 if status=='incomplete' else 0,path=str(root),
                manifest_sha256=sha(raw),receipt_ids=[r['receipt_id'] for r in rows])


def verify_bundle(path,expected):
    root=Path(path);raw=(root/'manifest.json').read_bytes()
    if sha(raw)!=expected:
        raise ValidationError('evidence manifest hash mismatch')
    manifest=json.loads(raw)
    for name,value in manifest['files'].items():
        target=(root/name).resolve()
        if not target.is_relative_to(root.resolve()) or sha(target.read_bytes())!=value:
            raise ValidationError('evidence artifact hash mismatch')
    return dict(status='verified',recomputed=False,control_status=manifest['status'],manifest_sha256=expected)
