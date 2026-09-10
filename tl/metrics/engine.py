"""Execute all disclosure queries before persisting any receipt-bearing result."""

from collections import defaultdict
import hashlib
import json
from pathlib import Path
import re
import uuid
import platform
import os

import duckdb
import pyarrow
import yaml

from tl.metrics.definitions import Definition,FAMILIES
from tl.receipts.metrics import (append_receipts,code_revision,digest,execution_artifacts,input_manifest,
                                metric_row,read_receipts,receipt_id)
from tl.scaffolds.session import ReportSession
from tl.scaffolds.invariants import verify
from tl.stream.events import ValidationError,canonical


def runtime():
    return dict(python=platform.python_version(),duckdb=duckdb.__version__,pyarrow=pyarrow.__version__,pyyaml=yaml.__version__)


def run_metrics(db,*,asof,known_at=None,watermark=None,names=FAMILIES,version=None,cuts=None,
                output_root=Path('metrics/out'),ledger=Path('ledger/metrics.jsonl'),application=None,session_options=None):
    if session_options and application is None:
        raise ValidationError('session options require an application context')
    if application is not None:
        application.check_roots(db,output_root,ledger)
    registry=None if application is None else application.registry()
    definitions=[Definition(name,version,cuts,registry=registry) for name in names]
    artifacts_for=execution_artifacts if application is None else application.artifacts
    artifacts=artifacts_for(definitions)
    revision=code_revision(artifacts)
    run_id=uuid.uuid4().hex
    root=Path(output_root)/run_id
    opened=(ReportSession(db,asof=asof,known_at=known_at,watermark=watermark) if application is None else
            application.session(asof=asof,known_at=known_at,watermark=watermark,**(session_options or {})))
    with opened as session:
        invariants=verify(session) if application is None else session.verify()
        results=[]
        query_contracts={}
        for definition in definitions:
            raw=definition.execute(session)
            if not raw:
                raw=[dict(month=asof,status='no_observations') if application is None else dict(status='no_observations')]
            rows=[metric_row(definition,row) for row in raw]
            query_contracts[definition.name]=dict(definition_version=definition.version,definition_sha256=definition.digest,
                sql=definition.sql,parameters=definition.parameters,cuts=definition.cuts,
                query_hash=digest(dict(sql=definition.sql,parameters=definition.parameters,scaffolds=session.sql)))
            results.extend(rows)
        if artifacts!=artifacts_for(definitions):
            raise ValidationError('execution artifacts changed during metric run')
        root.mkdir(parents=True,exist_ok=False)
        from tl.receipts.archive import retain_artifacts
        retain_artifacts(artifacts,root/'artifacts')
        inputs=input_manifest(session.snapshot,root/'inputs.jsonl')
        manifest=dict(schema_version='tokenledger-run/v1',run_id=run_id,synthetic=application is None,
                stream_path=str(Path(db).resolve()),**session.build_manifest(),**revision,
                execution_artifacts=artifacts,execution_hash=digest(artifacts),inputs=inputs,
            queries=query_contracts,invariants=invariants,row_count=len(results),runtime=runtime())
    manifest_bytes=(canonical(manifest)+'\n').encode()
    manifest_hash=hashlib.sha256(manifest_bytes).hexdigest()
    (root/'run.json').write_bytes(manifest_bytes)
    records=[]
    rows=[]
    for result in results:
        query=query_contracts[result['metric']]
        record=dict(schema_version='tokenledger-metric/v1',run_id=run_id,result=result,
            definition_version=query['definition_version'],definition_sha256=query['definition_sha256'],
            inputs=inputs,query_hash=query['query_hash'],git_sha=revision['git_sha'],execution_hash=manifest['execution_hash'],
            asof=manifest['asof'],known_at=manifest['known_at'],watermark=manifest['watermark'],
            run_manifest_sha256=manifest_hash)
        record['receipt_id']=receipt_id(record)
        records.append(record)
        rows.append({**result,'receipt_id':record['receipt_id']})
    if len({row['receipt_id'] for row in rows})!=len(rows):
        raise ValidationError('duplicate metric row identity')
    (root/'rows.jsonl').write_text(''.join(canonical(row)+'\n' for row in rows),encoding='utf-8',newline='\n')
    append_receipts(ledger,records)
    return dict(run_id=run_id,run_directory=str(root),manifest=manifest,rows=rows)


def read_run(run_id,*,output_root=Path('metrics/out'),ledger=Path('ledger/metrics.jsonl'),application=None):
    if not re.fullmatch(r'[0-9a-f]{32}',run_id):
        raise ValidationError('invalid run id')
    root=Path(output_root)/run_id
    content=(root/'run.json').read_bytes()
    manifest=json.loads(content)
    if manifest.get('application') or manifest.get('synthetic') is False:
        if (application is None or manifest.get('application')!=application.name
                or manifest.get('binding_id')!=application.binding.identity):
            raise ValidationError('run requires its matching explicit application context')
        application.check_roots(application.db,output_root,ledger)
    elif application is not None:
        raise ValidationError('synthetic run cannot use a private application context')
    if manifest.get('schema_version')=='tokenledger-derived/v1':
        from tl.metrics.changes import read_derived
        return read_derived(run_id,output_root=output_root,ledger=ledger)
    if manifest.get('run_id')!=run_id:
        raise ValidationError('run identity mismatch')
    manifest_hash=hashlib.sha256(content).hexdigest()
    records=read_receipts(ledger)
    rows=[json.loads(line) for line in (root/'rows.jsonl').read_text(encoding='utf-8').splitlines()]
    if len(rows)!=manifest['row_count'] or len({row['receipt_id'] for row in rows})!=len(rows):
        raise ValidationError('run row population differs from manifest')
    for row in rows:
        record=records.get(row['receipt_id'])
        if record is None or record['run_manifest_sha256']!=manifest_hash or record['run_id']!=run_id:
            raise ValidationError('missing or mismatched metric receipt')
        if record['result']!={key:value for key,value in row.items() if key!='receipt_id'}:
            raise ValidationError('metric result differs from its receipt')
        query=manifest['queries'][row['metric']]
        expected=dict(definition_version=query['definition_version'],definition_sha256=query['definition_sha256'],
            query_hash=query['query_hash'],inputs=manifest['inputs'],execution_hash=manifest['execution_hash'],
            git_sha=manifest['git_sha'],asof=manifest['asof'],known_at=manifest['known_at'],watermark=manifest['watermark'])
        if any(record.get(key)!=value for key,value in expected.items()):
            raise ValidationError('receipt contract differs from its run manifest')
    if hashlib.sha256((root/'inputs.jsonl').read_bytes()).hexdigest()!=manifest['inputs']['sha256']:
        raise ValidationError('input manifest hash mismatch')
    return dict(run_id=run_id,run_directory=str(root),manifest=manifest,rows=rows)


def replay_receipts(ids,*,db=None,output_root=Path('metrics/out'),ledger=Path('ledger/metrics.jsonl'),python=None,application=None,cloud_binding=None):
    if application is not None:
        application.check_roots(db or application.db,output_root,ledger)
    records=read_receipts(ledger)
    groups=defaultdict(list)
    for key in ids:
        if key not in records:
            raise ValidationError(f'unknown receipt: {key}')
        groups[records[key]['run_id']].append(records[key])
    verified=[]
    for run_id,selected in groups.items():
        if selected[0].get('schema_version') in ('tokenledger-reporting-learning-evaluation/v1',
                                                'tokenledger-reporting-learning-observation/v1'):
            from tl.evolution.evidence import replay
            verified.extend(replay([r['receipt_id'] for r in selected], output_root=output_root, ledger=ledger))
            continue
        if selected[0].get('schema_version')=='tokenledger-reporting-learning-walkthrough/v1':
            from tl.evolution.walkthrough import replay_evaluation
            verified.extend(replay_evaluation(r['receipt_id'],output_root=output_root,ledger=ledger) for r in selected)
            continue
        if selected[0].get('schema_version') in ('tokenledger-learning-trial/v1','tokenledger-learning-trial-observation/v1',
                                               'tokenledger-learning-authorization/v1'):
            from tl.learning.runner import replay
            verified.extend(replay([r['receipt_id'] for r in selected],application=application,
                output_root=output_root,ledger=ledger,python=python))
            continue
        if selected[0].get('schema_version') in ('tokenledger-learning/v1','tokenledger-learning-observation/v1'):
            from tl.learning.evaluation import replay
            verified.extend(replay([r['receipt_id'] for r in selected],application=application,
                output_root=output_root,ledger=ledger,python=python))
            continue
        if application is not None and any(r.get('schema_version')!='tokenledger-metric/v1' for r in selected):
            raise ValidationError('receipt is not registered for the private application')
        if selected[0].get('schema_version')=='tokenledger-challenge/v1':
            from tl.challenge.engine import replay
            verified.extend(replay([r['receipt_id'] for r in selected],db=db,
                output_root=output_root,ledger=ledger,python=python))
            continue
        if selected[0].get('schema_version') in ('tokenledger-challenge-score/v1','tokenledger-challenge-score-observation/v1'):
            from tl.challenge.grade import replay
            verified.extend(replay([r['receipt_id'] for r in selected],db=db,
                output_root=output_root,ledger=ledger,python=python))
            continue
        if selected[0].get('schema_version')=='tokenledger-demo-yield/v1':
            from tl.demo.yield_bridge import replay
            verified.extend(replay([r['receipt_id'] for r in selected],db=db,
                output_root=output_root,ledger=ledger,python=python))
            continue
        if selected[0].get('schema_version') in ('tokenledger-bigquery/v1','tokenledger-bigquery-observation/v1','tokenledger-bigquery-suite-observation/v1'):
            if cloud_binding is None:
                raise ValidationError('native cloud replay requires an explicit --bigquery-config binding')
            from tl.bigquery.engine import replay
            verified.extend(replay(cloud_binding,[r['receipt_id'] for r in selected],output_root=output_root,ledger=ledger))
            continue
        if selected[0].get('schema_version')=='tokenledger-demo-bridge/v1':
            from tl.demo.bridge import replay
            verified.extend(replay([r['receipt_id'] for r in selected],db=db,
                output_root=output_root,ledger=ledger,python=python))
            continue
        if selected[0].get('schema_version')=='tokenledger-demo-observation/v1':
            from tl.demo.engine import replay_observations
            verified.extend(replay_observations([r['receipt_id'] for r in selected],ledger=ledger))
            continue
        if selected[0].get('schema_version')=='tokenledger-views-suite-observation/v1':
            verified.extend(dict(receipt_id=r['receipt_id'],verified=True,recomputed=False,result=r['result']) for r in selected)
            continue
        if selected[0].get('schema_version') in ('tokenledger-views-comparison/v1','tokenledger-views-comparison-observation/v1'):
            from tl.views.benchmark import replay
            verified.extend(replay([record['receipt_id'] for record in selected],db=db,
                                   output_root=output_root,ledger=ledger,python=python))
            continue
        if selected[0].get('schema_version') in ('tokenledger-views/v1','tokenledger-views-observation/v1'):
            from tl.views.engine import replay
            verified.extend(replay([record['receipt_id'] for record in selected],db=db,
                                   output_root=output_root,ledger=ledger,python=python))
            continue
        if selected[0].get('schema_version') in ('tokenledger-comparison/v1','tokenledger-comparison-observation/v1'):
            from tl.compare.engine import replay
            verified.extend(replay([record['receipt_id'] for record in selected],db=db,
                                   output_root=output_root,ledger=ledger,python=python))
            continue
        if selected[0].get('schema_version')=='tokenledger-derived/v1':
            from tl.metrics.changes import replay_derived
            verified.extend(replay_derived([record['receipt_id'] for record in selected],db=db,
                                          output_root=output_root,ledger=ledger,python=python))
            continue
        run=read_run(run_id,output_root=output_root,ledger=ledger,application=application)
        manifest=run['manifest']
        if manifest.get('application'):
            if (application is None or manifest['application']!=application.name
                    or manifest['binding_id']!=application.binding.identity):
                raise ValidationError('receipt requires its matching explicit application context')
        elif application is not None:
            raise ValidationError('synthetic receipt cannot use a private application context')
        if not {record['receipt_id'] for record in selected}<={row['receipt_id'] for row in run['rows']}:
            raise ValidationError('receipt is not part of the recorded run population')
        try:
            registry=None if application is None else application.registry()
            definitions=[Definition(name,query['definition_version'],query['cuts'],registry=registry) for name,query in manifest['queries'].items()]
            artifacts_for=execution_artifacts if application is None else application.artifacts
            matches=artifacts_for(definitions)==manifest['execution_artifacts']
        except (OSError,ValueError,KeyError):
            matches=False
        if python or runtime()!=manifest['runtime'] or not matches:
            if manifest['execution_hash'] in json.loads(os.environ.get('TL_REPLAY_STACK','[]')):
                raise ValidationError('historical runtime or artifacts differ; restore the recorded Python and pinned dependencies')
            from tl.receipts.archive import historical_replay
            verified.extend(historical_replay([record['receipt_id'] for record in selected],manifest=manifest,
                db=db or manifest['stream_path'],output_root=output_root,ledger=ledger,python=python,
                application_config=None if application is None else application.config))
            continue
        opened=(ReportSession(db or manifest['stream_path'],asof=manifest['asof'],known_at=manifest['known_at'],watermark=manifest['watermark'])
                if application is None else application.session(asof=manifest['asof'],known_at=manifest['known_at'],watermark=manifest['watermark'],**manifest.get('session_options',{})))
        with opened as session:
            if input_manifest(session.snapshot)!=manifest['inputs']:
                raise ValidationError('receipt input stream differs from original snapshot')
            verify(session) if application is None else session.verify()
            computed={}
            needed={row['result']['metric'] for row in selected}
            for definition in definitions:
                if definition.name in needed:
                    query=manifest['queries'][definition.name]
                    actual=digest(dict(sql=definition.sql,parameters=definition.parameters,scaffolds=session.sql))
                    if actual!=query['query_hash']:
                        raise ValidationError('receipt query hash mismatch')
                    raw=definition.execute(session) or [dict(month=manifest['asof'],status='no_observations') if application is None else dict(status='no_observations')]
                    computed[definition.name]={canonical(metric_row(definition,row)) for row in raw}
            for record in selected:
                if canonical(record['result']) not in computed[record['result']['metric']]:
                    raise ValidationError(f'receipt reproduction failed: {record["receipt_id"]}')
                verified.append(dict(receipt_id=record['receipt_id'],verified=True,result=record['result']))
    return verified
