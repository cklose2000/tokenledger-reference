"""Three-period native parity plus late evidence, one day and definition change.

The independent conventional baseline runs locally. Its time is never presented
as BigQuery baseline performance. The spine executes only plain GoogleSQL.
"""
from pathlib import Path
import json
import shutil

from tl.bigquery.client import Client
from tl.bigquery.export import export
from tl.bigquery.engine import run,replay
from tl.bigquery.writer import append,validated_prefix
from tl.compare.scenarios import inject
from tl.receipts.metrics import append_receipts, digest, receipt_id
from tl.stream import ValidationError
from tl.stream.events import canonical


CASES=(('original_may','2026-05-31',None,'v2'),
       ('original_june','2026-06-30',None,'v2'),
       ('original_july','2026-07-31',None,'v2'),
       ('late_june','2026-06-30','2026-07-15T00:00:00Z','v2'),
       ('one_day_july','2026-07-31',None,'v2'),
       ('nrr_floor','2026-07-31',None,'v3'))


def suite(binding,db,directory,*,executable=None,client=None,progress=None):
    root=Path(directory).resolve()
    if root.exists(): raise ValidationError('native parity requires a fresh evidence directory')
    # Six full 13-output runs, five domain checks and two source checks each;
    # two transports with two reads each; one NRR replay (source/core/output).
    ceiling_jobs=127
    if binding.maximum_run_bytes_billed<ceiling_jobs*binding.maximum_bytes_billed:
        raise ValidationError('parity requires aggregate headroom for 127 independently capped jobs; change the explicit byte boundary or run selected outputs')
    validated_prefix(db)
    root.mkdir(parents=True)
    source=root/'world.duckdb';shutil.copyfile(db,source)
    late=inject(source,'late')
    client=client or Client(binding)
    report=dict(schema_version='tokenledger-bigquery-parity/v1',run_id=root.name,status='running',
        native_execution='running',binding_identity=binding.identity,cases=[],late_fixture=late,
        baseline_target='DuckDB independent dbt implementation',
        cloud_baseline_performance='not_run',iam_negative_tests='not_run',
        authority_mode=binding.authority_mode)
    def save(): (root/'parity.json').write_text(canonical(report)+'\n',encoding='utf-8',newline='\n')
    save()
    try:
        report['initial_append']=append(binding,source,root/'writer.json',client=client);save()
        from tl.views.worker import run_worker
        for label,asof,known,version in CASES:
            if label=='one_day_july':
                report['one_day_fixture']=inject(source,'day')
                report['incremental_append']=append(binding,source,root/'writer.json',client=client);save()
            if progress: progress(dict(case=label,status='baseline_started'))
            reference,process=run_worker('compare',dict(db=source,asof=asof,known_at=known,version=version,
                output_root=root/'reference-runs',ledger=root/'reference-ledger.jsonl',executable=executable),progress)
            if reference['status']!='equivalent': raise ValidationError('independent local baseline differs; cloud comparison held')
            exported=export(source,binding,root/'exports'/label,asof=asof,known_at=known,version=version)
            if progress: progress(dict(case=label,status='bigquery_started'))
            actual=run(binding,root/'exports'/label,output_root=root/'runs',ledger=root/'metrics.jsonl',
                       reference=reference['run_directory'],client=client)
            report['cases'].append(dict(case=label,asof=asof,known_at=known,definition_version=version,
                export=exported,reference=reference,reference_process=process,bigquery=actual))
            save()
            if actual['parity']!='equivalent': raise ValidationError('native BigQuery parity differs; all evidence retained')
        selected=next(c for c in report['cases'] if c['case']=='original_july')['bigquery']
        manifest=json.loads((Path(selected['run_directory'])/'run.json').read_text(encoding='utf-8'))
        report['original_nrr_replay']=replay(binding,[manifest['outputs']['consumption_nrr']['receipt_id']],
                     output_root=root/'runs',ledger=root/'metrics.jsonl',client=client)
        report['status']='equivalent';report['jobs']=client.jobs
        report['native_execution']='executed';save()
        export_manifest=json.loads((root/'exports/original_july/manifest.json').read_text(encoding='utf-8'))
        record=dict(schema_version='tokenledger-bigquery-suite-observation/v1',run_id=root.name,result=dict(report_sha256=digest(report)),
            definition_version='bigquery-parity/v1',inputs=export_manifest['inputs'],
            query_hash=digest(export_manifest['files']),git_sha=export_manifest['git_sha'],
            execution_hash=export_manifest['execution_hash'],binding_identity=binding.identity,asof=export_manifest['asof'],
            known_at=export_manifest['known_at'],watermark=export_manifest['watermark'])
        record['receipt_id']=receipt_id(record);append_receipts(root/'metrics.jsonl',[record])
        return dict(status='equivalent',directory=str(root),cases=len(CASES),outputs_per_case=13,
                    observation_receipt_id=record['receipt_id'],observation_output_root=str(root/'runs'),
                    iam_negative_tests='not_run',cloud_baseline_performance='not_run')
    except BaseException as exc:
        report.update(status='failed',error=str(exc),jobs=client.jobs);save();raise
