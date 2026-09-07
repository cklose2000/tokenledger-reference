"""Noninteractive controls and pre-ingestion evidence commands."""
from pathlib import Path
import click


def register(cli,options,output):
    @cli.group('sources')
    def sources():
        """Capture synthetic source observations before ingestion."""

    @sources.command('capture')
    @click.argument('input_path',type=click.Path(exists=True,path_type=Path))
    @click.option('--destination',required=True,type=click.Path(path_type=Path))
    @click.option('--source',required=True)
    @click.option('--start',required=True)
    @click.option('--end',required=True,help='Exclusive coverage end.')
    @options
    def capture(ctx,input_path,destination,source,start,end,json_output):
        from tl.controls.sources import capture
        output(capture(ctx.obj['db'],input_path,destination,source=source,start=start,end=end,catalog_path=ctx.obj['definitions']),json_output)

    @sources.command('ingest')
    @click.argument('manifest',type=click.Path(exists=True,path_type=Path))
    @options
    def ingest(ctx,manifest,json_output):
        from tl.controls.sources import ingest
        result=ingest(ctx.obj['db'],manifest,catalog_path=ctx.obj['definitions'])
        output(result,json_output)
        if result['rejected_observations']:
            raise click.exceptions.Exit(1)

    @cli.group('controls')
    def controls():
        """Render controls, verify bundles and guard released definitions."""

    @controls.command('render')
    @click.option('--output','destination',type=click.Path(path_type=Path),default=Path('docs/controls/kpi-control-matrix.md'))
    @options
    def render(ctx,destination,json_output):
        from tl.controls.matrix import render
        output(render(destination),json_output)

    @controls.command('check-definitions')
    @click.option('--base')
    @options
    def check(ctx,base,json_output):
        from tl.controls.matrix import check_definitions
        result=check_definitions(base);output(result,json_output)
        if result['status']!='passed':
            raise click.exceptions.Exit(2 if result['status']=='not_run' else 1)

    @controls.command('verify-evidence')
    @click.argument('path',type=click.Path(exists=True,path_type=Path))
    @click.option('--manifest-sha256',required=True)
    @options
    def verify(ctx,path,manifest_sha256,json_output):
        from tl.controls.evidence import verify_bundle
        output(verify_bundle(path,manifest_sha256),json_output)

    @controls.command('fixture')
    @click.argument('destination',type=click.Path(path_type=Path))
    @options
    def fixture(ctx,destination,json_output):
        from tl.controls.fixture import write_fixture
        output(write_fixture(destination),json_output)

    @controls.command('demo')
    @click.argument('destination',type=click.Path(path_type=Path))
    @options
    def demo(ctx,destination,json_output):
        """Build an isolated synthetic close; exit 2 exposes missing operational controls."""
        from tl.controls.fixture import write_fixture
        from tl.controls.sources import capture,ingest
        from tl.controls.evidence import collect
        from tl.metrics.engine import run_metrics
        from tl.metrics.pack import pack_run
        from tl.stream import Stream,ValidationError
        from tl.stream.validation import validate_stream
        destination.mkdir(parents=True,exist_ok=False)
        db=destination/'world.duckdb';Stream(db).init()
        write_fixture(destination/'fixture')
        captured=capture(db,destination/'fixture/source.jsonl',destination/'capture',source='sim:controls',start='2026-04-01',end='2026-07-01')
        ingest(db,captured['path'])
        if not validate_stream(db,'definitions/activities',source_manifest=[captured['path']])['passed']:
            raise ValidationError('control fixture source validation failed')
        roots=dict(output_root=destination/'runs',ledger=destination/'metrics.jsonl')
        run=run_metrics(db,asof='2026-06-30',known_at='2026-07-15T00:00:00Z',**roots)
        pack=pack_run(run['run_id'],pack_root=destination/'pack',**roots)
        result=collect('2026Q2',run_id=run['run_id'],db=db,pack=pack['path'],source_manifests=[captured['path']],
            erp=destination/'fixture/erp.csv',erp_manifest=destination/'fixture/erp-manifest.json',evidence_root=destination/'evidence',**roots)
        output({**result,'artifact_root':str(destination),'db':str(db),'metric_run_id':run['run_id']},json_output)
        raise click.exceptions.Exit(result['exit_code'])

    @cli.command('evidence')
    @click.option('--period',required=True)
    @click.option('--run-id')
    @click.option('--pack',type=click.Path(exists=True,path_type=Path))
    @click.option('--source-manifest','source_manifests',multiple=True,type=click.Path(exists=True,path_type=Path))
    @click.option('--erp',type=click.Path(exists=True,path_type=Path))
    @click.option('--erp-manifest',type=click.Path(exists=True,path_type=Path))
    @click.option('--external',type=click.Path(exists=True,path_type=Path),help='Declared external artifact descriptors; imports never grant verified identity.')
    @options
    def evidence(ctx,period,run_id,pack,source_manifests,erp,erp_manifest,external,json_output):
        from tl.controls.evidence import collect
        root=ctx.obj.get('artifact_root');kwargs={}
        if root:
            kwargs=dict(output_root=root/'runs',ledger=root/'metrics.jsonl',evidence_root=root/'evidence')
        result=collect(period,run_id=run_id,db=ctx.obj['db'],pack=pack,source_manifests=source_manifests,
                       erp=erp,erp_manifest=erp_manifest,external=external,**kwargs)
        output(result,json_output)
        raise click.exceptions.Exit(result['exit_code'])
