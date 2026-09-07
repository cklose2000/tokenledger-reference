from pathlib import Path
import click
from tl.compare.engine import run_compare
from tl.stream.events import ValidationError
from tl.stream.events import canonical


def register(cli,options,output):
    @cli.command('compare')
    @click.option('--baseline','project',type=click.Path(path_type=Path),default=Path('baseline'))
    @click.option('--asof')
    @click.option('--suite','run_suite',is_flag=True,help='Run the fixed three-period, late-evidence, increment and definition-change matrix.')
    @click.option('--repetitions',type=click.IntRange(1,10),default=3)
    @click.option('--report',type=click.Path(path_type=Path),default=Path('docs/collapse-report.md'))
    @click.option('--render',type=click.Path(exists=True,path_type=Path),help='Re-render a retained comparison suite bundle.')
    @click.option('--retain-to',type=click.Path(path_type=Path),help='With --render, retain portable report observations in a new directory.')
    @click.option('--known-at')
    @click.option('--watermark',type=click.IntRange(0))
    @click.option('--definition',type=click.Choice(['v2','v3']),default='v2')
    @click.option('--execution',type=click.Choice(['legacy','views-v1']),default='legacy')
    @click.option('--dbt-executable',type=click.Path(exists=True,path_type=Path))
    @options
    def compare(ctx,project,asof,run_suite,repetitions,report,render,retain_to,known_at,watermark,definition,execution,dbt_executable,json_output):
        """Reconcile independent accounting, closes and all registered KPI outputs."""
        if render:
            import json
            if json.loads(render.read_text(encoding='utf-8')).get('schema_version')=='tokenledger-views-suite/v1':
                if retain_to:
                    from tl.views.report import retain
                    render=retain(render,retain_to)
                from tl.views.report import render as render_views
                output(render_views(render,Path('docs/duckdb-execution-report.md') if report==Path('docs/collapse-report.md') else report),json_output);return
            if retain_to:
                from tl.compare.retain import retain
                render=retain(render,retain_to)
            from tl.compare.report import render as render_report
            output(render_report(render,report),json_output);return
        if retain_to: raise ValidationError('--retain-to requires --render')
        root=ctx.obj.get('artifact_root')
        if root is None:
            raise ValidationError('comparison requires --artifact-root for its retained benchmark evidence')
        if execution=='views-v1':
            if run_suite:
                from tl.views.suite import suite
                if asof or known_at or watermark is not None or definition!='v2':
                    raise ValidationError('--suite uses fixed reporting, knowledge and definition states')
                result=suite(ctx.obj['db'],root,repetitions=repetitions,project=project.as_posix(),
                    report=Path('docs/duckdb-execution-report.md') if report==Path('docs/collapse-report.md') else report,
                    executable=dbt_executable,progress=lambda value:click.echo(canonical(value),err=True))
            else:
                if asof is None: raise ValidationError('--asof is required')
                from tl.views.benchmark import compare as compare_views
                result=compare_views(ctx.obj['db'],asof=asof,known_at=known_at,watermark=watermark,version=definition,
                    project=project.as_posix(),output_root=root/'runs',ledger=root/'metrics.jsonl',executable=dbt_executable,
                    progress=lambda value:click.echo(canonical(value),err=True))
            output(result,json_output)
            if result['status'] not in ('equivalent','local_equivalent'): ctx.exit(1)
            return
        if dbt_executable: raise ValidationError('--dbt-executable is available with --execution views-v1')
        if run_suite:
            if asof or known_at or watermark is not None or definition!='v2':
                raise ValidationError('--suite uses fixed reporting, knowledge and definition states')
            from tl.compare.suite import suite
            result=suite(ctx.obj['db'],root,repetitions=repetitions,project=project.as_posix(),report=report,
                         progress=lambda value:click.echo(canonical(value),err=True))
            output(result,json_output);return
        if asof is None: raise ValidationError('--asof is required unless --suite or --render is selected')
        result=run_compare(ctx.obj['db'],asof=asof,known_at=known_at,watermark=watermark,
            project=project.as_posix(),version=definition,output_root=root/'runs',ledger=root/'metrics.jsonl')
        output(result,json_output)
        if result['status']!='equivalent':
            ctx.exit(1)
