from pathlib import Path
import click

from tl.stream import ValidationError
from tl.stream.events import canonical


def register(cli,options,output):
    @cli.command('profile')
    @click.option('--asof',required=True)
    @click.option('--known-at')
    @click.option('--watermark',type=click.IntRange(0))
    @click.option('--population','names',multiple=True,help='Registered output; omitted means the complete 13-population contract.')
    @click.option('--definition','version',type=click.Choice(['v2','v3']),default='v2')
    @click.option('--reference-run',type=click.Path(exists=True,path_type=Path),help='Retained baseline populations for row-grain parity; not fresh baseline execution.')
    @click.option('--explain',is_flag=True,help='Separate instrumented execution for each requested query.')
    @options
    def profile(ctx,asof,known_at,watermark,names,version,reference_run,explain,json_output):
        """Execute the view-only candidate and retain timings, populations and replay receipts."""
        from tl.views.engine import profile as run
        root=ctx.obj.get('artifact_root')
        if root is None: raise ValidationError('profile requires an isolated --artifact-root')
        result=run(ctx.obj['db'],asof=asof,known_at=known_at,watermark=watermark,names=names,version=version,
            reference=reference_run,explain=explain,output_root=root/'runs',ledger=root/'metrics.jsonl',
            progress=lambda value:click.echo(canonical(value),err=True))
        output(result,json_output)
        if result['status']=='different': ctx.exit(1)
