import click
from pathlib import Path
from tl.stream import ValidationError


def register(usage,app,options,output):
    @usage.group('learning')
    def learning():
        """Evaluate a frozen candidate; no automatic collection or self-approval."""

    @learning.command('freeze')
    @click.option('--packet',required=True)
    @click.option('--failure',required=True)
    @click.option('--prior-run',required=True)
    @click.option('--development-ordinal',multiple=True,type=click.IntRange(1))
    @options
    def freeze(ctx,packet,failure,prior_run,development_ordinal,json_output):
        from tl.learning.evaluation import freeze as run
        output(run(app(ctx),packet_hash=packet,failure_id=failure,prior_run_id=prior_run,
                   development_ordinals=development_ordinal),json_output)

    @learning.command('evaluate')
    @click.argument('study',type=click.Path(exists=True,path_type=Path))
    @options
    def evaluate(ctx,study,json_output):
        from tl.learning.evaluation import evaluate as run
        output(run(app(ctx),study),json_output)

    @learning.command('promote')
    @click.argument('study',type=click.Path(exists=True,path_type=Path))
    @click.option('--packet')
    @click.option('--source-run')
    @options
    def promote(ctx,study,packet,source_run,json_output):
        app(ctx).validate()
        if not packet or not source_run:
            raise ValidationError('Promotion unavailable: a signed trial plan, new admitted packet and source report are required; actor labels do not authorize it')
        from tl.learning.runner import run
        result=run(app(ctx),study,packet_sha256=packet,source_run_id=source_run)
        output(result,json_output)
        if result['status']=='rolled_back':
            ctx.exit(1)

    @learning.command('prepare')
    @click.argument('study',type=click.Path(exists=True,path_type=Path))
    @click.option('--evaluation-receipt',required=True)
    @click.option('--expires-at',required=True)
    @options
    def prepare(ctx,study,evaluation_receipt,expires_at,json_output):
        from tl.learning.runner import prepare as run
        output(run(app(ctx),study,evaluation_receipt=evaluation_receipt,expires_at=expires_at),json_output)

    @learning.command('revoke')
    @click.argument('trial',type=click.Path(exists=True,path_type=Path))
    @click.option('--actor',required=True)
    @options
    def revoke(ctx,trial,actor,json_output):
        from tl.learning.runner import revoke as run
        output(run(app(ctx),trial,actor=actor),json_output)

    @learning.command('authorize')
    @click.argument('trial',type=click.Path(exists=True,path_type=Path))
    @options
    def authorize(ctx,trial,json_output):
        from tl.learning.runner import authorize as run
        output(run(app(ctx),trial),json_output)
