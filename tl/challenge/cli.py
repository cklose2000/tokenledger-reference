from pathlib import Path
import click


def register(cli, options, output):
    @cli.group('challenge')
    def challenge():
        """Open-book native reporting exercise; expected answers are visible."""

    @challenge.command('prepare')
    @click.option('--directory',required=True,type=click.Path(path_type=Path))
    @options
    def prepare(ctx,directory,json_output):
        from tl.challenge.engine import prepare
        output(prepare(directory),json_output)

    @challenge.command('run')
    @click.option('--directory',required=True,type=click.Path(path_type=Path))
    @options
    def run(ctx,directory,json_output):
        """Prepare actual source/receipt fixtures and grade their visible reference answer."""
        from tl.challenge.engine import prepare
        from tl.challenge.grade import grade
        prepare(directory)
        result=grade(directory,directory/'expected.json',directory/'reference-score')
        output(result,json_output)
        if result['status']!='pass': raise click.exceptions.Exit(1)

    @challenge.command('verify')
    @click.argument('directory',type=click.Path(exists=True,path_type=Path))
    @options
    def verify(ctx,directory,json_output):
        from tl.challenge.engine import verify
        output(verify(directory),json_output)

    @challenge.command('task')
    @click.argument('directory',type=click.Path(exists=True,path_type=Path))
    @click.argument('name',type=click.Choice(['july_yield','late_invoice','nrr_definition','failed_close']))
    @options
    def task(ctx,directory,name,json_output):
        """Reperform the prepared evidence and print one task's exact required facts."""
        from tl.challenge.engine import verify,load
        verify(directory)
        manifest,proof=load(directory)
        output(dict(status='reproduced',fixture_id=manifest['fixture_id'],task=name,
                    result=proof['result'][name],receipt_id=proof['receipt_id']),json_output)

    @challenge.command('grade')
    @click.argument('directory',type=click.Path(exists=True,path_type=Path))
    @click.option('--answer',required=True,type=click.Path(exists=True,path_type=Path))
    @click.option('--output','destination',required=True,type=click.Path(path_type=Path))
    @options
    def grade(ctx,directory,answer,destination,json_output):
        from tl.challenge.grade import grade
        result=grade(directory,answer,destination)
        output(result,json_output)
        if result['status']!='pass': raise click.exceptions.Exit(1)

    @challenge.command('close')
    @click.argument('directory',type=click.Path(exists=True,path_type=Path))
    @click.option('--watermark',type=click.IntRange(0))
    @click.option('--recover',is_flag=True)
    @options
    def close(ctx,directory,watermark,recover,json_output):
        """Replay the source hold or retry the captured-source recovery."""
        from tl.challenge.engine import verify,load
        from tl.challenge.close import publish
        verify(directory)
        manifest,proof=load(directory)
        allowed={manifest['failed_source'][k]['watermark'] for k in ('failed','recovered')}
        if watermark is not None and watermark not in allowed:
            from tl.stream import ValidationError
            raise ValidationError('watermark is outside this challenge proof')
        result=publish(directory/'close',watermark=watermark,recover=recover)
        output(dict(**result,receipt_id=proof['receipt_id']),json_output)
        if result['status']=='held': raise click.exceptions.Exit(2)
