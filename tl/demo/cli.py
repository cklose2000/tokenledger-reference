from pathlib import Path
import click


def register(cli,options,output):
    @cli.command('demo')
    @click.option('--directory',type=click.Path(path_type=Path),help='New isolated directory; default data/demo/<unique-run>. Never overwrites.')
    @click.option('--customers',default=28,type=click.IntRange(7,500),show_default=True)
    @options
    def demo(ctx,directory,customers,json_output):
        """Generate, report July, append late evidence, bridge and reproduce. No credentials."""
        from tl.demo.engine import run
        result=run(directory,customers=customers,progress=lambda message:click.echo(message,err=True))
        if json_output: output(result,True);return
        business=result['yield_bridge']
        click.echo('July recognized usage revenue per million tokens (USD/Mtok):')
        click.echo(business['revenue_basis']+'.')
        click.echo(business['denominator']+'.')
        for name,label in [('net_of_discounts_and_fees_per_mtok','Net of discounts and fees'),
                           ('net_of_discounts_per_mtok','Net of discounts')]:
            click.echo(f"{label}: {business['original'][name]} -> {business['current'][name]} (delta {business['delta'][name]} USD/Mtok)")
        click.echo(f"Tokens unchanged: {business['original']['tokens']}; recognized revenue change: {business['delta']['net_revenue_cents']} USD cents")
        click.echo('Yield bridge receipt: '+business['receipt_id'])
        click.echo('July account bridge (integer cents; debit positive, credit negative):')
        for row in result['bridge']['rows']:
            click.echo(f"{row['month']} {row['account']}: {row['before_cents']} -> {row['after_cents']} ({row['delta_cents']:+d})")
        click.echo('Bridge receipt: '+result['bridge']['receipt_id'])
        click.echo('Original yield receipt reproduced: '+business['original_receipt_id'])
        click.echo(f"Workflow: {result['observed_workflow_seconds']:.3f}s; observation {result['observation_receipt_id']}")
        click.echo('Open: '+str(Path(result['artifact_root'])/'README.md'))
