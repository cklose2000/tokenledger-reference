"""One explicit reporting-learning interface for agents and human operators."""
from pathlib import Path
import uuid

import click

from tl.bigquery.config import Binding
from tl.bigquery.gateway import GatewayConfig
from tl.evolution import admission, evidence, transport
from tl.stream import ValidationError


def register(cli, options, output):
    @cli.group('learning')
    @click.option('--gateway-config', type=click.Path(exists=True, path_type=Path))
    @click.pass_context
    def group(ctx, gateway_config):
        """Governed reporting improvements. No promotion or background collection."""
        ctx.obj = {**ctx.obj, 'learning_gateway': gateway_config}

    def config(ctx):
        if ctx.obj['learning_gateway'] is None:
            raise ValidationError('an explicit --gateway-config is required')
        value = GatewayConfig.read(ctx.obj['learning_gateway'])
        if value.synthetic_rehearsal:
            raise ValidationError('a synthetic rehearsal configuration is confined to tl learning walkthrough')
        return value

    def paths(ctx):
        root = ctx.obj.get('artifact_root')
        if root is None:
            raise ValidationError('learning requires an explicit private --artifact-root')
        from tl.reporting.application import private_path
        root = private_path(root)
        return root / 'runs', root / 'metrics.jsonl'

    @group.command('walkthrough')
    @click.option('--directory', type=click.Path(path_type=Path),
                  help='New isolated directory; default data/learning-walkthrough/<unique-run>. Never overwrites.')
    @click.option('--customers', default=28, type=click.IntRange(7, 500), show_default=True)
    @click.option('--next-inspection-date', default='2026-07-01', show_default=True,
                  help='Date the synthetic next inspection requests; the report is dated 2026-07-31.')
    @options
    def walkthrough(ctx, directory, customers, next_inspection_date, json_output):
        """Synthetic rehearsal of the whole loop on one local report. No credentials, no real authority."""
        from tl.evolution.walkthrough import run
        result = run(directory, customers=customers, next_inspection_date=next_inspection_date,
                     progress=lambda message: click.echo(message, err=True))
        if json_output: output(result, True); return
        b = result['baseline_inspection']
        click.echo(f"July NRR report: {b['report_rows']} lenses dated {b['report_date']}; receipt {result['report_receipt_id']}")
        click.echo(f"Wrong-date inspection {b['requested_date']}: {b['selected_rows']} rows selected, no explanation (the finding)")
        click.echo(f"Workflow events admitted: {result['workflow_events']}; refused attempts: {result['refused_attempts']}; stream unchanged after each refusal")
        click.echo(f"Frozen evaluation: {result['evaluation']['cases']} cases, DuckDB candidate equals oracle; receipt {result['evaluation_receipt_id']}")
        click.echo('Approval: ' + result['approval_authority'])
        f = result['outcome']['finding']
        click.echo(f"Next inspection {result['next_inspection_date']}: diagnostic {f['status']}, suggested date {f['suggested_date']}; outcome {result['outcome_status']}")
        click.echo(result['outcome']['interpretation'])
        actual = result['outcome']['actual_native_trial']
        click.echo(f"Actual native signed trial ({actual['signed_revision'][:7]}): {actual['status']}, no promotion, no ongoing activation")
        click.echo(f"Original report reproduced; learning rows in business stream: {result['business_separation']['learning_rows_in_business_stream']}")
        click.echo(f"Workflow: {result['observed_workflow_seconds']:.3f}s; walkthrough receipt {result['walkthrough_receipt_id']}")
        click.echo('Open: ' + str(Path(result['artifact_root']) / 'README.md'))

    @group.command('replay-walkthrough')
    @click.argument('directory', type=click.Path(exists=True, file_okay=False, path_type=Path))
    @options
    def replay_walkthrough(ctx, directory, json_output):
        """Rebuild workflow state, re-verify the signature, re-perform the candidate and reproduce the report."""
        from tl.evolution.walkthrough import replay
        output(replay(directory), json_output)

    @group.command('prepare-event')
    @click.argument('activity', type=click.Choice(admission.ACTIVITIES))
    @click.option('--case', 'case_id', required=True)
    @click.option('--record', type=click.Path(exists=True, path_type=Path), required=True)
    @click.option('--event-id', required=True, help='Stable idempotency key; keep the resulting event file for retries.')
    @click.option('--output', 'destination', type=click.Path(path_type=Path), required=True)
    @options
    def prepare_event(ctx, activity, case_id, record, event_id, destination, json_output):
        """Prepare a closed event. Does not submit it or claim caller authority."""
        from tl.reporting.application import private_path
        destination = private_path(destination)
        value = transport.event(activity, case_id, admission.strict_json(record.read_bytes()), event_id)
        evidence.write(destination, value)
        output(dict(status='prepared_not_submitted', event_id=event_id, event_file=str(destination)), json_output)

    @group.command('submit')
    @click.argument('event_file', type=click.Path(exists=True, path_type=Path))
    @click.option('--producer', required=True, help='Explicit allowed service-account principal; never an actor label.')
    @click.option('--url', required=True, help='Exact pinned deployed HTTPS service origin.')
    @options
    def submit(ctx, event_file, producer, url, json_output):
        """POST one retained event using a signed Google identity; no secret logging."""
        output_root, ledger = paths(ctx)
        result = transport.submit(config(ctx), admission.strict_json(event_file.read_bytes()), producer, url,
                                  output_root=output_root, ledger=ledger)
        output(result, json_output)
        if result['status'] != 'accepted': raise click.exceptions.Exit(2)

    @group.command('snapshot')
    @options
    def snapshot(ctx, json_output):
        """Read and retain the validated workflow prefix with its evidence receipt."""
        output_root, ledger = paths(ctx)
        output(transport.snapshot(config(ctx), output_root=output_root, ledger=ledger), json_output)

    @group.command('view-sql')
    @click.argument('destination', type=click.Path(path_type=Path))
    @options
    def view_sql(ctx, destination, json_output):
        """Export the derived case-history view; does not create cloud objects."""
        sql = transport.case_view_sql(config(ctx))
        with destination.open('x', encoding='utf-8', newline='\n') as handle:
            handle.write(sql)
        output(dict(status='exported', path=str(destination), native_execution='not_run'), json_output)

    @group.command('evaluate')
    @click.argument('export', type=click.Path(exists=True, path_type=Path))
    @click.option('--bigquery-config', type=click.Path(exists=True, path_type=Path), required=True)
    @options
    def evaluate(ctx, export, bigquery_config, json_output):
        """Evaluate the frozen diagnostic natively against an NRR-only export."""
        output_root, ledger = paths(ctx)
        result = evidence.evaluate(Binding.read(bigquery_config), export, output_root=output_root, ledger=ledger)
        output(result, json_output)
        if result['status'] != 'passed': raise click.exceptions.Exit(2)

    @group.command('replay')
    @click.argument('receipt_id')
    @options
    def replay(ctx, receipt_id, json_output):
        """Offline diagnostic re-performance; native business recognition is separate."""
        output_root, ledger = paths(ctx)
        output(evidence.replay([receipt_id], output_root=output_root, ledger=ledger)[0], json_output)

    @group.command('pr-check')
    @click.argument('number', type=click.IntRange(1))
    @click.option('--output', 'destination', type=click.Path(path_type=Path), required=True)
    @options
    def pr_check(ctx, number, destination, json_output):
        """Inspect private PR head, fixed change scope and CI. Never approve it."""
        from tl.evolution.github import inspect
        result = inspect(number)
        evidence.write(destination, result)
        output(result, json_output)

    @group.command('prepare-trial')
    @click.option('--case', 'case_id', required=True)
    @click.option('--expires-at', required=True)
    @options
    def prepare_trial(ctx, case_id, expires_at, json_output):
        """Freeze exact candidate, native evaluation and one-run plan for reviewer signature."""
        from tl.evolution.trial import prepare
        output_root, ledger = paths(ctx)
        output(prepare(config(ctx), case_id, output_root=output_root, ledger=ledger, expires_at=expires_at), json_output)

    @group.command('rehearse')
    @click.option('--test-key', type=click.Path(exists=True,path_type=Path), required=True)
    @click.option('--evaluation', required=True)
    @click.option('--pr-observation',type=click.Path(exists=True,path_type=Path),required=True)
    @options
    def rehearse(ctx,test_key,evaluation,pr_observation,json_output):
        """Bounded native rejection tests on a disposable deployment, never human approval."""
        from tl.evolution.native_acceptance import run
        output_root,ledger=paths(ctx)
        result=run(config(ctx),test_key,evaluation,admission.strict_json(pr_observation.read_bytes()),
                   output_root=output_root,ledger=ledger)
        output(result,json_output)
        if result['status']!='passed': raise click.exceptions.Exit(2)

    @group.command('authorize')
    @click.argument('plan', type=click.Path(exists=True, path_type=Path))
    @click.option('--signature', type=click.Path(exists=True, path_type=Path), required=True)
    @click.option('--producer', required=True)
    @click.option('--url', required=True)
    @options
    def authorize(ctx, plan, signature, producer, url, json_output):
        """Verify and admit an exact detached reviewer signature. Never sign or enroll keys."""
        from tl.evolution.trial import authorize
        output_root, ledger = paths(ctx)
        result = authorize(config(ctx), plan, signature, producer, url, output_root=output_root, ledger=ledger)
        output(result, json_output)
        if result['status'] != 'accepted': raise click.exceptions.Exit(2)

    @group.command('run-trial')
    @click.argument('authorization_id')
    @click.option('--export', type=click.Path(exists=True, path_type=Path), required=True)
    @click.option('--bigquery-config', type=click.Path(exists=True, path_type=Path), required=True)
    @click.option('--producer', required=True)
    @click.option('--url', required=True)
    @options
    def run_trial(ctx, authorization_id, export, bigquery_config, producer, url, json_output):
        """Claim and measure the first next inspection once; no ongoing activation."""
        from tl.evolution.trial import run
        output_root, ledger = paths(ctx)
        result = run(config(ctx), authorization_id, export, Binding.read(bigquery_config), producer, url,
                     output_root=output_root, ledger=ledger)
        output(result, json_output)
        if result['status'] == 'failed' or result['completion_status'] != 'accepted': raise click.exceptions.Exit(2)
