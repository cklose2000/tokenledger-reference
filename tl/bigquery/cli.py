"""Noninteractive, explicit cloud boundaries and honest not-run states."""
from pathlib import Path
from functools import wraps

import click

from tl.bigquery.config import Binding,NotRun
from tl.stream import ValidationError


def register(cli,options,output):
    shared_options=options
    def options(function):
        @wraps(function)
        def guarded(*args,**kwargs):
            try: return function(*args,**kwargs)
            except NotRun as exc:
                output(dict(status='not_run',reason=str(exc),native_execution='not_run'),kwargs.get('json_output',False))
                raise click.exceptions.Exit(2)
            except (ValidationError,click.ClickException): raise
            except Exception as exc: raise ValidationError('BigQuery operation failed: '+type(exc).__name__+': '+str(exc)) from exc
        return shared_options(guarded)
    @cli.group('cloud')
    def cloud():
        """Explicitly bound native warehouse execution."""

    @cloud.group('bigquery')
    @click.option('--config',type=click.Path(exists=True,path_type=Path))
    @click.pass_context
    def group(ctx,config): ctx.obj={**ctx.obj,'bigquery_config':config}

    @group.command('configure')
    @click.option('--project',required=True)
    @click.option('--dataset',required=True)
    @click.option('--location',required=True)
    @click.option('--maximum-bytes-billed',type=click.IntRange(1),required=True)
    @click.option('--maximum-run-bytes-billed',type=click.IntRange(1),required=True)
    @click.option('--query-principal',help='Explicit reporting service account in the bound project.')
    @click.option('--writer-principal',help='Distinct ingestion service account in the bound project.')
    @click.option('--output','path',type=click.Path(path_type=Path),required=True)
    @options
    def configure(ctx,project,dataset,location,maximum_bytes_billed,maximum_run_bytes_billed,query_principal,writer_principal,path,json_output):
        """Write an explicit local binding. No cloud calls or credential changes."""
        binding=Binding(project,dataset,location,maximum_bytes_billed,maximum_run_bytes_billed,
                        query_principal=query_principal,writer_principal=writer_principal);binding.save(path)
        output(dict(status='configured',binding_identity=binding.identity,config=str(path),native_execution='not_run',
                    authority_mode=binding.authority_mode,iam_negative_tests='not_run'),json_output)

    @group.command('provision')
    @options
    def provision(ctx,json_output):
        """Create the bound scratch dataset and source table; refuse incompatible objects."""
        from tl.bigquery.client import Client
        output(Client(Binding.read(ctx.obj['bigquery_config'])).provision(),json_output)

    @group.command('access')
    @options
    def access(ctx,json_output):
        """Inspect bound identities; permission inspection is not a mutation-test pass."""
        from tl.bigquery.access import inspect
        output(inspect(Binding.read(ctx.obj['bigquery_config'])),json_output)

    @group.command('append')
    @click.option('--state',type=click.Path(path_type=Path),required=True)
    @options
    def append(ctx,state,json_output):
        """Replicate a validated synthetic stream prefix through Storage Write API."""
        from tl.bigquery.writer import append as execute
        output(execute(Binding.read(ctx.obj['bigquery_config']),ctx.obj['db'],state),json_output)

    @group.command('run')
    @click.argument('directory',type=click.Path(exists=True,path_type=Path))
    @click.option('--dry-run',is_flag=True)
    @click.option('--output-root',type=click.Path(path_type=Path),required=True)
    @click.option('--ledger',type=click.Path(path_type=Path),required=True)
    @click.option('--reference',type=click.Path(exists=True,path_type=Path),help='Independent retained baseline Parquet directory.')
    @options
    def run(ctx,directory,dry_run,output_root,ledger,reference,json_output):
        """Run frozen SQL, checks, source matching and receipt-backed outputs."""
        from tl.bigquery.engine import run as execute
        result=execute(Binding.read(ctx.obj['bigquery_config']),directory,output_root=output_root,ledger=ledger,reference=reference,dry_run=dry_run)
        output(result,json_output)
        if result['status']=='different': raise click.exceptions.Exit(1)

    @group.command('receipt')
    @click.argument('ids',nargs=-1,required=True)
    @click.option('--output-root',type=click.Path(path_type=Path),required=True,help='Population runs directory; suite observations read parity.json from its parent.')
    @click.option('--ledger',type=click.Path(path_type=Path),required=True)
    @options
    def receipt(ctx,ids,output_root,ledger,json_output):
        """Recompute populations or verify retained observations without cloud calls."""
        from tl.bigquery.engine import replay
        results=replay(Binding.read(ctx.obj['bigquery_config']),ids,output_root=output_root,ledger=ledger)
        output(dict(status='verified_observation' if all(r['recomputed'] is False for r in results) else 'reproduced',results=results),json_output)

    @cli.group('export')
    def exports():
        """Generate plain warehouse DDL and SQL; no dbt spine dependency."""

    @exports.command('bigquery')
    @click.option('--config',type=click.Path(exists=True,path_type=Path),required=True)
    @click.option('--asof',required=True)
    @click.option('--known-at')
    @click.option('--watermark',type=click.IntRange(0))
    @click.option('--definition','version',type=click.Choice(['v2','v3']),default='v2')
    @click.option('--output','directory',type=click.Path(path_type=Path),required=True)
    @click.option('--name','names',multiple=True)
    @options
    def export(ctx,config,asof,known_at,watermark,version,directory,names,json_output):
        from tl.bigquery.export import export as execute
        output(execute(ctx.obj['db'],Binding.read(config),directory,asof=asof,known_at=known_at,
                       watermark=watermark,version=version,names=names),json_output)

    @cli.command('parity')
    @click.option('--target',type=click.Choice(['bigquery']),required=True)
    @click.option('--config',type=click.Path(exists=True,path_type=Path),required=True)
    @click.option('--output','directory',type=click.Path(path_type=Path),required=True)
    @click.option('--baseline-executable',type=click.Path(exists=True,path_type=Path))
    @options
    def parity(ctx,target,config,directory,baseline_executable,json_output):
        """Execute three periods, late evidence, one day and NRR v3 in BigQuery."""
        from tl.bigquery.parity import suite
        def progress(value): click.echo(canonical(value),err=True)
        from tl.stream.events import canonical
        output(suite(Binding.read(config),ctx.obj['db'],directory,executable=baseline_executable,progress=progress),json_output)
