"""Application-bound commands. U1 token arithmetic/import remains a separate gate."""

from datetime import datetime
import json
from pathlib import Path

import click

from tl.reporting.application import Application
from tl.stream import Activity,ValidationError
from tl.stream.events import UTC,iso
from tl.receipts.metrics import digest


def register(cli, options, output):
    @cli.group('usage')
    @click.option('--config',type=click.Path(path_type=Path),required=True)
    @click.pass_context
    def usage(ctx,config):
        """Private application: binding, readiness and bounded source discovery."""
        root=ctx.find_root()
        if any(root.get_parameter_source(key)!=click.core.ParameterSource.DEFAULT
               for key in ('db','definitions','artifact_root')):
            raise ValidationError('usage uses its explicit configuration, not synthetic root overrides')
        ctx.obj={**ctx.obj,'application_config':config}

    def app(ctx):
        return Application(ctx.obj['application_config'])

    @usage.command('init')
    @click.option('--actor',required=True)
    @options
    def initialize(ctx,actor,json_output):
        application=Application.initialize(ctx.obj['application_config'],actor=actor)
        output(dict(status='bound',application=application.name,physical_columns=14,
                    config=str(application.config)),json_output)

    @usage.command('inspect')
    @options
    def inspect(ctx,json_output):
        application=app(ctx)
        application.validate()
        with application.reader().connect() as conn:
            admitted=conn.execute("SELECT count(*) FROM stream.activity WHERE activity='usage_observed'").fetchone()[0]
        output(dict(status='valid',application=application.name,physical_columns=14,
                    synthetic=False,token_report='usage_available' if admitted else 'U1_pending'),json_output)

    @usage.command('catalog-sync')
    @click.option('--actor',required=True)
    @options
    def sync(ctx,actor,json_output):
        Application.sync_catalog(ctx.obj['application_config'],actor=actor)
        output(dict(status='catalog_synced',activity_schema='unchanged'),json_output)

    @usage.command('import')
    @click.argument('source',type=click.Choice(['codex']))
    @click.option('--input','directory',type=click.Path(exists=True,path_type=Path),required=True)
    @click.option('--actor',required=True)
    @options
    def import_usage(ctx,source,directory,actor,json_output):
        from tl.usage.importer import import_sample
        output(import_sample(app(ctx),directory,actor=actor),json_output)

    def period_options(fn):
        fn=click.option('--timezone',default='UTC',show_default=True)(fn)
        fn=click.option('--known-at',required=True)(fn)
        fn=click.option('--to','end',required=True)(fn)
        return click.option('--from','start',required=True)(fn)

    @usage.command('report')
    @period_options
    @options
    def usage_report(ctx,start,end,known_at,timezone,json_output):
        from tl.usage.report import report
        output(report(app(ctx),start=start,end=end,known_at=known_at,timezone=timezone),json_output)

    @usage.command('gaps')
    @period_options
    @options
    def gaps(ctx,start,end,known_at,timezone,json_output):
        from tl.usage.report import report
        output(report(app(ctx),start=start,end=end,known_at=known_at,timezone=timezone,gaps_only=True),json_output)

    @usage.command('inventory')
    @click.option('--input','source',type=click.Path(exists=True,path_type=Path))
    @click.option('--actor')
    @options
    def inventory(ctx,source,actor,json_output):
        application=app(ctx)
        application.validate()
        if source:
            rows=json.loads(source.read_bytes())
            if not isinstance(rows,list) or not rows:
                raise ValidationError('inventory requires a nonempty list')
            now=datetime.now(UTC)
            events=[]
            for row in rows:
                if not isinstance(row,dict) or set(row)!={'account_alias','features'}:
                    raise ValidationError('invalid inventory item')
                key='inventory:'+digest(row)
                # Retries retain the original event time as part of identity.
                with application.reader().connect() as conn:
                    old=conn.execute('SELECT ts FROM stream.activity WHERE activity_id=?',[key]).fetchone()
                events.append(Activity(key,old[0] if old else now,'application_inventory',row['features'],row['account_alias']))
            application.append(events,source='inventory',actor=actor)
        with application.reader().connect() as conn:
            rows=conn.execute("""SELECT customer,feature_json::VARCHAR FROM stream.activity
                WHERE activity='application_inventory' ORDER BY customer,activity_id""").fetchall()
        output(dict(status='inventory',entries=[dict(account_alias=c,**json.loads(f)) for c,f in rows]),json_output)

    @usage.command('readiness')
    @click.option('--asof',required=True)
    @click.option('--known-at',required=True)
    @options
    def readiness(ctx,asof,known_at,json_output):
        """Receipt-backed structural counts, not U1 consumption totals."""
        from tl.metrics.engine import run_metrics
        application=app(ctx)
        result=run_metrics(application.db,asof=asof,known_at=known_at,names=['usage_readiness'],
            output_root=application.outputs,ledger=application.ledger,application=application)
        output(dict(status='readiness_only',run_id=result['run_id'],rows=result['rows']),json_output)

    @usage.command('receipt')
    @click.argument('receipt_id')
    @options
    def receipt(ctx,receipt_id,json_output):
        from tl.metrics.engine import replay_receipts
        application=app(ctx)
        result=replay_receipts([receipt_id],db=application.db,output_root=application.outputs,
                               ledger=application.ledger,application=application)
        output(result[0],json_output)

    @usage.command('sample-codex')
    @click.option('--source-root',type=click.Path(exists=True,path_type=Path),required=True)
    @click.option('--day',required=True)
    @click.option('--workspace',type=click.Path(exists=True,path_type=Path),required=True)
    @click.option('--client-version',required=True)
    @click.option('--account',required=True)
    @options
    def sample(ctx,source_root,day,workspace,client_version,account,json_output):
        from tl.usage.sources import token_sample
        output(token_sample(app(ctx),source_root=source_root,day=day,workspace=workspace,
            client_version=client_version,account=account),json_output)

    from tl.learning.cli import register as register_learning
    register_learning(usage,app,options,output)
