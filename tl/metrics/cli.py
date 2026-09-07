"""Phase 2 commands. Successful metric commands intentionally append metric receipts."""

from calendar import monthrange
from pathlib import Path
import click

from tl.metrics.definitions import FAMILIES
from tl.metrics.engine import run_metrics,replay_receipts
from tl.metrics.pack import pack_run
from tl.scaffolds.session import ReportSession
from tl.scaffolds.invariants import verify
from tl.stream import StreamReader,ValidationError


def register(cli,options,output):
    def roots(ctx,pack=False):
        root=ctx.obj.get('artifact_root')
        if root is None:
            return {}
        result=dict(output_root=root/'runs',ledger=root/'metrics.jsonl')
        if pack:
            result['pack_root']=root/'pack'
        return result

    def db_override(ctx):
        source=ctx.find_root().get_parameter_source('db')
        return None if source is click.core.ParameterSource.DEFAULT else ctx.obj['db']

    def dates(function):
        function=click.option('--watermark',type=click.IntRange(0))(function)
        function=click.option('--known-at',help='Exclusive knowledge cutoff, including timezone.')(function)
        return click.option('--asof',required=True,help='Inclusive calendar month-end.')(function)

    def execute(ctx,asof,known_at,watermark,**kwargs):
        return run_metrics(ctx.obj['db'],asof=asof,known_at=known_at,watermark=watermark,**kwargs,**roots(ctx))

    def native(ctx,asof,known_at,watermark,names=FAMILIES,version='v2'):
        from tl.views.engine import profile
        if ctx.obj.get('artifact_root') is None: raise ValidationError('views-v1 requires an isolated --artifact-root')
        return profile(ctx.obj['db'],asof=asof,known_at=known_at,watermark=watermark,names=names,version=version,**roots(ctx))

    def native_pack(ctx,run_id):
        from tl.views.pack import pack_run
        if ctx.obj.get('artifact_root') is None: raise ValidationError('views-v1 requires an isolated --artifact-root')
        return pack_run(run_id,**roots(ctx,pack=True))

    @cli.command('build')
    @click.option('--asof',help='Month-end for verification; defaults to the latest source month-end.')
    @click.option('--known-at')
    @click.option('--watermark',type=click.IntRange(0))
    @click.option('--target',type=click.Choice(['duckdb']),default='duckdb',show_default=True)
    @options
    def build(ctx,asof,known_at,watermark,target,json_output):
        """Build six derived views on a frozen snapshot and check reconciliation."""
        if asof is None:
            with StreamReader(ctx.obj['db']).connect() as conn:
                last=conn.execute('SELECT max(ts)::DATE FROM stream.activity').fetchone()[0]
            if last is None:
                raise ValidationError('generate or ingest activities before build')
            asof=last.replace(day=monthrange(last.year,last.month)[1]).isoformat()
        with ReportSession(ctx.obj['db'],asof=asof,known_at=known_at,watermark=watermark) as session:
            checks=verify(session)
            output(dict(status='built',**session.build_manifest(),checks=checks),json_output)

    @cli.command('metric')
    @click.argument('name',type=click.Choice(FAMILIES))
    @dates
    @click.option('--definition','version',default=None,help='Default: NRR v2; other families v1. Released versions remain selectable.')
    @click.option('--cut',multiple=True,help='Repeat or comma-separate dimensions; overall selects only fixed grain.')
    @click.option('--execution',type=click.Choice(['legacy','views-v1']),default='legacy')
    @options
    def metric(ctx,name,asof,known_at,watermark,version,cut,execution,json_output):
        """Compute a metric family, emitting receipts with every result row."""
        if execution=='views-v1':
            if cut: raise ValidationError('views-v1 currently emits the complete registered commercial cuts; omit --cut')
            if (name=='consumption_nrr' and version not in (None,'v2','v3')) or (name!='consumption_nrr' and version not in (None,'v1')):
                raise ValidationError('views-v1 supports NRR v2/v3 and other metrics v1')
            output(native(ctx,asof,known_at,watermark,names=[name],version=(version or 'v2') if name=='consumption_nrr' else 'v2'),json_output);return
        cuts=[part.strip() for item in cut for part in item.split(',')] if cut else None
        if cuts==['overall']:
            cuts=[]
        result=execute(ctx,asof,known_at,watermark,names=[name],version=version,cuts=cuts)
        output(dict(run_id=result['run_id'],run_directory=result['run_directory'],rows=result['rows']),json_output)

    @cli.group('metrics')
    def metrics():
        """Disclosure outputs from the seven versioned metric families."""

    @metrics.command('all')
    @dates
    @click.option('--execution',type=click.Choice(['legacy','views-v1']),default='legacy')
    @options
    def all_metrics(ctx,asof,known_at,watermark,execution,json_output):
        """Compute all families and render a receipt-bearing CSV/Markdown pack."""
        if execution=='views-v1':
            result=native(ctx,asof,known_at,watermark)
            output(dict(status='completed',run_id=result['run_id'],pack=native_pack(ctx,result['run_id'])),json_output);return
        result=execute(ctx,asof,known_at,watermark)
        pack=pack_run(result['run_id'],**roots(ctx,pack=True))
        output(dict(status='completed',run_id=result['run_id'],row_count=len(result['rows']),pack=pack),json_output)

    @cli.command('receipt')
    @click.argument('receipt_id')
    @click.option('--python',type=click.Path(exists=True,path_type=Path),help='Interpreter matching the recorded runtime.')
    @click.option('--bigquery-config',type=click.Path(exists=True,path_type=Path),help='Explicit billing/location boundary for native cloud replay.')
    @options
    def receipt(ctx,receipt_id,python,bigquery_config,json_output):
        """Recompute a metric row from its original snapshot and pinned artifacts."""
        kwargs={'python':python} if python else {}
        if bigquery_config:
            from tl.bigquery.config import Binding
            kwargs['cloud_binding']=Binding.read(bigquery_config)
        output(replay_receipts([receipt_id],db=db_override(ctx),**roots(ctx),**kwargs)[0],json_output)

    @cli.command('pack')
    @click.option('--asof')
    @click.option('--known-at')
    @click.option('--watermark',type=click.IntRange(0))
    @click.option('--run-id',help='Render an existing verified metric run without recalculation.')
    @click.option('--execution',type=click.Choice(['legacy','views-v1']),default='legacy')
    @options
    def pack(ctx,asof,known_at,watermark,run_id,execution,json_output):
        """Render only receipt-verified metrics/out rows into a new pack directory."""
        if run_id and any(value is not None for value in (asof,known_at,watermark)):
            raise ValidationError('choose an existing --run-id or reporting cutoffs, not both')
        if execution=='views-v1':
            if not run_id:
                if not asof: raise ValidationError('pack requires --asof or --run-id')
                run_id=native(ctx,asof,known_at,watermark)['run_id']
            output(native_pack(ctx,run_id),json_output);return
        if not run_id:
            if not asof:
                raise ValidationError('pack requires --asof or --run-id')
            result=execute(ctx,asof,known_at,watermark)
            run_id=result['run_id']
        output(pack_run(run_id,**roots(ctx,pack=True)),json_output)

    @cli.group('archive')
    def archive():
        """Retain and re-perform synthetic reports independently of original paths."""

    @cli.command('restate')
    @click.option('--original-run',required=True,help='Retained originally reported metric run.')
    @click.option('--current-run',required=True,help='Later-known or new-definition run at the same economic asof.')
    @options
    def restate_command(ctx,original_run,current_run,json_output):
        """Re-perform both runs and publish a receipt-backed changed-row bridge."""
        from tl.metrics.changes import restate
        result=restate(original_run,current_run,db=db_override(ctx),**roots(ctx))
        output(dict(run_id=result['run_id'],run_directory=result['run_directory'],rows=result['rows']),json_output)

    @cli.group('sensitivity')
    def sensitivity_commands():
        """Publish every registered alternate lens with an explicit delta unit."""

    @sensitivity_commands.command('nrr')
    @click.option('--run-id',help='Existing NRR-only run; preserves its explicit definition.')
    @click.option('--asof')
    @click.option('--known-at')
    @click.option('--watermark',type=click.IntRange(0))
    @click.option('--definition','version',default=None,help='Default for new sensitivity runs: v2; v3 is opt-in floor demonstration.')
    @options
    def sensitivity_nrr(ctx,run_id,asof,known_at,watermark,version,json_output):
        """Headline and three alternatives, cohort diagnostics, and receipt-backed deltas."""
        from tl.metrics.changes import sensitivity
        if run_id and any(v is not None for v in (asof,known_at,watermark,version)):
            raise ValidationError('choose an existing --run-id or reporting cutoffs/definition, not both')
        if not run_id:
            if not asof:
                raise ValidationError('sensitivity nrr requires --asof or --run-id')
            run_id=execute(ctx,asof,known_at,watermark,names=['consumption_nrr'],version=version or 'v2')['run_id']
        result=sensitivity(run_id,db=db_override(ctx),**roots(ctx))
        output(dict(run_id=result['run_id'],run_directory=result['run_directory'],rows=result['rows']),json_output)

    @archive.command('create')
    @click.argument('destination',type=click.Path(path_type=Path))
    @click.option('--pack',type=click.Path(exists=True,path_type=Path))
    @click.option('--run-id')
    @options
    def archive_create(ctx,destination,pack,run_id,json_output):
        from tl.receipts.archive import create_archive
        output(create_archive(destination,pack=pack,run_id=run_id,db=db_override(ctx),**roots(ctx)),json_output)

    @archive.command('verify')
    @click.argument('archive_path',type=click.Path(exists=True,path_type=Path))
    @click.option('--manifest-sha256',required=True)
    @options
    def archive_verify(ctx,archive_path,manifest_sha256,json_output):
        from tl.receipts.archive import verify_archive
        result=verify_archive(archive_path,manifest_sha256)
        output(dict(status='integrity_verified',run_id=result['run_id'],recomputed=False),json_output)

    @archive.command('replay')
    @click.argument('archive_path',type=click.Path(exists=True,path_type=Path))
    @click.option('--manifest-sha256',required=True)
    @click.option('--receipt-id','ids',multiple=True,help='Default: every row and value in the archived run.')
    @click.option('--python',type=click.Path(exists=True,path_type=Path))
    @options
    def archive_replay(ctx,archive_path,manifest_sha256,ids,python,json_output):
        from tl.receipts.archive import replay_archive
        result=replay_archive(archive_path,manifest_sha256,ids=list(ids),python=python)
        output(dict(status='reproduced',verified_rows=len(result),rows=result),json_output)

    @cli.group('ledger')
    def ledger_commands():
        """Recover an interrupted append without changing prior receipt bytes."""

    @ledger_commands.command('recover')
    @options
    def ledger_recover(ctx,json_output):
        from tl.receipts.journal import recover
        output(recover(roots(ctx).get('ledger',Path('ledger/metrics.jsonl'))),json_output)
