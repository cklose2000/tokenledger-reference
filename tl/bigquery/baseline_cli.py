"""Independent baseline commands; exporting SQL never submits warehouse jobs."""
from decimal import Decimal
from pathlib import Path

import click
import yaml

from tl.bigquery.config import Binding


def prepare(db, binding, directory, *, raw_dataset, model_dataset, asof,
            known_at=None, watermark=None, version='v2'):
    from tl.stream import ValidationError
    if version not in ('v2', 'v3'):
        raise ValidationError('baseline NRR version must be v2 or v3')
    from tl.bigquery.baseline_sql import export_baseline, ROOT
    from tl.bigquery.hashing import fingerprint
    from tl.stream.events import Catalog, iso
    from tl.views.session import ViewSession

    definition = yaml.safe_load((ROOT / f'definitions/metrics/consumption_nrr/{version}.yaml')
                               .read_text(encoding='utf-8'))
    headline = next(lens for lens in definition['lenses'] if lens['lens'] == 'base_t12m')
    cents = int(Decimal(str(headline['floor_usd'])) * 100)
    # Read the same physical, knowledge-filtered source used by native export.
    # This does not calculate any baseline recognition from the spine graph.
    with ViewSession(db, asof=asof, known_at=known_at, watermark=watermark) as session:
        identity = fingerprint(session.conn.execute('SELECT * FROM _visible').to_arrow_table(),
                               ['activity_id'], source=True)
        manifest = export_baseline(binding, directory, raw_dataset=raw_dataset,
            model_dataset=model_dataset, asof=asof, known_at=iso(session.known_at),
            watermark=session.watermark, first_month=session.first_month.isoformat(),
            nrr_version=version, nrr_floor_cents=cents, input_identity=identity,
            schema_identity=Catalog(ROOT / 'definitions/activities').digest)
    from tl.bigquery.baseline_sql import verify_export
    verified = verify_export(directory)
    return dict(status='exported', directory=str(Path(directory).resolve()),
        manifest_sha256=verified['manifest_sha256'], source=manifest['source'],
        cutoffs=manifest['cutoffs'], definition=manifest['definition'], counts=manifest['counts'],
        native_execution='not_run', native_parity='not_run', cloud_baseline_performance='not_run')


def register(group, options, output):
    @group.command('baseline-run')
    @click.option('--baseline-export', type=click.Path(exists=True, path_type=Path), required=True)
    @click.option('--native-export', type=click.Path(exists=True, path_type=Path), required=True)
    @click.option('--baseline-sha256', help='Externally frozen baseline manifest hash; requires --native-sha256.')
    @click.option('--native-sha256', help='Externally frozen native manifest hash; requires --baseline-sha256.')
    @click.option('--worker-python', type=click.Path(exists=True, path_type=Path), required=True)
    @click.option('--threads', type=click.IntRange(1, 4), default=4)
    @click.option('--architecture-order', type=click.Choice(['baseline-first', 'native-first']),
                  default='baseline-first', show_default=True)
    @click.option('--output', 'directory', type=click.Path(path_type=Path), required=True)
    @options
    def baseline_run(ctx, baseline_export, native_export, baseline_sha256, native_sha256,
                     worker_python, threads, architecture_order, directory, json_output):
        """Run one bounded native baseline/spine correctness trial in the pinned dbt worker."""
        import hashlib
        import json
        import os
        import re
        import subprocess
        import uuid
        from dataclasses import asdict
        from tl.bigquery.baseline_runner import preflight
        from tl.bigquery.baseline_sql import ROOT, verify_export
        from tl.stream import ValidationError
        from tl.stream.events import canonical
        if (baseline_sha256 is None) != (native_sha256 is None):
            raise ValidationError('external baseline and native manifest hashes must be supplied together')
        if baseline_sha256 is not None and any(not re.fullmatch('[0-9a-f]{64}', value)
                                               for value in (baseline_sha256, native_sha256)):
            raise ValidationError('external manifest hashes must be lowercase SHA-256 hex strings')
        binding = Binding.read(ctx.obj['bigquery_config'])
        root = directory.absolute()
        if root.exists():
            raise ValidationError('native trial output must be a fresh directory')
        baseline_export, native_export = baseline_export.absolute(), native_export.absolute()
        verify_export(baseline_export, expected_manifest_sha256=baseline_sha256)
        pins = (dict(baseline_sha256=baseline_sha256, native_sha256=native_sha256)
                if baseline_sha256 is not None else dict(
                    baseline_sha256=hashlib.sha256((baseline_export / 'manifest.json').read_bytes()).hexdigest(),
                    native_sha256=hashlib.sha256((native_export / 'manifest.json').read_bytes()).hexdigest()))
        preflight(binding, baseline_export, native_export, **pins)
        root.parent.mkdir(parents=True, exist_ok=True)
        stem = root.parent / (root.name + '-worker-' + uuid.uuid4().hex)
        request_path, result_path = Path(str(stem) + '.request.json'), Path(str(stem) + '.result.json')
        request = dict(binding=asdict(binding), baseline_directory=str(baseline_export),
            native_directory=str(native_export), directory=str(root), threads=threads,
            architecture_order=architecture_order, **pins)
        request_path.write_text(canonical(request) + '\n', encoding='utf-8', newline='\n')
        env = {**os.environ, 'PYTHONPATH': str(ROOT), 'PYTHONNOUSERSITE': '1',
               'DBT_SEND_ANONYMOUS_USAGE_STATS': 'false', 'DO_NOT_TRACK': '1'}
        with Path(str(stem) + '.log').open('x', encoding='utf-8') as log:
            process = subprocess.run([str(worker_python.resolve()), '-m', 'tl.bigquery.baseline_worker',
                '--request', str(request_path), '--result', str(result_path)], cwd=ROOT,
                stdout=log, stderr=subprocess.STDOUT, env=env)
        if not result_path.is_file():
            raise ValidationError('native worker did not finish; retained log: ' + str(stem) + '.log')
        result = json.loads(result_path.read_text(encoding='utf-8'))
        output(result, json_output)
        if process.returncode != 0 or result['status'] != 'equivalent':
            raise click.exceptions.Exit(1)

    @group.command('baseline-export')
    @click.option('--raw-dataset', required=True)
    @click.option('--model-dataset', required=True)
    @click.option('--asof', required=True)
    @click.option('--known-at')
    @click.option('--watermark', type=click.IntRange(1))
    @click.option('--definition', 'version', type=click.Choice(['v2', 'v3']), default='v2')
    @click.option('--output', 'directory', type=click.Path(path_type=Path), required=True)
    @options
    def baseline_export(ctx, raw_dataset, model_dataset, asof, known_at, watermark,
                        version, directory, json_output):
        """Freeze an independent native dbt baseline from the local source; no cloud calls."""
        result = prepare(ctx.obj['db'], Binding.read(ctx.obj['bigquery_config']), directory,
            raw_dataset=raw_dataset, model_dataset=model_dataset, asof=asof,
            known_at=known_at, watermark=watermark, version=version)
        output(result, json_output)

    @group.command('baseline-verify-export')
    @click.argument('directory', type=click.Path(exists=True, path_type=Path))
    @options
    def verify(ctx, directory, json_output):
        """Verify frozen baseline SQL bytes offline; no parity or execution inferred."""
        from tl.bigquery.baseline_sql import verify_export
        output(verify_export(directory), json_output)
