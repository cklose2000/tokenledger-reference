"""Optional public projection of retained synthetic cloud comparisons.

The projection omits operational bindings. Verification recalculates retained
populations, not cloud SQL or the source stream. No credentials are consulted.
"""
from contextlib import contextmanager
import hashlib
import io
import json
import math
from pathlib import Path, PurePosixPath
import socket
import statistics
import uuid
import zipfile

import click
import pyarrow.compute as pc
import pyarrow.parquet as pq

from tl.bigquery.hashing import fingerprint
from tl.compare.matching import compare_tables
from tl.stream import ValidationError


DEFAULT_MANIFEST = Path('evidence/bigquery/manifest.json')


def sha_file(path):
    value = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            value.update(block)
    return value.hexdigest()


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValidationError('duplicate public evidence JSON key')
        result[key] = value
    return result


def load_manifest(path):
    raw = Path(path).read_bytes()
    value = json.loads(raw, object_pairs_hook=_unique)
    if value.get('schema_version') != 'tokenledger-public-cloud-projection/v1':
        raise ValidationError('unsupported public evidence projection')
    names = list(value['files'])
    if not names or len(names) != len({n.casefold() for n in names}):
        raise ValidationError('empty or case-colliding public evidence inventory')
    for name, spec in value['files'].items():
        path = PurePosixPath(name)
        if (path.is_absolute() or '\\' in name or ':' in name
                or any(p in ('', '.', '..') for p in name.split('/'))
                or path.suffix != '.parquet' or not 0 < spec['bytes'] <= 256_000_000):
            raise ValidationError('invalid public population path or size')
    expected = []
    for pair in value['pairs']:
        if set(pair['outputs']) != set(value['outputs']):
            raise ValidationError('pair lacks a registered output')
        for row in pair['outputs'].values():
            expected.extend([row['native'], row['baseline']])
    if len(expected) != len(set(expected)) or set(expected) != set(names):
        raise ValidationError('population inventory is not exactly one file per side and output')
    if len({p['case'] for p in value['pairs']}) != len(value['pairs']):
        raise ValidationError('duplicate pair identity')
    return value, hashlib.sha256(raw).hexdigest()


@contextmanager
def offline():
    targets = [(socket, 'create_connection'), (socket, 'getaddrinfo'),
               (socket.socket, 'connect'), (socket.socket, 'connect_ex')]
    originals = [getattr(obj, name) for obj, name in targets]
    def refuse(*args, **kwargs):
        raise ValidationError('public retained verification forbids network access')
    try:
        for obj, name in targets:
            setattr(obj, name, refuse)
        yield
    finally:
        for (obj, name), original in zip(targets, originals):
            setattr(obj, name, original)


def measured_summary(manifest):
    """Derive resource results from the selected retained job counters."""
    measured = []
    all_jobs = set()
    for pair in manifest['pairs']:
        if any(j['side'] not in ('baseline', 'native') for j in pair['jobs']):
            raise ValidationError('unknown architecture in job evidence')
        if any(type(pair['accounted_wall_seconds'][side]) not in (int, float)
               or not math.isfinite(pair['accounted_wall_seconds'][side])
               or pair['accounted_wall_seconds'][side] <= 0
               for side in ('baseline', 'native')):
            raise ValidationError('unobserved or invalid accounted wall time')
        totals = {}
        for side in ('baseline', 'native'):
            jobs = [j for j in pair['jobs'] if j['side'] == side]
            if not jobs:
                raise ValidationError('missing architecture job evidence')
            for job in jobs:
                if job['key'] in all_jobs or job['state'] != 'DONE' or job['cache_hit'] is not False:
                    raise ValidationError('duplicate, incomplete or cached job in matched evidence')
                all_jobs.add(job['key'])
                if any(type(job[k]) is not int or job[k] < 0 for k in ('billed_bytes', 'slot_ms')):
                    raise ValidationError('unobserved resource counter cannot be treated as zero')
            totals[side] = dict(jobs=len(jobs), billed_bytes=sum(j['billed_bytes'] for j in jobs),
                                slot_ms=sum(j['slot_ms'] for j in jobs),
                                accounted_wall_seconds=pair['accounted_wall_seconds'][side])
        if pair['role'] == 'measured':
            measured.append(dict(case=pair['case'], **totals))
        elif pair['role'] != 'warmup':
            raise ValidationError('unknown pair role')
    if not measured:
        raise ValidationError('no measured pairs')
    medians = {side: {name: statistics.median(p[side][name] for p in measured)
                      for name in ('billed_bytes', 'slot_ms', 'accounted_wall_seconds')}
               for side in ('baseline', 'native')}
    base, native = medians['baseline'], medians['native']
    if not base['billed_bytes'] or not base['slot_ms'] or not base['accounted_wall_seconds']:
        raise ValidationError('zero comparison denominator')
    return dict(measured_pairs=len(measured), all_jobs=len(all_jobs), medians=medians,
                native_billed_bytes_reduction_pct=100*(1-native['billed_bytes']/base['billed_bytes']),
                native_slot_time_ratio=native['slot_ms']/base['slot_ms'],
                ratio_of_median_accounted_wall=native['accounted_wall_seconds']/base['accounted_wall_seconds'],
                median_paired_accounted_wall_ratio=statistics.median(
                    p['native']['accounted_wall_seconds']/p['baseline']['accounted_wall_seconds'] for p in measured),
                pairs=measured, cost_basis='retained job counters; not an invoice')


def verify_archive(manifest_path, archive_path, *, progress=lambda message: None):
    manifest, manifest_sha = load_manifest(manifest_path)
    archive = Path(archive_path)
    if archive.stat().st_size != manifest['archive']['bytes'] or sha_file(archive) != manifest['archive']['sha256']:
        raise ValidationError('archive differs from the release-pinned digest or size')
    checks = []
    with offline(), zipfile.ZipFile(archive) as zipped:
        info = zipped.infolist()
        if len(info) != len(manifest['files']) or {i.filename for i in info} != set(manifest['files']):
            raise ValidationError('archive inventory differs from the release manifest')
        for item in info:
            if item.file_size != manifest['files'][item.filename]['bytes'] or item.flag_bits & 1:
                raise ValidationError('archive member size or encryption differs')
        for pair in manifest['pairs']:
            for name, refs in pair['outputs'].items():
                tables, populations = {}, {}
                spec = manifest['outputs'][name]
                for side in ('baseline', 'native'):
                    filename = refs[side]
                    expected = manifest['files'][filename]
                    raw = zipped.read(filename)
                    if hashlib.sha256(raw).hexdigest() != expected['sha256']:
                        raise ValidationError('retained population bytes changed: '+filename)
                    table = pq.read_table(io.BytesIO(raw))
                    if set(table.column_names) != set(spec['columns']) | {'receipt_id'}:
                        raise ValidationError('retained population schema changed: '+filename)
                    ids = pc.unique(table['receipt_id']).to_pylist()
                    if len(table) and ids != [expected['original_receipt_id']]:
                        raise ValidationError('row receipt identity changed: '+filename)
                    table = table.drop(['receipt_id'])
                    population = fingerprint(table, spec['key'])
                    if population != expected['population']:
                        raise ValidationError('retained logical fingerprint changed: '+filename)
                    tables[side], populations[side] = table, population
                result = compare_tables(tables['native'], tables['baseline'], spec['key'])
                if result['status'] != 'equivalent' or result != refs['comparison']:
                    raise ValidationError('comparison differs from accepted evidence: '+pair['case']+'/'+name)
                checks.append(dict(case=pair['case'], output=name, comparison=result, populations=populations))
                progress(pair['case']+'/'+name+' verified')
                del tables, populations, table, raw
    return dict(status='verified', manifest_sha256=manifest_sha, archive_sha256=manifest['archive']['sha256'],
                comparisons=len(checks), population_fingerprints=2*len(checks), results=checks,
                measurements=measured_summary(manifest), cloud_sql_recomputed=False,
                source_stream_recomputed=False, no_cloud_jobs_submitted=True,
                scope='Public projection: retained population comparisons and selected job counters; original operational seals are outside this package')


def register(release, options, output):
    @release.group('evidence')
    def evidence():
        """Inspect or re-perform the optional public cloud evidence, offline."""

    @evidence.command('verify')
    @click.option('--archive', required=True, type=click.Path(exists=True, dir_okay=False, path_type=Path))
    @click.option('--manifest', default=DEFAULT_MANIFEST, type=click.Path(exists=True, dir_okay=False, path_type=Path))
    @click.option('--output', 'destination', type=click.Path(path_type=Path), help='New directory for the shared observation receipt.')
    @options
    def verify_command(ctx, archive, manifest, destination, json_output):
        from tl.compare.engine import record_observation
        root = destination or Path('data/cloud-review')/uuid.uuid4().hex
        if root.exists():
            raise ValidationError('evidence output directory must be new')
        value, manifest_sha = load_manifest(manifest)
        result = verify_archive(manifest, archive, progress=lambda message: click.echo(message, err=True))
        # Recheck the release manifest before emitting the recorded observation.
        if sha_file(manifest) != manifest_sha:
            raise ValidationError('release manifest changed during verification')
        observed = record_observation(result, inputs=value['inputs'], project='baseline',
                                      output_root=root/'runs', ledger=root/'metrics.jsonl')
        output(dict(status='verified', comparisons=result['comparisons'], population_fingerprints=result['population_fingerprints'],
                    receipt_id=observed['receipt_id'], artifact_root=str(root),
                    measurements=result['measurements'], cloud_sql_recomputed=False,
                    source_stream_recomputed=False, no_cloud_jobs_submitted=True), json_output)
