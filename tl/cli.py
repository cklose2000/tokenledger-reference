"""Noninteractive local CLI. Unimplemented phases are not advertised as working."""

import hashlib
import json
from pathlib import Path
from datetime import date, datetime, time, timedelta
from decimal import Decimal
import platform
import sys
from time import perf_counter

import click
import duckdb
import numpy
import pyarrow

from tl import __version__
from tl.generate import Config, SyntheticWorld
from tl.stream import Activity, Stream, StreamReader, ValidationError
from tl.stream.events import UTC, canonical, iso
from tl.stream.validation import validate_stream


def output(value, json_output):
    click.echo(canonical(value) if json_output else json.dumps(value, indent=2))


def options(function):
    function = click.option("--json", "json_output", is_flag=True, help="Machine-readable output.")(function)
    return click.pass_context(function)


@click.group()
@click.option("--db", type=click.Path(path_type=Path), default=Path("data/tokenledger.duckdb"), show_default=True,
              envvar="TL_DB", help="Local DuckDB file.")
@click.option("--definitions", type=click.Path(path_type=Path), default=Path("definitions/activities"), show_default=True)
@click.option("--artifact-root", type=click.Path(path_type=Path),
              help="Explicit metric ledger, runs, and pack root; use a scratch directory for demos.")
@click.version_option(__version__)
@click.pass_context
def cli(ctx, db, definitions, artifact_root):
    """Synthetic reporting from a single activity stream."""
    ctx.obj = dict(db=db, definitions=definitions, artifact_root=artifact_root)


@cli.command("init")
@options
def initialize(ctx, json_output):
    """Create the canonical stream and temporal compatibility view."""
    stream = Stream(**_stream_args(ctx))
    stream.init()
    output({"status": "initialized", "db": str(stream.path), "catalog_size": len(stream.catalog.schemas),
            "catalog_hash": stream.catalog.digest, "physical_columns": 14}, json_output)


def _stream_args(ctx):
    return {"path": ctx.obj["db"], "definitions": ctx.obj["definitions"]}


@cli.command()
@click.option("--seed", type=click.IntRange(0, 2**32-1), default=42, show_default=True)
@click.option("--months", type=click.IntRange(1, 120), default=30, show_default=True)
@click.option("--customers", type=click.IntRange(1, 1_000_000), default=50000, show_default=True)
@click.option("--start", default="2024-02-01", show_default=True)
@click.option("--profile", type=click.Choice(["default", "anthropic-like"]), default="default")
@click.option("--workers", type=click.IntRange(1, 8), default=4, show_default=True,
              help="Preparation processes; the database still has one writer.")
@options
def generate(ctx, seed, months, customers, start, profile, workers, json_output):
    """Append a deterministic synthetic world; exact retries insert no duplicates."""
    config = Config(seed, months, customers, start, profile)
    world = SyntheticWorld(config)
    stream = Stream(**_stream_args(ctx))
    with StreamReader(stream.path).connect() as conn:
        other = conn.execute("SELECT count(*) FROM stream.activity WHERE _source != ?", [world.prefix]).fetchone()[0]
        if other:
            raise ValidationError("generate requires an empty stream or the identical world; use a different --db")
        new_world = conn.execute("SELECT count(*)=0 FROM stream.activity").fetchone()[0]
    started = perf_counter()
    last_progress = started

    def progress(customers_done):
        nonlocal last_progress
        if perf_counter() - last_progress > 20:
            click.echo(f"Validated generation: {customers_done}/{customers} customers", err=True)
            last_progress = perf_counter()

    result = stream.generate(world, workers=workers, progress=progress, new_world=new_world)
    elapsed = perf_counter() - started
    manifest = world.manifest()
    artifacts = stream.path.parent / "manifests"
    artifacts.mkdir(parents=True, exist_ok=True)
    manifest_path = artifacts / f"{config.run_id}.json"
    content = canonical(manifest) + "\n"
    if manifest_path.exists() and manifest_path.read_text(encoding="utf-8") != content:
        raise ValidationError("existing source manifest differs; do not replace evidence")
    if not manifest_path.exists():
        manifest_path.write_text(content, encoding="utf-8", newline="\n")
    output({"status": "generated", "synthetic": True, "run_id": config.run_id, **result,
            "elapsed_seconds": round(elapsed, 6), "workers": workers, "source_manifest": str(manifest_path),
            "manifest_sha256": hashlib.sha256(content.encode()).hexdigest(),
            "runtime": {"python": platform.python_version(), "duckdb": duckdb.__version__,
                        "numpy": numpy.__version__, "pyarrow": pyarrow.__version__}}, json_output)


@cli.command()
@click.option("--reconcile-sources", is_flag=True,
              help="Reconcile upstream synthetic manifests by period, count and cents.")
@click.option("--source-manifest", type=click.Path(exists=True, path_type=Path),
              help="Explicit manifest; otherwise discover a manifest for every stream source.")
@options
def validate(ctx, reconcile_sources, source_manifest, json_output):
    """Revalidate every row and cross-event financial relationships."""
    if source_manifest and not reconcile_sources:
        raise ValidationError("--source-manifest requires --reconcile-sources")
    manifests = None
    if reconcile_sources:
        manifests = [source_manifest] if source_manifest else list((ctx.obj["db"].parent / "manifests").glob("*.json"))
        with StreamReader(ctx.obj["db"]).connect() as conn:
            sources = {row[0] for row in conn.execute("SELECT DISTINCT _source FROM stream.activity").fetchall()}
        matches = {}
        for path in manifests:
            source = json.loads(path.read_text(encoding="utf-8"))["source"]
            if source in sources:
                matches[source] = path
        if sources != matches.keys() or not sources:
            raise ValidationError("a source manifest is required for every stream source")
        manifests = list(matches.values())
    result = validate_stream(ctx.obj["db"], ctx.obj["definitions"], source_manifest=manifests)
    output(result, json_output)
    if not result["passed"]:
        raise click.exceptions.Exit(1)


@cli.command()
@click.argument("source_file", type=click.Path(exists=True, path_type=Path))
@click.option("--source", required=True, help="Stable, non-secret source name.")
@options
def ingest(ctx, source_file, source, json_output):
    """Validate and atomically append Activity JSONL, stamping actual arrival time."""
    def events():
        with source_file.open(encoding="utf-8") as handle:
            for number, line in enumerate(handle, 1):
                try:
                    yield Activity(**json.loads(line))
                except (TypeError, ValueError) as exc:
                    raise ValidationError(f"invalid activity on input line {number}: {exc}") from exc
    output(Stream(**_stream_args(ctx)).append(events(), source=source), json_output)


@cli.group("stream")
def stream_commands():
    """Read or export source evidence."""


@stream_commands.command("export")
@click.argument("destination", type=click.Path(path_type=Path))
@options
def export_stream(ctx, destination, json_output):
    """Write canonical JSONL, excluding physical row positions. Refuse overwrite."""
    output(StreamReader(ctx.obj["db"]).export(destination), json_output)


@stream_commands.command("snapshot")
@click.option("--asof", required=True, help="Inclusive economic end date, YYYY-MM-DD.")
@click.option("--known-at", help="Exclusive arrival cutoff, with explicit timezone.")
@click.option("--watermark", type=click.IntRange(0), help="Freeze an earlier committed stream position.")
@click.option("--output", "destination", type=click.Path(path_type=Path), required=True)
@options
def snapshot_stream(ctx, asof, known_at, watermark, destination, json_output):
    """Export an ActivitySchema view at economic and knowledge cutoffs."""
    if destination.exists():
        raise ValidationError("snapshot output already exists")
    reader = StreamReader(ctx.obj["db"])
    with reader.connect() as conn:
        latest = conn.execute("SELECT coalesce(max(_stream_position),0) FROM stream.activity").fetchone()[0]
    if watermark is None:
        watermark = latest
    if watermark > latest:
        raise ValidationError("watermark exceeds the committed stream")
    known_at = known_at or iso(datetime.combine(date.fromisoformat(asof) + timedelta(days=1), time(), UTC))
    result = reader.snapshot(asof=asof, known_at=known_at, watermark=watermark)
    digest = hashlib.sha256()
    with destination.open("xb") as handle:
        for batch in result.to_batches(max_chunksize=8192):
            for row in batch.to_pylist():
                for name, value in row.items():
                    if isinstance(value, datetime):
                        row[name] = iso(value)
                    elif isinstance(value, Decimal):
                        row[name] = format(value, ".2f")
                row["feature_json"] = json.loads(row["feature_json"])
                line = (canonical(row) + "\n").encode()
                handle.write(line)
                digest.update(line)
    output({"path": str(destination), "asof": asof, "known_at": known_at, "watermark": watermark,
            "rows": result.num_rows, "sha256": digest.hexdigest()}, json_output)


@cli.group("process")
def process_commands():
    """Record build work using the brief's provisional process contract."""


@process_commands.command("record")
@click.option("--phase", required=True)
@click.option("--actor", required=True, help="Actual model/operator making this record; a claim, not authenticated identity.")
@click.option("--role", type=click.Choice(["executive", "worker", "qa", "integrator"]), required=True)
@click.option("--work-unit", required=True)
@click.option("--output", "outputs", multiple=True, required=True)
@click.option("--verification", multiple=True, required=True)
@options
def process_record(ctx, phase, actor, role, work_unit, outputs, verification, json_output):
    """Append an idempotent process receipt with output hashes and input commit."""
    from tl.receipts.process import record_process
    output(record_process(root=Path.cwd(), actor=actor, role=role, phase=phase, work_unit=work_unit,
                          outputs=list(outputs), verification=list(verification)), json_output)


from tl.metrics.cli import register as register_metrics
register_metrics(cli,options,output)
from tl.controls.cli import register as register_controls
register_controls(cli,options,output)
from tl.compare.cli import register as register_compare
register_compare(cli,options,output)
from tl.usage.cli import register as register_usage
register_usage(cli,options,output)
from tl.views.cli import register as register_views
register_views(cli,options,output)
from tl.demo.cli import register as register_demo
register_demo(cli,options,output)
from tl.release import register as register_release
register_release(cli,options,output)
from tl.challenge.cli import register as register_challenge
register_challenge(cli,options,output)
from tl.bigquery.cli import register as register_bigquery
register_bigquery(cli,options,output)


def main():
    try:
        result=cli(standalone_mode=False)
        # Click returns Exit codes in non-standalone mode. Preserve the process
        # status as well as the JSON status for schedulers and evidence gates.
        if type(result) is int:
            raise SystemExit(result)
    except click.exceptions.Exit as exc:
        raise SystemExit(exc.exit_code)
    except (ValidationError, duckdb.Error, OSError, ValueError, click.ClickException) as exc:
        # Even failed commands honor the machine-output contract.
        if "--json" in sys.argv:
            click.echo(canonical({"status": "error", "error": str(exc)}))
        else:
            click.echo(f"Error: {exc}", err=True)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
