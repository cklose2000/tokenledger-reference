"""Jev token economics: exact OpenRouter probe, then a late hiked invoice."""
from pathlib import Path
from time import perf_counter
import json, uuid

from tl.generate import Config, SyntheticWorld
from tl.metrics.definitions import FAMILIES
from tl.receipts.metrics import append_receipts, read_receipts, receipt_id
from tl.stream import Activity, Stream, ValidationError
from tl.stream.events import canonical
from tl.views.engine import profile
from tl.views.pack import pack_run
from tl.demo.bridge import create
from tl.demo.yield_bridge import create as create_yield

ASOF = "2026-09-30"
ORIGINAL = "2026-10-01T00:00:00Z"
CURRENT = "2026-10-03T00:00:00Z"
MODEL = "typesafe-jev-1.13"
BATCH = "demo:jev-openrouter"
CUSTOMER = "customer:0000001"
PROBE = Path(__file__).resolve().parents[2] / "docs" / "examples" / "jev-openrouter-probe.json"


def _probe():
    return json.loads(PROBE.read_text(encoding="utf-8"))


def run(destination=None, *, customers=7, progress=None):
    started = perf_counter()
    probe = _probe()
    worked = probe["worked_close"]
    root = Path(destination or Path("data/demo-jev") / uuid.uuid4().hex).resolve()
    if root.exists() and any(root.iterdir()):
        raise ValidationError("demo-jev requires a new or empty directory; existing evidence is never overwritten")
    root.mkdir(parents=True, exist_ok=True)
    db = root / "world.duckdb"
    outputs = root / "runs"
    ledger = root / "metrics.jsonl"

    def step(message):
        if progress:
            progress(message)

    step("Generate a tiny synthetic world, then admit Jev usage at the observed list prices.")
    stream = Stream(db)
    stream.init()
    generated = stream.generate(
        SyntheticWorld(Config(customers=customers, months=3, start="2026-07-01")), workers=1
    )
    released = Activity(
        BATCH + ":model",
        "2026-09-15T00:00:00Z",
        "model_released",
        dict(model=MODEL, generation=1, release_date="2026-09-15", training_cost_usd_estimate="0.00"),
        CUSTOMER,
    )
    inbound = Activity(
        BATCH + ":input",
        "2026-09-18T00:00:00Z",
        "tokens_processed",
        dict(
            usage_batch_id=BATCH,
            model=MODEL,
            model_release_date="2026-09-15",
            token_type="input",
            tokens=worked["input_tokens"],
            list_price_per_mtok=worked["original_list_usd_per_mtok"],
            effective_price_per_mtok=worked["original_list_usd_per_mtok"],
            channel="direct",
            workload="api",
        ),
        CUSTOMER,
    )
    outbound = Activity(
        BATCH + ":output",
        "2026-09-18T00:00:00Z",
        "tokens_processed",
        dict(
            usage_batch_id=BATCH,
            model=MODEL,
            model_release_date="2026-09-15",
            token_type="output",
            tokens=worked["output_tokens"],
            list_price_per_mtok=probe["observed"]["list_price_output_usd_per_mtok"],
            effective_price_per_mtok=0.0,
            channel="direct",
            workload="api",
        ),
        CUSTOMER,
    )
    stream.append_simulation(
        [(released, "2026-09-15T00:00:00Z"), (inbound, "2026-09-18T00:00:00Z"), (outbound, "2026-09-18T00:00:00Z")],
        source="sim:demo-jev-v1",
        actor="synthetic-demo",
    )
    names = [*FAMILIES, "close_accounts"]
    step("Report September with native views. The Jev tokens are known; the hiked invoice is not.")
    original = profile(db, asof=ASOF, known_at=ORIGINAL, names=names, output_root=outputs, ledger=ledger)
    step("Admit the mid-month list-price hike as a late usage invoice, then report the same September.")
    invoice = Activity(
        BATCH + ":invoice",
        "2026-09-18T00:00:00Z",
        "usage_invoiced",
        dict(
            invoice_id=BATCH,
            usage_batch_id=BATCH,
            contract_id=None,
            period_start="2026-09-01",
            period_end="2026-10-01",
            gross_usd=worked["hiked_invoice_usd"],
            discount_usd="0.00",
            net_usd=worked["hiked_invoice_usd"],
            channel_fee_usd="0.00",
            commit_applied_usd="0.00",
            cash_due_usd=worked["hiked_invoice_usd"],
            channel="direct",
        ),
        CUSTOMER,
    )
    injected = stream.append_simulation([(invoice, "2026-10-02T12:00:00Z")], source="sim:demo-jev-v1", actor="synthetic-demo")
    current = profile(db, asof=ASOF, known_at=CURRENT, names=names, output_root=outputs, ledger=ledger)
    records = read_receipts(ledger)
    account_id = lambda run: next(
        k
        for k in run["receipt_ids"]
        if records[k]["schema_version"] == "tokenledger-views/v1" and records[k]["result"]["output"] == "close_accounts"
    )
    yield_id = lambda run: next(
        k
        for k in run["receipt_ids"]
        if records[k]["schema_version"] == "tokenledger-views/v1" and records[k]["result"]["output"] == "net_rev_per_mtok"
    )
    step("Reperform both closes and explain the yield change with the hiked invoice.")
    bridge = create(account_id(original), account_id(current), db=db, output_root=outputs, ledger=ledger)
    business = create_yield(yield_id(original), yield_id(current), bridge["receipt_id"], db=db, output_root=outputs, ledger=ledger)
    step("Reproduce every original KPI population after the append.")
    pack = pack_run(original["run_id"], output_root=outputs, ledger=ledger, pack_root=root / "pack")
    verified_ids = [k for k in original["receipt_ids"] if records[k]["schema_version"] == "tokenledger-views/v1"]
    observed = probe["observed"]
    manifest = dict(
        schema_version="tokenledger-demo-jev/v1",
        asof=ASOF,
        synthetic=True,
        execution="views-v1",
        database=str(db),
        artifact_root=str(root),
        generation=generated,
        injection=injected,
        observed_probe=observed,
        worked_close=worked,
        original_run_id=original["run_id"],
        current_run_id=current["run_id"],
        original_receipt_ids=verified_ids,
        bridge_receipt_id=bridge["receipt_id"],
        yield_bridge_receipt_id=business["receipt_id"],
        pack=pack,
        observed_workflow_seconds=perf_counter() - started,
        timing_scope="Generation through reports, bridges and original pack; excludes summary serialization",
        target_seconds=60,
        bigquery="not_run",
        matched_agent_benchmark="not_run",
    )
    parent = records[account_id(current)]
    observation = dict(
        schema_version="tokenledger-demo-observation/v1",
        run_id=uuid.uuid4().hex,
        result=manifest,
        **{k: parent[k] for k in ("inputs", "definition_version", "query_hash", "git_sha", "execution_hash", "asof", "known_at", "watermark")},
    )
    observation["receipt_id"] = receipt_id(observation)
    append_receipts(ledger, [observation])
    manifest["receipt_id"] = observation["receipt_id"]
    (root / "demo.json").write_text(canonical(manifest) + "\n", encoding="utf-8", newline="\n")
    lines = [
        "# Jev token economics",
        "",
        "One real OpenRouter Decisions call, then a worked September close at the same rates.",
        "The live call is below one USD cent, so the close scales input tokens to 1,000,000.",
        "A later invoice bills the same batch at double the list price. Tokens do not change.",
        "The original receipt still reproduces.",
        "",
        "## Observed OpenRouter call",
        "",
        "| Field | Value |",
        "|---|---|",
        f"| When | {observed['at']} |",
        f"| Route | {observed['route']} |",
        f"| Model | `{observed['model']}` |",
        f"| Input tokens | {observed['input_tokens']} |",
        f"| Output tokens | {observed['output_tokens']} |",
        f"| List input USD/Mtok | {observed['list_price_input_usd_per_mtok']} |",
        f"| List output USD/Mtok | {observed['list_price_output_usd_per_mtok']} |",
        f"| Exact cost USD | {observed['cost_usd']} |",
        f"| Identity | {observed['identity']} |",
        "",
        "## Worked close (same rates, integer cents)",
        "",
        f"Input {worked['input_tokens']} tokens at ${worked['original_list_usd_per_mtok']}/Mtok rates to ${worked['original_rated_usd']}.",
        f"Output {worked['output_tokens']} tokens remain free. The hiked invoice is ${worked['hiked_invoice_usd']}.",
        "",
        business["revenue_basis"] + ".",
        "",
        business["denominator"] + ".",
        "",
        "| Measure | Original | Currently known | Delta | Unit | Receipt |",
        "|---|---:|---:|---:|---|---|",
    ]
    for field, unit in business["units"].items():
        lines.append(
            "| "
            + field
            + " | "
            + " | ".join(str(business[k][field]) for k in ("original", "current", "delta"))
            + " | "
            + unit
            + " | "
            + business["receipt_id"]
            + " |"
        )
    lines.extend(
        [
            "",
            business["aggregation"] + ". " + business["rounding"] + ".",
            "",
            "The Jev tokens were already present at the original list price. One newly admitted invoice bills the batch at the hiked rate. Debit and credit entries explain that single revenue change.",
            "",
            "## Account bridge",
            "",
            bridge["scope"],
            "",
            "| Month | Account | Before cents | After cents | Delta cents | Receipt |",
            "|---|---|---:|---:|---:|---|",
        ]
    )
    for row in bridge["rows"]:
        lines.append(
            "| "
            + " | ".join(str(row[k]) for k in ("month", "account", "before_cents", "after_cents", "delta_cents"))
            + " | "
            + bridge["receipt_id"]
            + " |"
        )
    lines.extend(
        [
            "",
            f"Responsible activity: `{invoice.activity_id}`. {bridge['causal_basis']}.",
            "",
            f"Workflow observation: {manifest['observed_workflow_seconds']:.3f} seconds; receipt `{observation['receipt_id']}`.",
            "",
            "Replay:",
            "",
            "```powershell",
            f"tl --db '{db}' --artifact-root '{root}' receipt {business['receipt_id']} --json",
            f"tl --db '{db}' --artifact-root '{root}' receipt {business['original_receipt_id']} --json",
            "```",
            "",
            "No API key, account identifier or private application database is in this directory.",
        ]
    )
    (root / "README.md").write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
    return dict(
        status="demonstrated",
        artifact_root=str(root),
        observed_probe=observed,
        worked_close=worked,
        bridge=bridge,
        yield_bridge=business,
        original_receipt_ids=verified_ids,
        original_reproduced=True,
        pack=pack,
        observed_workflow_seconds=manifest["observed_workflow_seconds"],
        observation_receipt_id=observation["receipt_id"],
    )
