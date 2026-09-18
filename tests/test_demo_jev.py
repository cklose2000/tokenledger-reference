from pathlib import Path
import json

from click.testing import CliRunner

from tl.cli import cli
from tl.metrics.engine import replay_receipts
from tl.receipts.metrics import read_receipts


def test_demo_jev_hike_keeps_tokens_and_replays_original(tmp_path):
    root = tmp_path / "jev"
    shared = Path("ledger/metrics.jsonl").read_bytes()
    result = CliRunner().invoke(cli, ["demo-jev", "--directory", str(root), "--json"])
    assert result.exit_code == 0, result.output
    answer = json.loads(next(line for line in result.output.splitlines() if line.startswith("{")))
    assert Path("ledger/metrics.jsonl").read_bytes() == shared
    assert answer["status"] == "demonstrated" and answer["original_reproduced"]
    observed = answer["observed_probe"]
    assert observed["input_tokens"] == 307 and observed["output_tokens"] == 20
    assert observed["cost_usd"] == "0.000012894"
    assert observed["list_price_input_usd_per_mtok"] == 0.042
    business = answer["yield_bridge"]
    assert business["delta"]["tokens"] == 0 and business["original"]["tokens"] > 0
    assert business["delta"]["net_revenue_cents"] == 8400
    assert business["responsible_activity_ids"] == ["demo:jev-openrouter:invoice"]
    records = read_receipts(root / "metrics.jsonl")
    ids = [
        *answer["original_receipt_ids"],
        answer["bridge"]["receipt_id"],
        business["receipt_id"],
        answer["observation_receipt_id"],
    ]
    replayed = replay_receipts(
        ids, db=root / "world.duckdb", output_root=root / "runs", ledger=root / "metrics.jsonl"
    )
    assert all(r["verified"] for r in replayed)
    assert next(r for r in replayed if r["receipt_id"] == business["original_receipt_id"])["verified"]
    readme = (root / "README.md").read_text(encoding="utf-8")
    assert "0.000012894" in readme and "typesafe/jev-1.13-20260917" in readme
    assert records[answer["observation_receipt_id"]]["result"]["worked_close"]["hiked_invoice_usd"] == "84.00"
