from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal
import hashlib
import json

import pytest

from tl.generate import Config, SyntheticWorld
from tl.stream import Stream, StreamReader
from tl.stream.validation import validate_stream


def test_determinism_bytes_and_seed_effect(tmp_path):
    config = Config(customers=100, months=30)
    outputs = []
    for name, seed in [("a", 42), ("b", 42), ("c", 43)]:
        stream = Stream(tmp_path / f"{name}.duckdb")
        stream.init()
        world = SyntheticWorld(replace(config, seed=seed))
        stream.append_simulation(world.events(), source=world.prefix)
        destination = tmp_path / f"{name}.jsonl"
        StreamReader(stream.path).export(destination)
        outputs.append(destination.read_bytes())
    assert outputs[0] == outputs[1]
    assert outputs[0] != outputs[2]
    # Captured before the QA fix from main b06119a: 7,197 events, same
    # seed/config, activity registry, and pinned runtime as the reviewed build.
    assert hashlib.sha256(outputs[0]).hexdigest() == "6ef77bd1a6410e03e3fbdd8cd441d4c2c854961c0852881f8a12901f4721a855"


def test_preparation_worker_count_does_not_change_bytes_or_source_manifest(tmp_path):
    config = Config(customers=256, months=30)
    exported, manifests = [], []
    for workers in (1, 2, 4):
        stream = Stream(tmp_path / f"workers-{workers}.duckdb")
        stream.init()
        world = SyntheticWorld(config)
        stream.generate(world, workers=workers, new_world=True)
        destination = tmp_path / f"workers-{workers}.jsonl"
        StreamReader(stream.path).export(destination)
        exported.append(destination.read_bytes())
        manifests.append(world.manifest())
    assert exported[0] == exported[1] == exported[2]
    assert manifests[0] == manifests[1] == manifests[2]


@pytest.fixture(scope="module")
def world():
    generated = SyntheticWorld(Config(customers=200, months=30))
    events = list(generated.events())
    return generated, events


def test_rich_world_covers_catalog_segments_models_and_churn(world):
    generated, events = world
    from tl.stream.events import Catalog
    from pathlib import Path
    assert set(generated.counts) == set(Catalog(Path("definitions/activities")).schemas)
    assert {e.feature_json["segment"] for e, _ in events if e.activity == "customer_created"} == {
        "enterprise", "mid_market_api", "startup_api", "marketplace", "consumer_subscription", "team_subscription", "claude_code_seats"}
    assert len({e.feature_json["model"] for e, _ in events if e.activity == "model_released"}) == 5
    assert len({e.feature_json["provider"] for e, _ in events if e.activity == "capacity_contracted"}) == 3
    assert {e.feature_json["token_type"] for e, _ in events if e.activity == "tokens_processed"} == {"input", "output", "cache_read", "cache_write"}


def test_invoice_cents_drawdowns_and_late_arrivals(world):
    generated, events = world
    invoices = [e for e, _ in events if e.activity == "usage_invoiced"]
    assert 0.03 <= generated.late_invoices / len(invoices) <= 0.08
    for e, arrival in events:
        if e.activity != "usage_invoiced":
            continue
        features = e.feature_json
        assert Decimal(features["gross_usd"]) - Decimal(features["discount_usd"]) == Decimal(features["net_usd"])
        assert Decimal(features["commit_applied_usd"]) + Decimal(features["cash_due_usd"]) == Decimal(features["net_usd"])
        delay = (arrival - e.ts).days
        assert delay == 0 or 5 <= delay <= 40
    assert any(Decimal(e.feature_json["commit_applied_usd"]) > 0 for e in invoices)
    assert any(Decimal(e.feature_json["channel_fee_usd"]) > 0 for e in invoices)


def test_no_usage_after_model_deprecation(world):
    _, events = world
    deprecated = {e.feature_json["model"]: e.ts for e, _ in events if e.activity == "model_deprecated"}
    for event, _ in events:
        if event.activity == "tokens_processed" and event.feature_json["model"] in deprecated:
            assert event.ts < deprecated[event.feature_json["model"]]


def test_generated_stream_validates_and_manifest_detects_missing_source_rows(stream, tmp_path, world):
    generated, events = world
    stream.append_simulation(events, source=generated.prefix)
    manifest_path = tmp_path / "manifest.json"
    manifest = generated.manifest()
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    result = validate_stream(stream.path, stream.catalog.directory, source_manifest=manifest_path)
    assert result["passed"], result
    manifest["periods"][0]["count"] += 1
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    assert not validate_stream(stream.path, stream.catalog.directory, source_manifest=manifest_path)["passed"]


def test_subscription_earned_revenue_matches_independent_daily_exposure(world):
    _, events = world
    # Hand-workable oracle: reconstruct each day's seats and price from state
    # changes, then accumulate cents-days. Does not use generator's proration.
    by_customer = {}
    for event, _ in events:
        by_customer.setdefault(event.customer, []).append(event)
    checked = 0
    for customer_events in by_customer.values():
        recognized = [e for e in customer_events if e.activity == "revenue_recognized" and e.feature_json["source"] == "subscription"]
        changes = [e for e in customer_events if e.activity in {"subscription_started", "subscription_renewed", "subscription_upgraded",
                   "subscription_downgraded", "seat_added", "seat_removed", "subscription_cancelled"}]
        changes.sort(key=lambda e: (e.ts, 1 if e.activity.startswith("seat_") else 0))
        for revenue in recognized:
            first = date.fromisoformat(revenue.feature_json["period"])
            end = revenue.ts.date() + timedelta(days=1)
            seat_days_cents = Decimal(0)
            for day_index in range((end - first).days):
                day = first + timedelta(days=day_index)
                seats, price = 0, Decimal(0)
                for change in changes:
                    if change.ts.date() > day:
                        break
                    if change.activity == "subscription_cancelled":
                        seats = 0
                    elif change.activity.startswith("seat_"):
                        seats = change.feature_json["seats_after"]
                    else:
                        seats = change.feature_json["seats"]
                        price = Decimal(change.feature_json["price_per_seat"]) * 100
                seat_days_cents += seats * price
            expected_cents = int((seat_days_cents / (end - first).days).quantize(Decimal(1), rounding="ROUND_HALF_UP"))
            assert Decimal(revenue.feature_json["net_usd"]) * 100 == expected_cents
            checked += 1
    assert checked > 100
