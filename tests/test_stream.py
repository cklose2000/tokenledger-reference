from dataclasses import replace
from datetime import datetime, timezone
import json

import duckdb
import pytest

from tl.stream import Activity, StreamReader, ValidationError
from tl.stream.events import timestamp


def count(stream):
    with StreamReader(stream.path).connect() as conn:
        return conn.execute("SELECT count(*) FROM stream.activity").fetchone()[0]


def test_v2_physical_contract_and_only_one_source_table(stream):
    with StreamReader(stream.path).connect() as conn:
        columns = [row[0] for row in conn.execute("DESCRIBE stream.activity").fetchall()]
        assert columns[:8] == ["activity_id", "ts", "customer", "anonymous_customer_id", "activity",
                               "feature_json", "revenue_impact", "link"]
        assert len(columns) == 14
        assert all(column.startswith("_") for column in columns[8:])
        assert conn.execute("SELECT table_schema, table_name FROM information_schema.tables WHERE table_type='BASE TABLE'").fetchall() == [("stream", "activity")]


@pytest.mark.parametrize("statement", [
    "UPDATE stream.activity SET customer='tampered'",
    "DELETE FROM stream.activity",
    "INSERT INTO stream.activity SELECT * FROM stream.activity",
    "DROP TABLE stream.activity",
])
def test_reader_cannot_mutate_database(stream, customer_event, statement):
    stream.append([customer_event])
    with StreamReader(stream.path).connect() as conn, pytest.raises(duckdb.Error):
        conn.execute(statement)
    assert count(stream) == 1


def test_retry_deduplicates_and_conflict_rolls_back(stream, customer_event):
    assert stream.append([customer_event, customer_event])["inserted"] == 1
    assert stream.append([customer_event])["duplicates"] == 1
    with pytest.raises(ValidationError, match="conflicting idempotency"):
        stream.append([replace(customer_event, activity_id="new"), replace(customer_event, customer="changed")])
    assert count(stream) == 1


def test_new_world_optimization_keeps_primary_key_and_empty_stream_guards(stream, customer_event):
    stream.append_simulation([(customer_event, timestamp(customer_event.ts))], source="sim:test", new_world=True)
    with pytest.raises(ValidationError, match="empty stream"):
        stream.append_simulation([(customer_event, timestamp(customer_event.ts))], source="sim:test", new_world=True)
    assert stream.append_simulation([(customer_event, timestamp(customer_event.ts))], source="sim:test")["duplicates"] == 1


def test_bad_row_after_flushed_batch_rolls_back_entire_call(stream, customer_event):
    def events():
        for i in range(65540):
            yield replace(customer_event, activity_id=f"good-{i}")
        yield replace(customer_event, activity_id="invalid", activity="unregistered_activity")
    with pytest.raises(ValidationError, match="unknown activity"):
        stream.append(events())
    assert count(stream) == 0


@pytest.mark.parametrize("change,pattern", [
    ({"ts": "2024-02-01"}, "timestamp"),
    ({"activity_id": ""}, "nonempty"),
    ({"activity": []}, "unknown activity"),
    ({"customer": None}, "identified customer"),
    ({"feature_json": {}}, "missing segment"),
    ({"feature_json": {1: "bad-key"}}, "keys must be strings"),
    ({"feature_json": {"segment": "enterprise", "channel": "direct", "country": "US", "parent_customer": None, "surprise": 1}}, "unknown features"),
    ({"revenue_impact": "1.00"}, "only revenue_recognized"),
])
def test_invalid_envelopes_never_write(stream, customer_event, change, pattern):
    with pytest.raises(ValidationError, match=pattern):
        stream.append([replace(customer_event, **change)])
    assert count(stream) == 0


def trial(key, when, customer=None, anonymous="source:visitor"):
    return Activity(key, when, "trial_started", dict(trial_id=key, plan="pro", seats=1, end="2024-03-01"),
                    customer, anonymous)


def test_occurrence_and_next_event_are_computed_after_knowledge_filter(stream):
    stream.append_simulation([
        (trial("one", "2024-02-01T00:00:00Z"), timestamp("2024-02-01T00:00:00Z")),
        (trial("late", "2024-02-02T00:00:00Z"), timestamp("2024-03-10T00:00:00Z")),
        (trial("three", "2024-02-03T00:00:00Z"), timestamp("2024-02-03T00:00:00Z")),
    ], source="sim:test")
    reader = StreamReader(stream.path)
    original = reader.snapshot(asof="2024-02-29").to_pylist()
    assert [r["activity_occurrence"] for r in original] == [1, 2]
    assert original[0]["activity_repeated_at"].day == 3
    current = reader.snapshot(asof="2024-02-29", known_at="2024-03-11T00:00:00Z").to_pylist()
    assert [r["activity_occurrence"] for r in current] == [1, 2, 3]
    assert current[0]["activity_repeated_at"].day == 2
    assert current[-1]["activity_repeated_at"] is None


def test_anonymous_identities_are_separate_and_both_ids_are_allowed(stream):
    events = [trial("a", "2024-02-01T00:00:00Z", anonymous="source:a"),
              trial("b", "2024-02-02T00:00:00Z", anonymous="source:b"),
              trial("associated", "2024-02-03T00:00:00Z", customer="known", anonymous="source:a")]
    stream.append_simulation([(e, timestamp(e.ts)) for e in events], source="sim:test")
    rows = StreamReader(stream.path).snapshot(asof="2024-02-29").to_pylist()
    assert [r["activity_occurrence"] for r in rows] == [1, 1, 1]
    assert all(r["activity_repeated_at"] is None for r in rows)


def test_watermark_prevents_later_backdated_simulation_from_entering_replay(stream):
    event = trial("one", "2024-02-01T00:00:00Z")
    first = stream.append_simulation([(event, timestamp(event.ts))], source="sim:test")
    event2 = replace(event, activity_id="two")
    stream.append_simulation([(event2, timestamp(event2.ts))], source="sim:test")
    reader = StreamReader(stream.path)
    assert reader.snapshot(asof="2024-02-29", watermark=first["watermark"]).num_rows == 1
    assert reader.snapshot(asof="2024-02-29").num_rows == 2


def test_normal_arrival_is_stamped_and_cannot_be_backdated(stream, customer_event):
    before = datetime.now(timezone.utc)
    stream.append([customer_event])
    with StreamReader(stream.path).connect() as conn:
        recorded = conn.execute("SELECT _recorded_at FROM stream.activity").fetchone()[0]
    assert before <= recorded <= datetime.now(timezone.utc)
    with pytest.raises(TypeError):
        stream.append([customer_event], recorded_at=timestamp("2024-02-01T00:00:00Z"))


def test_simulated_arrival_cannot_precede_event(stream, customer_event):
    with pytest.raises(ValidationError, match="precedes"):
        stream.append_simulation([(customer_event, timestamp("2024-01-01T00:00:00Z"))], source="sim:test")


def test_revenue_money_is_exact_and_bool_is_not_seats(stream):
    event = Activity("rev", "2024-02-01T00:00:00Z", "revenue_recognized",
                     dict(period="2024-02-01", net_usd="0.29", source="usage", source_document="invoice:1",
                          recognition_kind="usage", channel="direct"), "c1", revenue_impact="0.29")
    stream.append([event])
    with StreamReader(stream.path).connect() as conn:
        assert str(conn.execute("SELECT revenue_impact FROM stream.activity").fetchone()[0]) == "0.29"
    for invalid in (0.29, "0.291", "NaN"):
        with pytest.raises(ValidationError, match="money"):
            stream.append([replace(event, activity_id="invalid", revenue_impact=invalid)])
    zero = replace(event, activity_id="negative-zero", revenue_impact="-0.00",
                   feature_json={**event.feature_json, "net_usd": "-0.00"})
    stream.append([zero])
    with StreamReader(stream.path).connect() as conn:
        features = conn.execute("SELECT feature_json::VARCHAR FROM stream.activity WHERE activity_id='negative-zero'").fetchone()[0]
        assert json.loads(features)["net_usd"] == "0.00"
    t = trial("bad", "2024-02-01T00:00:00Z")
    with pytest.raises(ValidationError, match="integer"):
        stream.append([replace(t, feature_json={**t.feature_json, "seats": True})])
