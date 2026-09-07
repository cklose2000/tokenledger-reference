from pathlib import Path

import pytest

from tl.stream import Activity, Stream

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def stream(tmp_path):
    result = Stream(tmp_path / "test.duckdb", ROOT / "definitions/activities")
    result.init()
    return result


@pytest.fixture
def customer_event():
    return Activity("c-created", "2024-02-01T00:00:00Z", "customer_created",
                    dict(segment="enterprise", channel="direct", country="US", parent_customer=None), "c1")
