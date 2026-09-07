"""Reporting semantics are separate from the released activity shape registry."""

from datetime import date
import hashlib
from pathlib import Path

import yaml

from tl.stream.events import ValidationError


class RecognitionPolicy:
    def __init__(self, path=Path("definitions/policies/recognition/v1.yaml")):
        content = Path(path).read_bytes().replace(b"\r\n", b"\n")
        self.digest = hashlib.sha256(content).hexdigest()
        spec = yaml.safe_load(content)
        if (spec["name"] != "recognition" or spec["version"] != "v1"
                or spec["period_bounds"] != "[period_start, period_end)"
                or spec["reporting_input"] != "StreamReader.snapshot"):
            raise ValidationError("unsupported recognition policy contract")
        self.version = spec["version"]
        self.pairs = {(item["source"], item["recognition_kind"]): item["family"]
                      for item in spec["recognition_pairs"]}
        if len(self.pairs) != len(spec["recognition_pairs"]):
            raise ValidationError("duplicate recognition policy pair")

    def family(self, features):
        pair = (features["source"], features["recognition_kind"])
        if pair not in self.pairs:
            raise ValidationError(f"unmapped source/recognition_kind: {pair}")
        return self.pairs[pair]

    @staticmethod
    def period_contains(features, day):
        start, end = date.fromisoformat(features["period_start"]), date.fromisoformat(features["period_end"])
        if start >= end:
            raise ValidationError("service period must have period_start < exclusive period_end")
        return start <= date.fromisoformat(day) < end
