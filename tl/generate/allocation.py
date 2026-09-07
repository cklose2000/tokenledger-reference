"""Resolve and verify the released v1.1 energy inputs before generation."""

from decimal import Decimal
import hashlib
from pathlib import Path

import yaml

from tl.stream.events import ValidationError


def pinned_coefficients(*, energy, token_types, root=Path("definitions")):
    binding = yaml.safe_load((root / "generators/synthetic-v1.1.yaml").read_text(encoding="utf-8"))
    if binding["version"] != "v1.1" or binding["allocation_version"] != "v1":
        raise ValidationError("generator v1.1 requires its released allocation binding")
    result = []
    for generation in range(5):
        model = f"synthetic-g{generation + 1}"
        path = root / "allocation" / f"{model}.yaml"
        content = path.read_bytes().replace(b"\r\n", b"\n")
        if hashlib.sha256(content).hexdigest() != binding["allocation_files"][path.name]:
            raise ValidationError(f"allocation changed for {model}; create a new allocation and generator version")
        spec = yaml.safe_load(content)
        if (spec["model"] != model or spec["version"] != "v1" or spec["synthetic"] is not True
                or spec["unit"] != "MWh per million tokens" or set(spec["coefficients"]) != set(token_types)):
            raise ValidationError(f"invalid pinned allocation: {model}")
        coefficients = tuple(Decimal(str(spec["coefficients"][kind])) * 10**9 for kind in token_types)
        # This guard binds YAML to the coefficients used by the independently
        # reviewed v1.1 stream, even if the binding file's hashes were changed.
        legacy = tuple(round(value * 0.8**generation * 1e9) for value in energy)
        if coefficients != legacy:
            raise ValidationError(f"allocation differs from released generator v1.1: {model}")
        result.append(tuple(int(value) for value in coefficients))
    return tuple(result)
