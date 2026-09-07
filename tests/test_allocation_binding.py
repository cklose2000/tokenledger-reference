import hashlib
from pathlib import Path
import shutil

import pytest
import yaml

from tl.generate.allocation import pinned_coefficients
from tl.generate.synthetic import ENERGY, TOKEN_TYPES
from tl.stream import ValidationError


def test_allocation_edits_cannot_silently_reinterpret_v11_world(tmp_path):
    shutil.copytree("definitions/allocation", tmp_path / "allocation")
    shutil.copytree("definitions/generators", tmp_path / "generators")
    arguments = dict(energy=ENERGY, token_types=TOKEN_TYPES, root=tmp_path)
    assert pinned_coefficients(**arguments)[0] == (20_000_000, 80_000_000, 3_000_000, 25_000_000)
    path = tmp_path / "allocation/synthetic-g1.yaml"
    edited = yaml.safe_load(path.read_text())
    edited["coefficients"]["input"] = 0.04
    path.write_text(yaml.safe_dump(edited), encoding="utf-8", newline="\n")
    with pytest.raises(ValidationError, match="allocation changed"):
        pinned_coefficients(**arguments)
    binding_path = tmp_path / "generators/synthetic-v1.1.yaml"
    binding = yaml.safe_load(binding_path.read_text())
    binding["allocation_files"][path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    binding_path.write_text(yaml.safe_dump(binding), encoding="utf-8")
    with pytest.raises(ValidationError, match="released generator v1.1"):
        pinned_coefficients(**arguments)


def test_allocation_binding_is_platform_independent(tmp_path):
    shutil.copytree("definitions/allocation", tmp_path / "allocation")
    shutil.copytree("definitions/generators", tmp_path / "generators")
    expected = pinned_coefficients(energy=ENERGY, token_types=TOKEN_TYPES)
    for path in (tmp_path / "allocation").glob("*.yaml"):
        path.write_bytes(path.read_bytes().replace(b"\r\n", b"\n").replace(b"\n", b"\r\n"))
    assert pinned_coefficients(energy=ENERGY, token_types=TOKEN_TYPES, root=tmp_path) == expected
