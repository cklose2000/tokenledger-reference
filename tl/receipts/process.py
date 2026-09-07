"""Provisional process evidence until the 022 source is available."""

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess

from tl.stream.events import ValidationError, canonical


def record_process(*, root: Path, role: str, phase: str, work_unit: str, outputs: list[str],
                   verification: list[str], actor: str) -> dict:
    root = root.resolve()
    if not isinstance(actor, str) or not actor.strip() or actor != actor.strip() or any(ord(c) < 32 for c in actor):
        raise ValidationError("process receipt requires an explicit, nonempty actor without surrounding whitespace or control characters")
    if role not in {"executive", "worker", "qa", "integrator"} or not verification or not outputs:
        raise ValidationError("process receipt requires an allowed role, outputs, and verification")
    sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, text=True, capture_output=True, check=True).stdout.strip()
    manifest = []
    for path in outputs:
        resolved = (root / path).resolve()
        if not resolved.is_relative_to(root) or not resolved.is_file():
            raise ValidationError(f"output must be a file in the repository: {path}")
        if resolved == root / "ledger/process.jsonl":
            raise ValidationError("a process receipt cannot hash the ledger containing itself")
        manifest.append({"path": resolved.relative_to(root).as_posix(),
                         "sha256": hashlib.sha256(resolved.read_bytes()).hexdigest()})
    row = {"schema_version": "tokenledger-process/provisional-v2", "phase": phase, "actor": actor,
           "actor_basis": "explicit_caller_claim", "authenticated_identity": False,
           "role": role, "work_unit": work_unit, "status": "completed_artifact",
           "factory_conformance": "pending_022_reference", "git_sha": sha,
           "git_sha_semantics": "input_revision; output hashes pin resulting content; containing commit supplies integration revision",
           "inputs": [{"kind": "git_commit", "sha": sha}], "outputs": manifest,
           "verification_performed": verification}
    row["receipt_id"] = f"p{phase}-" + hashlib.sha256(canonical(row).encode()).hexdigest()[:20]
    destination = root / "ledger/process.jsonl"
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        for line in destination.read_text(encoding="utf-8").splitlines():
            existing = json.loads(line)
            if existing["receipt_id"] == row["receipt_id"]:
                return existing
    row["ts"] = datetime.now(timezone.utc).isoformat()
    with destination.open("ab") as handle:
        handle.write((canonical(row) + "\n").encode("utf-8"))
    return row
