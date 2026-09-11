"""Exact detached approval verification, shared by both learning applications.

This module verifies only. Key enrollment and signing remain reviewer operations.
"""
from pathlib import Path
import subprocess

from tl.stream import ValidationError

NAMESPACE = 'tokenledger-learning-trial'
PRINCIPAL = 'chandler'


def verify(plan, signature, trust):
    plan, signature, trust = map(Path, (plan, signature, trust))
    if not trust.is_file() or not signature.is_file():
        raise ValidationError('Promotion unavailable: reviewer trust and detached signature are required')
    try:
        result = subprocess.run(['ssh-keygen', '-Y', 'verify', '-f', str(trust),
            '-I', PRINCIPAL, '-n', NAMESPACE, '-s', str(signature)],
            input=plan.read_bytes(), capture_output=True, timeout=15, check=False)
    except (OSError, subprocess.TimeoutExpired):
        raise ValidationError('reviewer signature verifier unavailable') from None
    if result.returncode:
        raise ValidationError('Promotion unavailable: authorized reviewer signature did not verify')
