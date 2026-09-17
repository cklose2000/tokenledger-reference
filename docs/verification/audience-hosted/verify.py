import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from datetime import datetime, timezone

root = Path('/workspaces/tokenledger-reference')
os.chdir(root)
os.environ.update(PYTHONDONTWRITEBYTECODE='1', PYTHONHASHSEED='0')
out = root / '.tmp/audience-hosted-20260916'
out.mkdir(parents=True, exist_ok=False)
sha = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
assert sha == '0ce1bbf391c6ac779285fd8ad0f261bd7a4cef0a'
record = {'schema_version': 'audience-hosted/v1', 'actor': 'GPT-6', 'actor_basis': 'explicit_caller_claim', 'independent_qa': False, 'execution_sha': sha, 'started_utc': datetime.now(timezone.utc).isoformat(), 'python': sys.version, 'provisioning': {'status': 'completed', 'machine': 'basicLinux32gb', 'create_command_seconds': 5.4659606, 'limitation': 'CLI creation duration is not total readiness or image/postCreate installation time; those were not separately instrumented.'}, 'commands': []}

def run(name, args):
    start = time.perf_counter()
    p = subprocess.run(args, capture_output=True)
    row = {'name': name, 'argv': args, 'exit': p.returncode, 'seconds': time.perf_counter() - start}
    for stream, data in [('stdout', p.stdout), ('stderr', p.stderr)]:
        (out / (name + '.' + stream)).write_bytes(data)
        row[stream + '_sha256'] = hashlib.sha256(data).hexdigest()
    record['commands'].append(row)
    (out / 'verification.json').write_text(json.dumps(record, indent=2) + '\n')
    print(json.dumps(row), flush=True)
    if p.returncode:
        raise SystemExit(p.returncode)
    return p.stdout

run('venv', [sys.executable, '-m', 'venv', '/tmp/tl-audience-venv-20260916'])
python = '/tmp/tl-audience-venv-20260916/bin/python'
tl = '/tmp/tl-audience-venv-20260916/bin/tl'
run('install', [python, '-m', 'pip', 'install', '-e', '.[dev]'])
run('demo', [tl, 'demo', '--directory', 'data/demo/hosted-20260916', '--json'])
run('challenge-prepare', [tl, 'challenge', 'prepare', '--directory', 'data/challenge/hosted-20260916', '--json'])
run('challenge-verify', [tl, 'challenge', 'verify', 'data/challenge/hosted-20260916', '--json'])
run('challenge-grade', [tl, 'challenge', 'grade', 'data/challenge/hosted-20260916', '--answer', 'data/challenge/hosted-20260916/expected.json', '--output', 'data/score/hosted-20260916', '--json'])
run('retained-receipt', [tl, '--db', 'data/demo/audience/world.duckdb', '--artifact-root', 'data/demo/audience', 'receipt', 'mr-16259514a4d2d12bcdc5b11ad84a7209', '--json'])
run('nrr-receipt', [tl, '--db', 'data/demo/recorded/world.duckdb', '--artifact-root', 'data/demo/recorded', 'receipt', 'mr-fd1a138b3be0457f060a81a70341be67', '--json'])
record['status'] = 'passed'
record['challenge_scope'] = 'open-book reference-answer onboarding check; not a held-out or matched-agent benchmark'
record['finished_utc'] = datetime.now(timezone.utc).isoformat()
(out / 'verification.json').write_text(json.dumps(record, indent=2) + '\n')
