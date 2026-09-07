"""Portable synthetic replay archives; execution is resolved against local Git."""

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import sys
from tempfile import TemporaryDirectory

import pyarrow.parquet as pq

from tl.receipts.metrics import input_manifest, read_receipts
from tl.stream import Stream, StreamReader, ValidationError
from tl.stream.events import canonical


def sha(content):
    return hashlib.sha256(content).hexdigest()


def safe_path(root, name):
    if (not isinstance(name,str) or not re.fullmatch(r'[A-Za-z0-9_./-]+',name)
            or any(part in ('..','.') for part in name.split('/')) or PurePosixPath(name).is_absolute()):
        raise ValidationError('invalid archive artifact path')
    path = Path(root) / name
    if not path.resolve().is_relative_to(Path(root).resolve()):
        raise ValidationError('archive artifact escapes its root')
    return path


def git_code(manifest, destination=None, repository=None):
    """Read blobs in one Git call, without executing any imported source text."""
    revision = manifest['git_sha']
    if not re.fullmatch(r'[0-9a-f]{40}',revision):
        raise ValidationError('invalid receipt Git revision')
    artifacts = manifest['execution_artifacts']
    names = sorted(artifacts)
    if not names:
        raise ValidationError('missing historical execution artifacts')
    for name in names:
        safe_path(Path.cwd(),name)
        comparison_path=manifest.get('schema_version') in (
            'tokenledger-comparison/v1','tokenledger-views/v1','tokenledger-views-comparison/v1',
            'tokenledger-demo-bridge/v1','tokenledger-demo-yield/v1',
            'tokenledger-challenge/v1','tokenledger-challenge-score/v1') and name.startswith('baseline/')
        if not (name in ('pyproject.toml','requirements/replay-1.2.1.txt') or name.startswith(('tl/','definitions/','scaffolds/')) or comparison_path):
            raise ValidationError('unregistered historical execution path')
    request = ''.join(f'{revision}:{name}\n' for name in names).encode()
    result = subprocess.run(['git','cat-file','--batch'],input=request,capture_output=True,
                            cwd=repository or Path.cwd(),check=False)
    if result.returncode:
        raise ValidationError('historical Git objects unavailable; use a checkout retaining the receipt revision')
    position = 0
    content = {}
    for name in names:
        end = result.stdout.find(b'\n',position)
        header = result.stdout[position:end].split()
        if len(header)!=3 or header[1]!=b'blob':
            raise ValidationError(f'historical artifact unavailable at {revision}: {name}')
        size = int(header[2])
        raw = result.stdout[end+1:end+1+size].replace(b'\r\n',b'\n')
        position = end+size+2
        if sha(raw) != artifacts[name]:
            raise ValidationError(f'historical artifact differs from receipt: {name}; commit execution files before publishing a replay archive')
        content[name] = raw
    if destination is not None:
        for name, raw in content.items():
            target = safe_path(destination,name)
            target.parent.mkdir(parents=True,exist_ok=True)
            target.write_bytes(raw)
    return content


def retain_artifacts(artifacts, destination):
    for name, expected in artifacts.items():
        raw = Path(name).read_bytes().replace(b'\r\n',b'\n')
        if sha(raw)!=expected:
            raise ValidationError(f'execution artifact changed before retention: {name}')
        target = safe_path(destination,name)
        target.parent.mkdir(parents=True,exist_ok=True)
        target.write_bytes(raw)


def load_pack(pack, destination):
    """Validate saved bytes and reconstruct exact run rows from their receipts."""
    from tl.metrics.engine import read_run
    pack = Path(pack)
    manifest = json.loads((pack/'manifest.json').read_bytes())
    if manifest.get('schema_version')!='tokenledger-pack/v1':
        raise ValidationError('unsupported pack manifest')
    for name, expected in manifest['files'].items():
        if sha(safe_path(pack,name).read_bytes())!=expected:
            raise ValidationError(f'pack artifact hash mismatch: {name}')
    if not {'run.json','inputs.jsonl','receipts.jsonl'} <= manifest['files'].keys():
        raise ValidationError('pack lacks its run/input/receipt artifacts')
    run_id = manifest['run_id']
    if not re.fullmatch(r'[0-9a-f]{32}',run_id):
        raise ValidationError('invalid pack run identity')
    records = read_receipts(pack/'receipts.jsonl')
    run_root = Path(destination)/'runs'/run_id
    run_root.mkdir(parents=True,exist_ok=False)
    for name in ('run.json','inputs.jsonl'):
        shutil.copyfile(pack/name,run_root/name)
    ledger = Path(destination)/'receipts.jsonl'
    shutil.copyfile(pack/'receipts.jsonl',ledger)
    rows = [{**record['result'],'receipt_id':key} for key,record in records.items()]
    (run_root/'rows.jsonl').write_bytes((''.join(canonical(row)+'\n' for row in rows)).encode())
    return read_run(run_id,output_root=Path(destination)/'runs',ledger=ledger)


def create_archive(destination, *, db=None, pack=None, run_id=None,
                   output_root=Path('metrics/out'), ledger=Path('ledger/metrics.jsonl')):
    from tl.metrics.engine import read_run
    if bool(pack)==bool(run_id):
        raise ValidationError('archive create requires exactly one of --pack or --run-id')
    destination = Path(destination)
    if destination.exists():
        raise ValidationError('archive destination already exists')
    with TemporaryDirectory(prefix='tl-archive-') as scratch:
        if pack:
            run = load_pack(pack,scratch)
            run_ledger = Path(scratch)/'receipts.jsonl'
        else:
            run = read_run(run_id,output_root=output_root,ledger=ledger)
            run_ledger = Path(ledger)
        manifest = run['manifest']
        if manifest.get('schema_version')!='tokenledger-run/v1':
            raise ValidationError('archive each dependency metric run; derived reports require their dependency runs and ledger')
        code = git_code(manifest)
        if manifest.get('synthetic') is not True:
            raise ValidationError('this archive version supports only the synthetic application')
        reader = StreamReader(db or manifest['stream_path'])
        snapshot = reader.snapshot(asof=manifest['asof'],known_at=manifest['known_at'],
                                   watermark=manifest['watermark'],include_provenance=True)
        if input_manifest(snapshot)!=manifest['inputs']:
            raise ValidationError('receipt input stream differs from original snapshot')
        with reader.connect() as conn:
            source = conn.execute('SELECT * FROM stream.activity WHERE _stream_position<=? ORDER BY _stream_position',
                                  [manifest['watermark']]).fetch_arrow_table()
        if any(lane!='sim' for lane in source['_lane'].unique().to_pylist()):
            raise ValidationError('synthetic archive cannot include a live-data lane')
        destination.mkdir(parents=True)
        for name, raw in code.items():
            target = safe_path(destination/'code',name)
            target.parent.mkdir(parents=True,exist_ok=True)
            target.write_bytes(raw)
        run_root = destination/'runs'/manifest['run_id']
        run_root.mkdir(parents=True)
        for name in ('run.json','inputs.jsonl','rows.jsonl'):
            shutil.copyfile(Path(run['run_directory'])/name,run_root/name)
        records = read_receipts(run_ledger)
        (destination/'receipts.jsonl').write_bytes((''.join(canonical(records[row['receipt_id']])+'\n' for row in run['rows'])).encode())
        pq.write_table(source,destination/'source.parquet',compression='zstd')
        files = {path.relative_to(destination).as_posix():sha(path.read_bytes())
                 for path in sorted(destination.rglob('*')) if path.is_file()}
        contract = dict(schema_version='tokenledger-replay-archive/v1',run_id=manifest['run_id'],
                        git_sha=manifest['git_sha'],runtime=manifest['runtime'],files=files,
                        source_scope='complete physical stream prefix through frozen watermark; synthetic only',
                        pack_manifest_sha256=sha((Path(pack)/'manifest.json').read_bytes()) if pack else None)
        raw = (canonical(contract)+'\n').encode()
        (destination/'archive.json').write_bytes(raw)
    return dict(status='archived',path=str(destination),run_id=manifest['run_id'],manifest_sha256=sha(raw),
                runtime=manifest['runtime'],git_sha=manifest['git_sha'])


def verify_archive(path, expected_hash):
    from tl.metrics.engine import read_run
    path = Path(path)
    raw = (path/'archive.json').read_bytes()
    if sha(raw)!=expected_hash:
        raise ValidationError('archive manifest hash mismatch')
    contract = json.loads(raw)
    if contract.get('schema_version')!='tokenledger-replay-archive/v1':
        raise ValidationError('unsupported replay archive')
    actual = {item.relative_to(path).as_posix() for item in path.rglob('*') if item.is_file() and item!=path/'archive.json'}
    if actual!=set(contract['files']):
        raise ValidationError('archive file population differs from manifest')
    for name, expected in contract['files'].items():
        if sha(safe_path(path,name).read_bytes())!=expected:
            raise ValidationError(f'archive artifact hash mismatch: {name}')
    run = read_run(contract['run_id'],output_root=path/'runs',ledger=path/'receipts.jsonl')
    manifest = run['manifest']
    if manifest.get('schema_version')!='tokenledger-run/v1':
        raise ValidationError('archive each dependency metric run; derived reports require their dependency runs and ledger')
    if contract['git_sha']!=manifest['git_sha'] or contract['runtime']!=manifest['runtime']:
        raise ValidationError('archive execution contract differs from run')
    code = git_code(manifest)
    for name, raw in code.items():
        if safe_path(path/'code',name).read_bytes()!=raw:
            raise ValidationError('archive code differs from trusted Git objects')
    return run


WORKER = '''import json, os, sys, platform, tomllib
from importlib.metadata import version
from pathlib import Path
os.environ["TL_HISTORICAL_REPLAY"]="1"
sys.path.insert(0, str(Path.cwd()))
request=json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
os.environ["TL_REPLAY_STACK"]=json.dumps(request.get("execution_stack",[]))
try:
    if platform.python_version()!=request["runtime"]["python"]:
        raise ValueError("receipt runtime differs; restore the recorded Python version")
    specification=tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
    for requirement in specification["project"]["dependencies"]:
        name,pin=requirement.split("==")
        if version(name)!=pin:
            raise ValueError("receipt dependency differs; restore pinned "+requirement)
    from tl.metrics.engine import replay_receipts
    from tl.stream.events import canonical
    kwargs={}
    if request.get("application_config"):
        from tl.reporting.application import Application
        kwargs["application"]=Application(request["application_config"])
    result=replay_receipts(request["ids"],db=Path(request["db"]),output_root=Path(request["runs"]),ledger=Path(request["ledger"]),**kwargs)
    Path(sys.argv[2]).write_text(canonical(result),encoding="utf-8")
except Exception as exc:
    Path(sys.argv[2]).write_text(json.dumps({"error":str(exc)}),encoding="utf-8")
    raise SystemExit(1)
'''


def historical_replay(ids, *, manifest, db, output_root, ledger, python=None, application_config=None):
    """Execute only hash-matched local Git code, with no mutation of its inputs."""
    # Preserve the selected virtual environment rather than its symlink target.
    executable = str(Path(python or sys.executable).absolute())
    if python is None:
        # The only implicit alternative is our explicitly provisioned historical
        # runtime. Never execute a Python path supplied by a receipt.
        import duckdb
        if manifest['runtime'].get('duckdb')=='1.2.1' and duckdb.__version__!='1.2.1':
            retained=Path('.venv-replay-1.2.1')/('Scripts/python.exe' if os.name=='nt' else 'bin/python')
            if not retained.is_file():
                raise ValidationError('historical DuckDB 1.2.1 runtime required; provision .venv-replay-1.2.1 from requirements/replay-1.2.1.txt or pass --python')
            executable=str(retained.absolute())
    stack=json.loads(os.environ.get('TL_REPLAY_STACK','[]'))
    if manifest['execution_hash'] in stack or len(stack)>=8:
        raise ValidationError('historical runtime or artifacts differ; recursive replay refused')
    # Nested derived replay can require a second retained revision. Preserve a
    # caller-resolved, read-only Git object location, never one from the receipt.
    git_dir=subprocess.run(['git','rev-parse','--absolute-git-dir'],capture_output=True,text=True,check=True).stdout.strip()
    with TemporaryDirectory(prefix='tl-replay-') as temporary:
        root = Path(temporary)
        git_code(manifest,root/'code')
        request = dict(ids=ids,db=str(Path(db).resolve()),runs=str(Path(output_root).resolve()),ledger=str(Path(ledger).resolve()),
                       runtime=manifest['runtime'],execution_stack=stack+[manifest['execution_hash']])
        if application_config is not None:
            request['application_config']=str(Path(application_config).resolve())
        (root/'request.json').write_text(canonical(request),encoding='utf-8')
        # Diagnostics are not the protocol: native libraries can emit bytes in
        # an encoding different from the host locale. The result file below is
        # the sole protocol and is decoded explicitly as UTF-8.
        result = subprocess.run([executable,'-I','-c',WORKER,str(root/'request.json'),str(root/'result.json')],
                                cwd=root/'code',capture_output=True,check=False,env={**os.environ,'GIT_DIR':git_dir})
        if not (root/'result.json').exists():
            raise ValidationError('historical runtime could not start; restore recorded Python and pinned dependencies')
        answer = json.loads((root/'result.json').read_text(encoding='utf-8'))
        if result.returncode:
            raise ValidationError('historical replay failed: '+answer.get('error','worker failed'))
        return answer


def replay_archive(path, expected_hash, *, ids=None, python=None):
    path = Path(path).resolve()
    run = verify_archive(path,expected_hash)
    selected = ids or [row['receipt_id'] for row in run['rows']]
    if not set(selected)<={row['receipt_id'] for row in run['rows']} or len(set(selected))!=len(selected):
        raise ValidationError('archive receipt selection is invalid')
    with TemporaryDirectory(prefix='tl-source-') as temporary:
        db = Path(temporary)/'source.duckdb'
        Stream(db,path/'code/definitions/activities').restore(pq.read_table(path/'source.parquet'))
        return historical_replay(selected,manifest=run['manifest'],db=db,
                                 output_root=path/'runs',ledger=path/'receipts.jsonl',python=python)
