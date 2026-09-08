"""Sealed, portable observations of native write-boundary probes.

Reperformance here verifies retained evidence bytes. It never repeats a denied
mutation, calls Google, or converts an observed refusal into control effectiveness.
"""
from dataclasses import asdict
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import uuid

from tl.bigquery import denials
from tl.receipts.metrics import (append_receipts, code_revision, digest,
    execution_artifacts, parse_receipts, read_receipts, receipt_id)
from tl.stream import ValidationError
from tl.stream.events import canonical


SCHEMA = 'tokenledger-bigquery-boundary-observation/v1'
MANIFEST = 'tokenledger-bigquery-boundary-files/v1'


def _now(): return datetime.now(timezone.utc).isoformat()


def _write(path, value, *, exclusive=False):
    with Path(path).open('x' if exclusive else 'w', encoding='utf-8', newline='\n') as handle:
        handle.write(canonical(value)+'\n'); handle.flush(); os.fsync(handle.fileno())


def _file_hash(path):
    total=hashlib.sha256()
    with Path(path).open('rb') as handle:
        for block in iter(lambda: handle.read(1024*1024), b''): total.update(block)
    return total.hexdigest()


def _relative(name):
    if (not isinstance(name,str) or not name or '\\' in name or ':' in name
            or PurePosixPath(name).is_absolute() or any(p in ('', '.', '..') for p in name.split('/'))):
        raise ValidationError('evidence names must be safe relative POSIX paths')
    return PurePosixPath(name)


def _reject_links(path):
    current=Path(path).absolute()
    while True:
        if current.is_symlink() or (hasattr(current,'is_junction') and current.is_junction()):
            raise ValidationError('evidence paths must not use symlinks or junctions')
        if current.parent==current: break
        current=current.parent


def _path(root, name):
    relative=_relative(name)
    path=root.joinpath(*relative.parts)
    if not path.resolve().is_relative_to(root.resolve()):
        raise ValidationError('evidence path escapes the bundle')
    current=path
    while current!=root:
        if current.is_symlink() or (hasattr(current,'is_junction') and current.is_junction()):
            raise ValidationError('linked evidence paths are not permitted')
        current=current.parent
    return path


def _inventory(root):
    files={}
    for path in sorted(root.rglob('*')):
        name=path.relative_to(root).as_posix()
        _path(root,name)
        if path.is_file() and name not in ('manifest.json','receipt.json'):
            files[name]=dict(sha256=_file_hash(path), bytes=path.stat().st_size)
    return files


def _new_root(directory):
    root=Path(directory).absolute()
    # Do not resolve through a link and silently select a different workspace.
    _reject_links(root)
    if root.exists(): raise ValidationError('boundary evidence requires a fresh directory')
    root.mkdir(parents=True)
    return root


def _execution():
    artifacts=execution_artifacts([])
    return dict(**code_revision(artifacts), execution_hash=digest(artifacts), artifacts=artifacts)


def _seal(result, root, *, binding, artifacts=None, ledger=None, provenance=None):
    # Artifacts are an explicit caller-selected allowlist. Never scan credential
    # directories, source sessions, local profiles, or another repository.
    artifact_failures=[]
    for name, source in (artifacts or {}).items():
        relative=_relative(name)
        target=_path(root, 'proof/'+relative.as_posix())
        source=Path(source)
        _reject_links(source)
        if not source.is_file():
            artifact_failures.append(dict(artifact=name,status='missing'))
            continue
        target.parent.mkdir(parents=True,exist_ok=True)
        if target.exists(): raise ValidationError('duplicate retained evidence artifact')
        shutil.copyfile(source,target)
    evidence_status='incomplete' if artifact_failures else 'sealed'
    if artifact_failures: _write(root/'artifact-errors.json',artifact_failures,exclusive=True)
    execution=({**_execution(),'pin_scope':'recording_environment'} if provenance is None else deepcopy(provenance))
    if (not re.fullmatch(r'[a-f0-9]{40}', execution.get('git_sha',''))
            or not isinstance(execution.get('artifacts'),dict)
            or execution.get('execution_hash')!=digest(execution['artifacts'])):
        raise ValidationError('observation requires a source revision and exact execution artifact hashes')
    _write(root/'result.json',result,exclusive=True)
    _write(root/'binding.json',asdict(binding),exclusive=True)
    _write(root/'execution.json',execution,exclusive=True)
    manifest=dict(schema_version=MANIFEST, binding_identity=binding.identity,evidence_status=evidence_status,
                  files=_inventory(root))
    _write(root/'manifest.json',manifest,exclusive=True)
    inputs=result.get('original') or result.get('inputs') or {}
    known_at=result.get('finished_at') or _now()
    record=dict(schema_version=SCHEMA, run_id=uuid.uuid4().hex,
        definition_version='bigquery-boundary-observation/v1', binding_identity=binding.identity,
        result=dict(manifest_sha256=digest(manifest), manifest_bytes_sha256=_file_hash(root/'manifest.json'),
                    report_sha256=digest(result), observed_status=result.get('status','unknown'), evidence_status=evidence_status),
        inputs=inputs, query_hash=digest(execution['artifacts']), git_sha=execution['git_sha'],
        execution_hash=execution['execution_hash'], asof=known_at[:10], known_at=known_at,
        watermark=(inputs.get('input_row_range') or [None,None])[-1])
    record['receipt_id']=receipt_id(record)
    ledger=Path(ledger).absolute() if ledger is not None else root.parent/'boundary-receipts.jsonl'
    if ledger.resolve().is_relative_to(root.resolve()):
        raise ValidationError('append ledger must be outside the sealed evidence directory')
    append_receipts(ledger,[record])
    # A receipt file is exposed only after the append ledger confirms its batch.
    _write(root/'receipt.json',record,exclusive=True)
    return dict(status='observation_recorded', observed_status=record['result']['observed_status'],
        receipt_id=record['receipt_id'], directory=str(root), ledger=str(ledger),
        evidence_status=evidence_status,production_acceptance='not_established', recomputed=False)


def record(result, directory, *, binding, artifacts=None, ledger=None, provenance=None):
    """Seal an already collected result and explicitly selected evidence files.

Failed and partial observations are valid records of failure or partial work;
they are never changed into successful controls by receipt creation.
"""
    return _seal(result,_new_root(directory),binding=binding,artifacts=artifacts,
                 ledger=ledger,provenance=provenance)


def _authorities(binding, authorities_file, credentials_by_role, credential_factory=None):
    if bool(authorities_file)==bool(credentials_by_role):
        raise ValidationError('supply either a role-principal file or explicit role credentials')
    if credentials_by_role is not None: return credentials_by_role
    raw=json.loads(Path(authorities_file).read_text(encoding='utf-8'))
    pattern=r'[a-z][a-z0-9-]{4,28}[a-z0-9]@'+re.escape(binding.project)+r'\.iam\.gserviceaccount\.com'
    if (not isinstance(raw,dict) or not raw or set(raw)-set(denials.ROLES)
            or any(not isinstance(p,str) or not re.fullmatch(pattern,p) for p in raw.values())
            or len(set(raw.values()))!=len(raw)):
        raise ValidationError('authority file must contain only distinct role-to-service-account names in the bound project')
    if credential_factory is None:
        import google.auth
        from google.auth import impersonated_credentials
        source,_=google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
        def credential_factory(role,principal):
            # Probe IAM under sufficient API scope. The gateway may issue a
            # narrower writer token, but insufficient OAuth scope must not be
            # mistaken for a proven IAM denial in this independent harness.
            scopes=['https://www.googleapis.com/auth/cloud-platform']
            return impersonated_credentials.Credentials(source_credentials=source,target_principal=principal,
                target_scopes=scopes,lifetime=900,quota_project_id=binding.project)
    return {role:denials.RoleCredential(principal,credential_factory(role,principal)) for role,principal in raw.items()}


def run_denials(binding, directory, *, expected_creation_time, expected_source_sha256,
        authorities_file=None, credentials_by_role=None, copy_source_creation_time=None,
        artifacts=None, ledger=None, provenance=None, reader=None,
        transport_factory=denials.NativeTransport, credential_factory=None, execute=denials.run,
        progress=None):
    """Collect denial observations once; preserve and seal partial exceptions.

The role file contains principal names only, never tokens, keys or credential
paths. Progress is operational output. Final observed numbers are in the sealed
result referenced by the returned receipt. Offline verification does not call
this function or attempt any mutation again.
"""
    root=_new_root(directory)
    frozen_execution=({**_execution(),'pin_scope':'before_execute_checked_after'}
                      if provenance is None else deepcopy(provenance))
    state=dict(schema_version='tokenledger-bigquery-boundary-progress/v1',status='running',
        started_at=_now(), binding_identity=binding.identity, cases=[],
        inputs=dict(sha256=expected_source_sha256,input_row_range=[None,None]),
        pinned_creation_time=str(expected_creation_time),production_acceptance='not_established')
    _write(root/'progress-state.json',state,exclusive=True)
    events=root/'progress.jsonl'; events.touch(exist_ok=False)
    def retain(event):
        with events.open('a',encoding='utf-8',newline='\n') as handle:
            handle.write(canonical(event)+'\n');handle.flush();os.fsync(handle.fileno())
        state['cases'].append(event)
        _write(root/'progress-state.json',state)
        if progress: progress(event)
    try:
        roles=_authorities(binding,authorities_file,credentials_by_role,credential_factory)
        result=execute(binding,expected_creation_time=expected_creation_time,
            expected_source_sha256=expected_source_sha256,credentials_by_role=roles,reader=reader,
            transport_factory=transport_factory,copy_source_creation_time=copy_source_creation_time,progress=retain)
    except BaseException as exc:
        result={**state,'status':'failed','finished_at':_now(),'partial_evidence':True,
            'failure':dict(type=type(exc).__name__,message_sha256=hashlib.sha256(str(exc).encode('utf-8')).hexdigest()),
            'remaining':['run did not produce a complete denial matrix; no retry was attempted']}
    if provenance is None:
        after=_execution()
        if (after['execution_hash']!=frozen_execution['execution_hash']
                or after['git_sha']!=frozen_execution['git_sha']):
            _write(root/'execution-after.json',after,exclusive=True)
            result={**result,'status':'failed','collected_status':result.get('status','unknown'),
                    'execution_changed_during_run':True}
    state.update(status=result.get('status','unknown'),finished_at=result.get('finished_at') or _now())
    _write(root/'progress-state.json',state)
    return _seal(result,root,binding=binding,artifacts=artifacts,ledger=ledger,provenance=frozen_execution)


def verify(key, directory, *, ledger=None):
    """Verify every retained proof byte without credentials, cloud calls or SQL."""
    root=Path(directory).absolute()
    _reject_links(root)
    for name in ('receipt.json','manifest.json'):
        if not _path(root,name).is_file(): raise ValidationError('missing boundary '+name)
    record=json.loads((root/'receipt.json').read_text(encoding='utf-8'))
    if (root/'receipt.json').read_bytes()!=(canonical(record)+'\n').encode('utf-8'):
        raise ValidationError('boundary receipt bytes are not canonical')
    # Reuse the shared receipt parser and caller-supplied content address.
    parsed=parse_receipts((canonical(record)+'\n').encode('utf-8'))
    if key not in parsed or record.get('schema_version')!=SCHEMA:
        raise ValidationError('unknown boundary observation receipt')
    if ledger is not None and read_receipts(ledger).get(key)!=record:
        raise ValidationError('boundary receipt differs from append ledger')
    manifest=json.loads((root/'manifest.json').read_text(encoding='utf-8'))
    if (manifest.get('schema_version')!=MANIFEST or manifest.get('binding_identity')!=record['binding_identity']
            or manifest.get('evidence_status')!=record['result']['evidence_status']
            or digest(manifest)!=record['result']['manifest_sha256']
            or _file_hash(root/'manifest.json')!=record['result']['manifest_bytes_sha256']):
        raise ValidationError('boundary evidence manifest changed')
    for name in manifest.get('files',{}): _path(root,name)
    if _inventory(root)!=manifest.get('files'):
        raise ValidationError('boundary evidence file missing, changed, or unregistered')
    result=json.loads((root/'result.json').read_text(encoding='utf-8'))
    binding=json.loads((root/'binding.json').read_text(encoding='utf-8'))
    execution=json.loads((root/'execution.json').read_text(encoding='utf-8'))
    if (digest(binding)!=record['binding_identity'] or digest(result)!=record['result']['report_sha256']
            or result.get('status','unknown')!=record['result']['observed_status']
            or execution['git_sha']!=record['git_sha']
            or digest(execution['artifacts'])!=record['query_hash']
            or execution['execution_hash']!=record['execution_hash']):
        raise ValidationError('boundary observation differs from retained inputs')
    return dict(receipt_id=key,verified=True,recomputed=False,no_cloud_jobs_submitted=True,
        observed_status=record['result']['observed_status'],result=record['result'],
        evidence_status=record['result']['evidence_status'],production_acceptance='not_established')
