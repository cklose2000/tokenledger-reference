"""Render the registered matrix and reject edits to released calculation contracts."""
import hashlib
from pathlib import Path
import subprocess
import yaml
from tl.stream import ValidationError


def render(destination,source=Path('controls/matrix.yaml')):
    raw=Path(source).read_bytes();matrix=yaml.safe_load(raw)
    fields=['id','objective','control_activity','system_mechanism','evidence_artifact','frequency','owner_role','test_procedure','assertion']
    lines=['# KPI control matrix','',matrix['scope'],'',f"Source: `{source}`; version `{matrix['version']}`; SHA-256 `{hashlib.sha256(raw).hexdigest()}`.",'',
        '| '+' | '.join(fields)+' |','|'+'|'.join(['---']*len(fields))+'|']
    for control in matrix['controls']:
        lines.append('| '+' | '.join(str(control[f]).replace('|','\\|').replace('\n',' ') for f in fields)+' |')
    Path(destination).parent.mkdir(parents=True,exist_ok=True)
    Path(destination).write_text('\n'.join(lines)+'\n',encoding='utf-8',newline='\n')
    return dict(status='rendered',path=str(destination),matrix_sha256=hashlib.sha256(raw).hexdigest())


def check_definitions(base=None):
    base=base or yaml.safe_load(Path('definitions/controls/v1.yaml').read_bytes())['released_definitions_base']
    if not isinstance(base,str) or len(base)!=40 or any(c not in '0123456789abcdef' for c in base):
        raise ValidationError('released definition base must be an exact git SHA')
    listing=subprocess.run(['git','ls-tree','-r','--name-only',base,'--','definitions/metrics'],capture_output=True,text=True)
    if listing.returncode:
        return dict(status='not_run',base=base,reason='released git history unavailable',violations=[])
    paths=set(listing.stdout.splitlines())
    for path in list(paths):
        raw=subprocess.run(['git','show',f'{base}:{path}'],capture_output=True,check=True).stdout
        if path.endswith('.yaml'):
            paths.add(yaml.safe_load(raw)['formula']['sql'])
    if not paths:
        raise ValidationError('released definition population is empty')
    violations=[];hashes={}
    for path in sorted(paths):
        raw=subprocess.run(['git','show',f'{base}:{path}'],capture_output=True,check=True).stdout.replace(b'\r\n',b'\n')
        hashes[path]=hashlib.sha256(raw).hexdigest()
        if not Path(path).is_file() or Path(path).read_bytes().replace(b'\r\n',b'\n')!=raw:
            violations.append(path)
    return dict(status='failed' if violations else 'passed',base=base,violations=violations,released_hashes=hashes,
                scope='immutability against identified released checkpoint; new versions still require review and bridge')
