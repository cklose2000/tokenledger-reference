"""An allowlisted container context; no Git history, accounts, keys or evidence."""
from pathlib import Path
import hashlib
import json
import shutil
import stat
import subprocess

from tl.bigquery.gateway import GatewayConfig
from tl.release import IMAGE
from tl.stream import ValidationError


def package(config_path, output, *, root=Path('.')):
    def reject_links(path):
        for part in (Path(path).absolute(), *Path(path).absolute().parents):
            if part.exists() and (part.is_symlink() or getattr(part.lstat(),'st_file_attributes',0) & stat.FILE_ATTRIBUTE_REPARSE_POINT):
                raise ValidationError('linked gateway source or destination refused')
    reject_links(root); reject_links(config_path); reject_links(output)
    config = GatewayConfig.read(config_path)
    destination = Path(output)
    if destination.exists():
        raise ValidationError('gateway build context already exists')
    root = Path(root).resolve()
    if destination.resolve().is_relative_to(root):
        raise ValidationError('gateway build context must be outside the source checkout')
    tracked=subprocess.check_output(['git','ls-files','-z','--','tl','definitions/activities'],cwd=root).decode().split('\0')
    selected={name for name in tracked if name and (name.endswith('.py') or
              name=='tl/stream/schema.sql' or name.startswith('definitions/activities/') and name.endswith('.yaml'))}
    # These reviewed files may be packaged before their containing private PR.
    selected.add('tl/bigquery/gateway.py')
    destination.mkdir(parents=True)
    for name in ('pyproject.toml', 'LICENSE'):
        reject_links(root/name)
        shutil.copyfile(root/name, destination/name)
    for name in sorted(selected):
        source=root/name
        reject_links(source)
        target=destination/name
        target.parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(source,target)
    config.save(destination/'gateway.json')
    (destination/'Dockerfile').write_text(f'''FROM {IMAGE}
WORKDIR /app
COPY pyproject.toml LICENSE /app/
COPY tl /app/tl
COPY definitions /app/definitions
RUN python -m pip install --no-cache-dir '.[bigquery]'
COPY gateway.json /app/gateway.json
ENV PYTHONUNBUFFERED=1
USER 10001:10001
CMD ["python", "-m", "tl", "cloud", "bigquery", "gateway-serve", "--gateway-config", "/app/gateway.json", "--json"]
''', encoding='utf-8')
    files = [dict(path=p.relative_to(destination).as_posix(), sha256=hashlib.sha256(p.read_bytes()).hexdigest())
             for p in sorted(destination.rglob('*')) if p.is_file()]
    result = dict(schema_version='tokenledger-gateway-context/v1',
                  gateway_identity=config.identity, base_image=IMAGE, files=files,
                  deployment='not_run', credential_scan='required_before_upload',
                  reproducibility='context bytes are hashed; resolved image digest and transitive dependency inventory must be retained after build')
    (destination/'context-manifest.json').write_text(json.dumps(result, sort_keys=True, indent=2)+'\n', encoding='utf-8')
    return result
