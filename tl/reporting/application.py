"""Explicit private application roots and durable stream identity."""

from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
import shutil
import uuid

from tl.stream import Activity, Stream, StreamReader, ValidationError
from tl.stream.binding import Binding
from tl.stream.events import Catalog, UTC, canonical
from tl.stream.store import prepare_row


def private_path(path):
    path = Path(path).resolve()
    # Includes linked worktrees (.git file), nested repos and symlink escapes.
    for parent in (path, *path.parents):
        if (parent / '.git').exists():
            raise ValidationError('private application paths must be outside repositories')
    return path


class Application:
    name = 'personal-usage'
    template = Path('definitions/usage')

    def __init__(self, config, *, _allow_missing_derived=False):
        self.config = private_path(config)
        self.root = self.config.parent
        try:
            self.spec = json.loads(self.config.read_bytes())
        except (OSError, ValueError):
            raise ValidationError('private application configuration unavailable or invalid') from None
        if (not isinstance(self.spec,dict)
                or set(self.spec) != {'schema_version','application','binding_id','catalog_sha256','database'}
                or self.spec['schema_version'] != 'tokenledger-application/v1'
                or self.spec['application'] != self.name
                or not isinstance(self.spec['binding_id'],str)
                or not isinstance(self.spec['catalog_sha256'],str)
                or not re.fullmatch('[0-9a-f]{32}', self.spec['binding_id'])
                or not re.fullmatch('[0-9a-f]{64}', self.spec['catalog_sha256'])):
            raise ValidationError('unsupported private application binding')
        self.db = self.path(self.spec['database'])
        self.catalog_root = self.path('catalog')
        self.evidence = self.path('evidence')
        self.outputs = self.path('outputs')
        self.ledger = self.path('receipts/metrics.jsonl')
        self.binding = Binding(self.name, self.spec['binding_id'], self.spec['catalog_sha256'])
        self._catalog_check(allow_missing_derived=_allow_missing_derived)

    def path(self, relative):
        if (not isinstance(relative,str) or not relative or Path(relative).is_absolute()
                or '..' in Path(relative).parts):
            raise ValidationError('application path must be relative to its private root')
        result = private_path(self.root / relative)
        if not result.is_relative_to(self.root) or result == self.root:
            raise ValidationError('application path escapes private root')
        return result

    def _catalog_check(self, *, allow_missing_derived=False):
        paths = sorted(self.template.rglob('*.yaml'))
        if not paths:
            raise ValidationError('application catalog templates are unavailable')
        relative = {p.name for p in (self.template/'activities').glob('*.yaml')}
        if {p.name for p in (self.catalog_root/'activities').glob('*.yaml')} != relative:
            raise ValidationError('private activity inventory differs from the registered release')
        for p in paths:
            local = self.path('catalog/' + p.relative_to(self.template).as_posix())
            if not local.is_file() and allow_missing_derived and p.relative_to(self.template).parts[0] != 'activities':
                continue
            if not local.is_file():
                raise ValidationError('private derived catalog needs explicit catalog-sync')
            if local.read_bytes().replace(b'\r\n',b'\n') != p.read_bytes().replace(b'\r\n',b'\n'):
                raise ValidationError('private catalog differs from the registered immutable version')
        if Catalog(self.catalog_root/'activities').digest != self.binding.catalog_hash:
            raise ValidationError('application catalog does not match the stream binding')

    @classmethod
    def sync_catalog(cls, config, *, actor):
        if not isinstance(actor,str) or not actor.strip() or actor != actor.strip():
            raise ValidationError('an explicit actor is required')
        app=cls(config,_allow_missing_derived=True)
        with app.reader().connect():
            pass
        added={}
        for p in sorted(cls.template.rglob('*.yaml')):
            target=app.path('catalog/'+p.relative_to(cls.template).as_posix())
            if target.exists():
                continue
            if p.relative_to(cls.template).parts[0]=='activities':
                raise ValidationError('activity catalog requires a separate version transition')
            target.parent.mkdir(parents=True,exist_ok=True)
            content=p.read_bytes().replace(b'\r\n',b'\n')
            with target.open('xb') as handle:
                handle.write(content)
            added[p.relative_to(cls.template).as_posix()]=hashlib.sha256(content).hexdigest()
        app.validate()
        if added:
            evidence=app.path('evidence/catalog-sync/'+uuid.uuid4().hex+'.json')
            evidence.parent.mkdir(parents=True,exist_ok=True)
            evidence.write_text(canonical(dict(actor=actor,actor_basis='caller_claim',
                binding_id=app.binding.identity,added=added,ts=datetime.now(UTC).isoformat()))+'\n',encoding='utf-8')
        return app

    @classmethod
    def initialize(cls, config, *, actor):
        if not isinstance(actor,str) or not actor.strip() or actor != actor.strip():
            raise ValidationError('an explicit nonempty actor is required')
        config = private_path(config)
        if config.exists():
            app = cls(config)
            app.validate()
            return app
        root = config.parent
        if root.exists() and any(root.iterdir()):
            raise ValidationError('new application requires an empty private root; no adoption or overwrite')
        identity = uuid.uuid4().hex
        catalog_hash = Catalog(cls.template/'activities').digest
        root.mkdir(parents=True,exist_ok=True)
        shutil.copytree(cls.template,root/'catalog')
        for directory in ('evidence','outputs','receipts'):
            (root/directory).mkdir()
        spec = dict(schema_version='tokenledger-application/v1', application=cls.name,
                    binding_id=identity,catalog_sha256=catalog_hash,database='usage.duckdb')
        with config.open('x',encoding='utf-8',newline='\n') as handle:
            handle.write(canonical(spec)+'\n')
        app = cls(config)
        writer = Stream(app.db,app.catalog_root/'activities',binding=app.binding)
        writer.init()
        now = datetime.now(UTC)
        event = Activity('binding:'+identity,now,'application_bound',
            dict(application=cls.name,binding_id=identity,catalog_sha256=catalog_hash),'app:'+identity)
        row = prepare_row(event,now,writer.catalog,source=app.binding.source_prefix+'binding',actor=actor,lane='dev')
        writer._write([row],_binding_init=True)
        app.validate()
        return app

    def validate(self):
        self._catalog_check()
        # Re-resolve every configured root to catch replacement with a symlink.
        for relative in (self.spec['database'],'catalog','evidence','outputs','receipts/metrics.jsonl'):
            self.path(relative)
        with self.reader().connect():
            pass

    def reader(self):
        return StreamReader(self.db,binding=self.binding)

    def append(self, events, *, source, actor):
        self.validate()
        if not re.fullmatch('[a-z][a-z0-9_-]*',source) or source == 'binding':
            raise ValidationError('invalid application source alias')
        if not isinstance(actor,str) or not actor.strip() or actor != actor.strip():
            raise ValidationError('an explicit actor is required')
        def admitted():
            for event in events:
                if event.activity == 'application_bound':
                    raise ValidationError('application binding is immutable')
                yield event
        return Stream(self.db,self.catalog_root/'activities',binding=self.binding).append(
            admitted(),source=self.binding.source_prefix+source,actor=actor)

    def registry(self):
        from tl.metrics.definitions import DefinitionRegistry
        return DefinitionRegistry(self.catalog_root/'metrics',('usage_readiness','usage_tokens','usage_gaps'),
                                  Path('tl/usage/sql'),self.template/'metrics',{'usage_gaps':'v2'})

    def artifacts(self, definitions):
        from tl.receipts.metrics import execution_artifacts
        self.validate()
        result = execution_artifacts(definitions)
        for path in sorted(self.template.rglob('*.yaml')):
            result[path.as_posix()] = hashlib.sha256(path.read_bytes().replace(b'\r\n',b'\n')).hexdigest()
        for path in sorted(Path('tl/usage/sql').rglob('*.sql')):
            result[path.as_posix()] = hashlib.sha256(path.read_bytes().replace(b'\r\n',b'\n')).hexdigest()
        return dict(sorted(result.items()))

    def session(self, *, asof, known_at=None, watermark=None, **options):
        from tl.usage.session import UsageSession
        return UsageSession(self,asof=asof,known_at=known_at,watermark=watermark,**options)

    def check_roots(self, db, output_root, ledger):
        self.validate()
        if (Path(db).resolve()!=self.db or Path(output_root).resolve()!=self.outputs
                or Path(ledger).resolve()!=self.ledger):
            raise ValidationError('report paths differ from the explicit application binding')
