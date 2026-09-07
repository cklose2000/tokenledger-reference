"""An operator-owned scratch binding, separate from source and receipt data."""
from dataclasses import asdict, dataclass
from pathlib import Path
import json
import re

from tl.receipts.metrics import digest
from tl.stream import ValidationError


class NotRun(ValidationError):
    """Required operator cloud binding is absent; no cloud call was made."""


@dataclass(frozen=True)
class Binding:
    project: str
    dataset: str
    location: str
    maximum_bytes_billed: int
    maximum_run_bytes_billed: int
    purpose: str = 'synthetic-scratch'
    query_principal: str | None = None
    writer_principal: str | None = None

    def __post_init__(self):
        if not re.fullmatch(r'[a-z][a-z0-9-]{4,61}[a-z0-9]', self.project):
            raise ValidationError('invalid explicit GCP project ID')
        if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,127}', self.dataset):
            raise ValidationError('invalid explicit BigQuery dataset')
        if not re.fullmatch(r'(US|EU|[a-z]+(?:-[a-z]+)+[0-9])', self.location):
            raise ValidationError('explicit BigQuery location is required')
        if (type(self.maximum_bytes_billed) is not int or self.maximum_bytes_billed <= 0
                or type(self.maximum_run_bytes_billed) is not int
                or self.maximum_run_bytes_billed < self.maximum_bytes_billed):
            raise ValidationError('positive per-query and aggregate run byte ceilings are required')
        if self.purpose != 'synthetic-scratch':
            raise ValidationError('this release accepts only a synthetic scratch binding')
        if bool(self.query_principal)!=bool(self.writer_principal):
            raise ValidationError('bind both query and writer principals, or explicitly use operator scratch mode')
        if self.query_principal:
            pattern=r'[a-z][a-z0-9-]{4,28}[a-z0-9]@'+re.escape(self.project)+r'\.iam\.gserviceaccount\.com'
            if (not re.fullmatch(pattern,self.query_principal) or not re.fullmatch(pattern,self.writer_principal)
                    or self.query_principal==self.writer_principal):
                raise ValidationError('query/writer principals must be distinct service accounts in the bound project')

    @property
    def table(self): return f'{self.project}.{self.dataset}.activity'

    @property
    def identity(self): return digest(asdict(self))

    @property
    def authority_mode(self):
        return 'explicit_impersonation_unverified_grants' if self.query_principal else 'operator_scratch_unverified_grants'

    def save(self, path):
        path = Path(path)
        if path.exists(): raise ValidationError('binding already exists; use a new path for a changed boundary')
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(asdict(self), indent=2)+'\n', encoding='utf-8')

    @classmethod
    def read(cls, path):
        if not path: raise NotRun('Supply an explicit --config binding; no cloud project was selected.')
        try: return cls(**json.loads(Path(path).read_text(encoding='utf-8')))
        except (TypeError, KeyError) as exc: raise ValidationError('invalid BigQuery binding') from exc
