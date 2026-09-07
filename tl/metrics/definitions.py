"""Load immutable version paths and restrict query substitutions to declared cuts."""

from pathlib import Path
from dataclasses import dataclass
from decimal import Decimal
import hashlib
import re
import yaml
import pyarrow as pa

from tl.stream.events import ValidationError

FAMILIES=('net_rev_per_mtok','cost_per_mtok','rev_per_mw','consumption_nrr','model_vintage','subscriptions','customer_cohorts')
DEFAULT_VERSIONS={name:('v2' if name=='consumption_nrr' else 'v1') for name in FAMILIES}


@dataclass(frozen=True)
class DefinitionRegistry:
    root: Path
    names: tuple
    sql_root: Path
    artifact_root: Path
    default_versions: dict | None = None


class Definition:
    def __init__(self,name,version=None,cuts=None,*,registry=None):
        version=version or (DEFAULT_VERSIONS.get(name,'v1') if registry is None else (registry.default_versions or {}).get(name,'v1'))
        names=FAMILIES if registry is None else registry.names
        if name not in names or not re.fullmatch(r'v[1-9][0-9]*',version):
            raise ValidationError('unknown metric or invalid definition version')
        self.path=(Path('definitions/metrics') if registry is None else registry.root)/name/f'{version}.yaml'
        self.artifact_path=self.path if registry is None else registry.artifact_root/name/f'{version}.yaml'
        if not self.path.is_file():
            raise ValidationError(f'definition does not exist: {name}/{version}')
        self.content=self.path.read_bytes().replace(b'\r\n',b'\n')
        if registry is not None and self.content!=self.artifact_path.read_bytes().replace(b'\r\n',b'\n'):
            raise ValidationError('application definition differs from its registered release')
        self.spec=yaml.safe_load(self.content)
        if self.spec['name']!=name or self.spec['version']!=version:
            raise ValidationError('definition name/version differs from its immutable path')
        self.name,self.version=name,version
        self.digest=hashlib.sha256(self.content).hexdigest()
        self.cuts=list(self.spec['default_cuts'] if cuts is None else cuts)
        if len(set(self.cuts))!=len(self.cuts) or set(self.cuts)-set(self.spec['allowed_cuts']+self.spec['dimensions']):
            raise ValidationError(f'unsupported or duplicate cut for {name}')
        # Fixed grain dimensions (for example plan) never duplicate GROUP BY fields.
        self.cuts=[cut for cut in self.cuts if cut not in self.spec['dimensions']]
        self.dimensions=self.spec['dimensions']+self.cuts
        path=Path(self.spec['formula']['sql'])
        sql_root=Path('tl/metrics/sql') if registry is None else registry.sql_root
        versioned=re.fullmatch(rf'{re.escape(sql_root.as_posix())}/{name}/v([1-9][0-9]*)\.sql',path.as_posix())
        if path.resolve()!=(sql_root/f'{name}.sql').resolve() and not (
                versioned and int(versioned.group(1))<=int(version[1:])):
            raise ValidationError('metric SQL must use its registered query path')
        template=path.read_text(encoding='utf-8')
        self.sql=template.format(cuts=''.join(','+cut for cut in self.cuts))
        self.parameters={}
        if name=='consumption_nrr':
            lenses=[]
            for row in self.spec['lenses']:
                cents=Decimal(str(row['floor_usd']))*100
                if cents!=int(cents) or cents<=0 or row['months'] not in (3,12) or row['annualization']!=12//row['months']:
                    raise ValidationError('invalid NRR lens window/floor')
                lenses.append({key:row[key] for key in ('lens','months','annualization','include_subscription')} | {'floor_cents':int(cents)})
            if len({row['lens'] for row in lenses})!=len(lenses):
                raise ValidationError('duplicate NRR lens')
            self.parameters['_nrr_lenses']=lenses
        if name=='customer_cohorts':
            thresholds=[int(Decimal(str(value))*100) for value in self.spec['thresholds_usd']]
            if not thresholds or thresholds!=sorted(set(thresholds)) or thresholds[0]<=0:
                raise ValidationError('cohort thresholds must be positive, distinct, and increasing')
            self.parameters['_cohort_thresholds']=[dict(threshold_cents=value) for value in thresholds]

    def execute(self,session):
        for name,rows in self.parameters.items():
            session.conn.register(name,pa.Table.from_pylist(rows))
        result=session.rows(self.sql)
        if self.name=='consumption_nrr':
            for row in result:
                if row['current_revenue_cents']!=row['base_revenue_cents']+row['expansion_cents']-row['contraction_cents']-row['churn_cents']:
                    raise ValidationError('NRR decomposition does not reconcile to the cent')
        if self.name=='customer_cohorts':
            previous={}
            for row in result:
                if row['customers']>previous.get(row['month'],float('inf')):
                    raise ValidationError('cohort threshold counts are not monotone')
                previous[row['month']]=row['customers']
        return result
