"""Subprocess boundary around an independently authored dbt project."""
from pathlib import Path
from time import perf_counter
import json
import os
import subprocess

import duckdb
import yaml

from tl.stream.events import ValidationError


def dbt_executable():
    configured=os.environ.get('TL_DBT_EXECUTABLE')
    if configured:
        return str(Path(configured).resolve())
    path=Path('.venv-baseline')/('Scripts/dbt.exe' if os.name=='nt' else 'bin/dbt')
    if not path.is_file():
        raise ValidationError('install baseline/requirements-lock.txt into .venv-baseline or set TL_DBT_EXECUTABLE')
    return str(path.resolve())


def prepare(snapshot,root,allocation):
    root=Path(root);root.mkdir(parents=True,exist_ok=True)
    path=root/'baseline.duckdb'
    started=perf_counter()
    with duckdb.connect(str(path)) as conn:
        conn.execute('SET threads=4');conn.execute('CREATE SCHEMA IF NOT EXISTS raw')
        conn.register('snapshot',snapshot)
        for spec in sorted(Path('definitions/activities').glob('*.yaml')):
            kind=spec.stem
            if kind=='revenue_recognized':
                continue
            conn.execute(f"CREATE OR REPLACE TABLE raw.{kind} AS SELECT * FROM snapshot WHERE activity=?",[kind])
        conn.register('allocation_input',allocation)
        conn.execute('CREATE OR REPLACE TABLE raw.allocation AS SELECT * FROM allocation_input')
    profile={'tokenledger_baseline':{'target':'local','outputs':{'local':{
        'type':'duckdb','path':str(path.resolve()),'schema':'main','threads':4,
        'settings':{'TimeZone':'UTC','memory_limit':'4GB','threads':4}}}}}
    (root/'profiles.yml').write_text(yaml.safe_dump(profile),encoding='utf-8')
    return perf_counter()-started


def execute(root,*,project='baseline',asof,first_month,nrr_floor_cents=1000000,select=None,command='build',executable=None):
    root=Path(root).resolve();project=Path(project).resolve()
    variables=dict(asof=asof,first_month=first_month,nrr_floor_cents=nrr_floor_cents)
    args=[executable or dbt_executable(),'--no-use-colors','--no-partial-parse',command,'--project-dir',str(project),
          '--profiles-dir',str(root),'--target-path',str(root/'target'),'--log-path',str(root/'logs'),
          '--vars',json.dumps(variables)]
    if select:
        args+=['--select',*select]
    env={**os.environ,'DBT_SEND_ANONYMOUS_USAGE_STATS':'false','DO_NOT_TRACK':'1'}
    start=perf_counter()
    with (root/'dbt.log').open('w',encoding='utf-8') as log:
        result=subprocess.run(args,stdout=log,stderr=subprocess.STDOUT,env=env)
    seconds=perf_counter()-start
    if result.returncode:
        raise ValidationError(f'dbt {command} failed; inspect {root / "dbt.log"}')
    return dict(seconds=seconds,command=args,variables=variables,
                run_results=json.loads((root/'target/run_results.json').read_text(encoding='utf-8')),
                manifest=json.loads((root/'target/manifest.json').read_text(encoding='utf-8')))


def append_projection(snapshot,root):
    """Append newly visible immutable events; shrinking knowledge must use a new build."""
    changed=[];counts={};started=perf_counter()
    with duckdb.connect(str(Path(root)/'baseline.duckdb')) as conn:
        conn.execute('SET threads=4');conn.register('next_snapshot',snapshot)
        for spec in sorted(Path('definitions/activities').glob('*.yaml')):
            kind=spec.stem
            if kind=='revenue_recognized': continue
            missing=conn.execute(f'SELECT count(*) FROM raw.{kind} r ANTI JOIN next_snapshot n USING(activity_id)').fetchone()[0]
            if missing: raise ValidationError('incremental baseline requires monotone frozen source knowledge')
            count=conn.execute(f"SELECT count(*) FROM next_snapshot n ANTI JOIN raw.{kind} r USING(activity_id) WHERE n.activity=?",[kind]).fetchone()[0]
            if count:
                conn.execute(f"INSERT INTO raw.{kind} SELECT n.* FROM next_snapshot n ANTI JOIN raw.{kind} r USING(activity_id) WHERE n.activity=?",[kind])
                changed.append(kind);counts[kind]=count
    return dict(seconds=perf_counter()-started,changed_sources=changed,inserted=counts)


def rows(root,name):
    return table(root,name).to_pylist()


def table(root,name):
    if not name.replace('_','').isalnum():
        raise ValidationError('invalid registered baseline model')
    with duckdb.connect(str(Path(root)/'baseline.duckdb'),read_only=True) as conn:
        return conn.execute(f'SELECT * FROM main.{name}').fetch_arrow_table()
