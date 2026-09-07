"""An isolated analytical connection; the source database is opened read-only."""

from calendar import monthrange
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
import hashlib
import json
import math

import duckdb
import pyarrow as pa
import yaml

from tl.generate.synthetic import month_add
from tl.reporting.policy import RecognitionPolicy
from tl.stream import StreamReader, ValidationError
from tl.stream.events import UTC, canonical, iso, timestamp

SCAFFOLDS = ('customer_timeline', 'revenue_ledger', 'compute_ledger', 'token_ledger', 'seat_ledger', 'customer_month')


class ReportSession:
    def __init__(self, path, *, asof, known_at=None, watermark=None, _exclude_activity_ids=()):
        self.asof = date.fromisoformat(asof)
        if self.asof.day != monthrange(self.asof.year,self.asof.month)[1]:
            raise ValidationError('Phase 2 reporting requires a calendar month-end --asof')
        self.period_end = self.asof + timedelta(days=1)
        self.known_at = timestamp(known_at) if known_at else datetime.combine(self.period_end,time(),UTC)
        self.reader = StreamReader(path)
        with self.reader.connect() as conn:
            latest = conn.execute('SELECT coalesce(max(_stream_position),0) FROM stream.activity').fetchone()[0]
        self.watermark = latest if watermark is None else watermark
        if not 0 <= self.watermark <= latest:
            raise ValidationError('invalid committed stream watermark')
        self.snapshot = self.reader.snapshot(asof=asof,known_at=iso(self.known_at),watermark=self.watermark,
                                             include_provenance=True,exclude_activity_ids=_exclude_activity_ids)
        self.conn = duckdb.connect()
        self.conn.execute("SET TimeZone='UTC'")
        self.conn.execute('SET threads=4')
        self.conn.register('_snapshot',self.snapshot)
        self.policy = RecognitionPolicy()
        self.conn.register('_recognition_policy',pa.Table.from_pylist([
            dict(source=source,recognition_kind=kind,family=family) for (source,kind),family in self.policy.pairs.items()]))
        allocation = []
        for path in sorted(Path('definitions/allocation').glob('*.yaml')):
            spec = yaml.safe_load(path.read_text(encoding='utf-8'))
            if (spec['unit'] != 'MWh per million tokens' or spec['synthetic'] is not True
                    or spec['version']!='v1' or spec['model']!=path.stem):
                raise ValidationError('unsupported allocation unit or provenance')
            for kind,coefficient in spec['coefficients'].items():
                if type(coefficient) not in (int,float) or not math.isfinite(coefficient) or coefficient<=0:
                    raise ValidationError('energy coefficients must be positive')
                nano = Decimal(str(coefficient))*10**9
                if nano != int(nano):
                    raise ValidationError('allocation precision exceeds integer nano-MWh')
                allocation.append(dict(model=spec['model'],token_type=kind,coefficient_nano=int(nano)))
        if not allocation:
            raise ValidationError('no allocation definitions')
        self.conn.register('_allocation',pa.Table.from_pylist(allocation))
        first = self.conn.execute('SELECT min(ts)::DATE FROM _snapshot').fetchone()[0]
        self.first_month = (first or self.asof).replace(day=1)
        months = []
        current = self.first_month
        while current < self.period_end:
            end = month_add(current,1)
            months.append(dict(month=current,month_end=end,days=(end-current).days))
            current = end
        self.conn.register('_calendar',pa.Table.from_pylist(months))
        self.conn.register('_context',pa.Table.from_pylist([dict(asof=self.asof,period_end=self.period_end,first_month=self.first_month)]))
        self.conn.execute('CREATE SCHEMA report')
        self.sql = {}
        try:
            for name in SCAFFOLDS:
                sql = Path(f'scaffolds/{name}.sql').read_text(encoding='utf-8')
                self.sql[name] = sql
                self.conn.execute(f'CREATE VIEW report.{name} AS {sql}')
        except BaseException:
            self.close()
            raise

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self,*args):
        self.close()

    def rows(self,sql):
        cursor = self.conn.execute(sql)
        names = [column[0] for column in cursor.description]
        return [dict(zip(names,row)) for row in cursor.fetchall()]

    def build_manifest(self):
        return dict(asof=self.asof.isoformat(),known_at=iso(self.known_at),watermark=self.watermark,
                    target='duckdb',scaffolds={name:hashlib.sha256(sql.encode()).hexdigest() for name,sql in self.sql.items()},
                    recognition_policy=dict(version=self.policy.version,sha256=self.policy.digest))
