"""Readiness queries on the shared snapshot; no synthetic reporting dependencies."""

from datetime import date,datetime,time,timedelta
from pathlib import Path

import duckdb
import pyarrow as pa

from tl.stream.events import UTC,ValidationError,iso,timestamp


class UsageSession:
    def __init__(self, application, *, asof, known_at=None, watermark=None, start=None,end=None,timezone='UTC'):
        application.validate()
        self.application = application
        self.asof = date.fromisoformat(asof)
        default_end = datetime.combine(self.asof+timedelta(days=1),time(),UTC)
        self.known_at = timestamp(known_at) if known_at else default_end
        self.options={}
        self.window=None
        if start is not None or end is not None:
            from tl.usage.window import window
            self.window=window(start,end,timezone)
            self.options=dict(start=start,end=end,timezone=timezone)
        self.reader = application.reader()
        with self.reader.connect() as conn:
            latest = conn.execute('SELECT max(_stream_position) FROM stream.activity').fetchone()[0]
        self.watermark = latest if watermark is None else watermark
        if type(self.watermark) is not int or not 1 <= self.watermark <= latest:
            raise ValidationError('invalid application watermark')
        # Usage bounds filter the domain query. Administrative inventory remains
        # visible at the knowledge cutoff even for an earlier usage interval.
        self.snapshot_asof=max(self.asof,self.known_at.date()).isoformat() if self.window else asof
        self.snapshot = self.reader.snapshot(asof=self.snapshot_asof,known_at=iso(self.known_at),
            watermark=self.watermark,include_provenance=True)
        self.conn = duckdb.connect()
        self.conn.execute("SET TimeZone='UTC'")
        self.conn.execute('SET threads=4')
        self.conn.register('_snapshot',self.snapshot)
        self.sql = {}
        if self.window:
            bounds=dict(start_utc=timestamp(self.window['start_utc']),end_utc=timestamp(self.window['end_utc']),
                        known_at=self.known_at,report_timezone=timezone)
            self.conn.register('_usage_window',pa.Table.from_pylist([bounds]))
            for name in ('observations','intervals'):
                query=Path(f'tl/usage/sql/views/{name}.sql').read_text(encoding='utf-8')
                self.sql[name]=query
                self.conn.execute(query)

    def rows(self, sql):
        result = self.conn.execute(sql)
        names = [col[0] for col in result.description]
        return [dict(zip(names,row)) for row in result.fetchall()]

    def verify(self):
        self.application.validate()
        from tl.usage.importer import verify_packets
        verify_packets(self.application,self.snapshot)
        return dict(application_binding=True,physical_schema=True,
                    interpretation='observed cumulative counter growth; no billed or full-account totals' if self.window else
                    'readiness only; no token-consumption or billing totals')

    def build_manifest(self):
        result=dict(asof=self.asof.isoformat(),known_at=iso(self.known_at),watermark=self.watermark,
                    application=self.application.name,binding_id=self.application.binding.identity,
                    application_config=str(self.application.config),scaffolds=self.sql)
        if self.window:
            result.update(session_options=self.options,report_window=self.window,snapshot_asof=self.snapshot_asof)
        return result

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self,*_):
        self.close()
