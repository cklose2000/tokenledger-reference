"""Optional application binding, encoded in the canonical stream itself.

An application guard, not administrator isolation or authenticated identity.
Unbound historical synthetic streams retain their original behavior.
"""

from dataclasses import dataclass
from functools import lru_cache
from importlib.resources import files

import duckdb

from .events import ValidationError


def contract(conn):
    columns = conn.execute("PRAGMA table_info('stream.activity')").fetchall()
    constraints = conn.execute("""SELECT constraint_type, constraint_column_names, expression
        FROM duckdb_constraints() WHERE schema_name='stream' AND table_name='activity'
        ORDER BY constraint_type, constraint_column_names, expression""").fetchall()
    return columns, constraints


@lru_cache(maxsize=1)
def physical_contract():
    with duckdb.connect() as conn:
        conn.execute(files('tl.stream').joinpath('schema.sql').read_text(encoding='utf-8'))
        return contract(conn)


@dataclass(frozen=True)
class Binding:
    application: str
    identity: str
    catalog_hash: str

    @property
    def source_prefix(self):
        return f'tl-app:{self.identity}:'


def check_binding(conn, binding=None, *, initializing=False):
    exists = conn.execute("""SELECT count(*) FROM information_schema.tables
        WHERE table_schema='stream' AND table_name='activity'""").fetchone()[0]
    if not exists:
        if initializing:
            return
        raise ValidationError('application stream is missing')
    # A bound writer also rejects a substituted or weakened physical contract.
    if binding is not None and contract(conn) != physical_contract():
        raise ValidationError('application physical stream contract differs')
    markers = conn.execute("""SELECT activity_id, customer, feature_json::VARCHAR,
        _source, _schema_hash, _stream_position FROM stream.activity
        WHERE activity='application_bound' OR starts_with(_source,'tl-app:')
        ORDER BY _stream_position LIMIT 1""").fetchall()
    if binding is None:
        if markers:
            raise ValidationError('bound application requires its explicit context')
        return
    import json
    total = conn.execute('SELECT count(*) FROM stream.activity').fetchone()[0]
    if initializing and total == 0:
        return
    if not markers:
        raise ValidationError('missing application binding; unknown streams cannot be adopted')
    row = markers[0]
    expected = dict(application=binding.application, binding_id=binding.identity,
                    catalog_sha256=binding.catalog_hash)
    if (row[0] != 'binding:' + binding.identity or row[1] != 'app:' + binding.identity
            or json.loads(row[2]) != expected or row[3] != binding.source_prefix + 'binding'
            or row[4] != binding.catalog_hash or row[5] != 1):
        raise ValidationError('application binding does not match protected configuration')
    invalid = conn.execute("""SELECT count(*) FROM stream.activity
        WHERE NOT starts_with(_source, ?) OR _schema_hash != ? OR _lane != 'dev'
           OR (activity='application_bound' AND activity_id != ?)""",
        [binding.source_prefix, binding.catalog_hash, 'binding:' + binding.identity]).fetchone()[0]
    if invalid:
        raise ValidationError('mixed application provenance or catalog')
