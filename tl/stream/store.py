"""DuckDB's application-enforced append path and read-only reporting connection.

File owners can bypass this API. This adapter does not claim database IAM or
tamper resistance against an administrator. Native access tests are Phase 4.
"""

from contextlib import contextmanager
from datetime import date, datetime, time, timedelta
from importlib.resources import files
from pathlib import Path
import json

import duckdb
import pyarrow as pa

from .events import Activity, Catalog, UTC, ValidationError, canonical, iso, timestamp


def logical_sql(predicate: str = "TRUE", *, source="stream.activity", provenance=False) -> str:
    # Filter knowledge BEFORE row_number/lead. Distinct typed anonymous keys
    # prevent null customers from leaking unrelated temporal state.
    return f"""
    WITH visible AS (
      SELECT activity_id, ts, customer, anonymous_customer_id, activity,
             feature_json, revenue_impact, link
             {', _recorded_at, _stream_position, _source, _actor, _lane, _schema_hash' if provenance else ''}
      FROM {source} WHERE {predicate}
    )
    SELECT *,
      row_number() OVER timeline AS activity_occurrence,
      lead(ts) OVER timeline AS activity_repeated_at
    FROM visible
    WINDOW timeline AS (
      PARTITION BY CASE WHEN customer IS NOT NULL THEN 'c:' || customer
                        WHEN anonymous_customer_id IS NOT NULL THEN 'a:' || anonymous_customer_id
                        ELSE 'e:' || activity_id END, activity
      ORDER BY ts, activity_id
    )"""


class Stream:
    def __init__(self, path: Path | str, definitions: Path | str = "definitions/activities", *, binding=None):
        self.path = Path(path)
        self.catalog = Catalog(Path(definitions))
        self.binding = binding

    def init(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with duckdb.connect(str(self.path)) as conn:
            from .binding import check_binding
            check_binding(conn, self.binding, initializing=True)
            conn.execute("SET TimeZone='UTC'")
            conn.execute(files("tl.stream").joinpath("schema.sql").read_text(encoding="utf-8"))
            conn.execute("CREATE OR REPLACE VIEW activity_stream AS " + logical_sql())

    def append(self, events, *, source="local", actor="local-writer") -> dict:
        recorded_at = datetime.now(UTC)
        return self._append(((event, recorded_at) for event in events), source=source, actor=actor, lane="dev")

    def generate(self, world, *, workers=4, progress=None, new_world=False):
        """Parallel validated preparation; this instance remains the only DB writer."""
        from tl.generate.parallel import prepared_world
        rows = prepared_world(world, self.catalog, workers=workers, progress=progress)
        return self._write(rows, new_world=new_world)

    def append_simulation(self, events, *, source, actor="synthetic-generator", new_world=False) -> dict:
        """Pairs of (Activity, simulated arrival time). Never used for live ingestion."""
        return self._append(events, source=source, actor=actor, lane="sim", new_world=new_world)

    def restore(self, table):
        """Restore a verified canonical backup to a NEW file, preserving provenance.

        This is the writer's recovery route, never a way to backdate live ingest.
        The caller verifies the transport hash; this method revalidates each row.
        """
        if self.path.exists():
            raise ValidationError('restore requires a new database path')
        columns = ['activity_id','ts','customer','anonymous_customer_id','activity','feature_json',
                   'revenue_impact','link','_recorded_at','_stream_position','_source','_actor','_lane','_schema_hash']
        if set(table.column_names) != set(columns):
            raise ValidationError('backup does not have the canonical physical columns')
        expected_position = 1
        # Restore only whole committed prefixes. No sparse row-position invention.
        for batch in table.sort_by([('_stream_position','ascending')]).to_batches(max_chunksize=8192):
            for row in batch.to_pylist():
                if row['_stream_position'] != expected_position:
                    raise ValidationError('backup positions must be a complete committed prefix')
                expected_position += 1
                event = Activity(**{name:(json.loads(row[name]) if name=='feature_json' else row[name])
                                    for name in columns[:8]})
                if (row['_schema_hash'] != self.catalog.digest or row['_lane'] not in ('sim','dev')
                        or any(not isinstance(row[k],str) or not row[k].strip() for k in ('_source','_actor'))):
                    raise ValidationError('backup provenance or registry mismatch')
                prepared = prepare_row(event,row['_recorded_at'],self.catalog,source=row['_source'],
                                       actor=row['_actor'],lane=row['_lane'])
                if prepared['feature_json'] != row['feature_json']:
                    raise ValidationError('backup payload is not canonical')
        self.init()
        with duckdb.connect(str(self.path)) as conn:
            conn.register('_restore',table.select(columns))
            conn.execute('BEGIN TRANSACTION')
            try:
                conn.execute('INSERT INTO stream.activity SELECT * FROM _restore ORDER BY _stream_position')
                from .binding import check_binding
                check_binding(conn,self.binding)
                conn.execute('COMMIT')
            except BaseException:
                conn.execute('ROLLBACK')
                raise
        return dict(restored=table.num_rows,watermark=expected_position-1)

    def _append(self, events, *, source, actor, lane, batch_size=65536, new_world=False):
        if not isinstance(source, str) or not source.strip() or not isinstance(actor, str) or not actor.strip():
            raise ValidationError("source and actor must be nonempty")
        rows = (prepare_row(event, arrival, self.catalog, source=source, actor=actor, lane=lane)
                for event, arrival in events)
        return self._write(rows, batch_size=batch_size, new_world=new_world)

    def _write(self, rows, *, batch_size=65536, new_world=False, _binding_init=False):
        if not self.path.exists():
            raise ValidationError("stream does not exist; run tl init")
        inserted = attempted = 0
        with duckdb.connect(str(self.path)) as conn:
            conn.execute("SET TimeZone='UTC'")
            conn.execute("SET threads=4")
            conn.execute("BEGIN TRANSACTION")
            try:
                from .binding import check_binding
                check_binding(conn, self.binding, initializing=_binding_init)
                next_id = conn.execute("SELECT coalesce(max(_stream_position), 0) FROM stream.activity").fetchone()[0]
                if new_world and next_id:
                    raise ValidationError("new-world loading requires an empty stream")
                batch = []
                for row in rows:
                    batch.append(row)
                    attempted += 1
                    if len(batch) == batch_size:
                        count = self._insert_batch(conn, batch, next_id, new_world=new_world)
                        inserted += count
                        next_id += count
                        batch = []
                if batch:
                    count = self._insert_batch(conn, batch, next_id, new_world=new_world)
                    inserted += count
                    next_id += count
                check_binding(conn, self.binding)
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        return {"attempted": attempted, "inserted": inserted, "duplicates": attempted - inserted, "watermark": next_id}

    @staticmethod
    def _insert_batch(conn, batch, next_id, *, new_world=False):
        unique = {}
        for row in batch:
            key = row["activity_id"]
            previous = unique.get(key)
            if previous is not None and previous != row:
                raise ValidationError(f"conflicting idempotency key in batch: {key}")
            unique[key] = row
        arrow_schema = pa.schema([("ts", pa.timestamp("us", tz="UTC")), ("activity", pa.string()),
                                 ("customer", pa.string()), ("anonymous_customer_id", pa.string()),
                                 ("feature_json", pa.string()), ("activity_id", pa.string()),
                                 ("revenue_impact", pa.string()), ("link", pa.string()),
                                 ("_recorded_at", pa.timestamp("us", tz="UTC")), ("_source", pa.string()),
                                 ("_actor", pa.string()), ("_lane", pa.string()), ("_schema_hash", pa.string())])
        conn.register("incoming", pa.Table.from_pylist(list(unique.values()), schema=arrow_schema))
        try:
            if new_world:
                # Empty-world generation has unique source IDs by construction.
                # Keep PK enforcement and rollback on any repeated ID; avoid
                # rescanning prior JSON payloads for each fresh batch. Existing
                # streams always take the exact-retry path below.
                return conn.execute("""
                  INSERT INTO stream.activity
                  SELECT activity_id, ts, customer, anonymous_customer_id, activity,
                         feature_json::JSON, revenue_impact::DECIMAL(18,2), link, _recorded_at,
                         ? + row_number() OVER (ORDER BY activity_id), _source, _actor, _lane, _schema_hash
                  FROM incoming
                """, [next_id]).fetchone()[0]
            conflict = conn.execute("""
              SELECT n.activity_id FROM incoming n JOIN stream.activity s USING (activity_id)
              WHERE n.activity IS DISTINCT FROM s.activity OR n.customer IS DISTINCT FROM s.customer
                 OR n.anonymous_customer_id IS DISTINCT FROM s.anonymous_customer_id OR n.ts != s.ts
                 OR n.feature_json IS DISTINCT FROM CAST(s.feature_json AS VARCHAR)
                 OR CAST(n.revenue_impact AS DECIMAL(18,2)) IS DISTINCT FROM s.revenue_impact
                 OR n.link IS DISTINCT FROM s.link OR n._source IS DISTINCT FROM s._source
                 OR n._actor IS DISTINCT FROM s._actor OR n._schema_hash IS DISTINCT FROM s._schema_hash
                 OR n._lane IS DISTINCT FROM s._lane OR (n._lane = 'sim' AND n._recorded_at != s._recorded_at)
              LIMIT 1
            """).fetchone()
            if conflict:
                raise ValidationError(f"conflicting idempotency key: {conflict[0]}")
            result = conn.execute("""
              INSERT INTO stream.activity
              SELECT n.activity_id, n.ts, n.customer, n.anonymous_customer_id, n.activity,
                     n.feature_json::JSON, n.revenue_impact::DECIMAL(18,2), n.link, n._recorded_at,
                     ? + row_number() OVER (ORDER BY n.activity_id), n._source, n._actor, n._lane, n._schema_hash
              FROM incoming n ANTI JOIN stream.activity s USING (activity_id)
            """, [next_id]).fetchone()
            return result[0]
        finally:
            conn.unregister("incoming")


def prepare_row(event, arrival, catalog, *, source, actor, lane):
    payload = catalog.validate(event)
    recorded = timestamp(arrival)
    if recorded < payload["event_ts"]:
        raise ValidationError("recorded time precedes economic event time")
    return {"ts": payload["event_ts"], "activity": event.activity, "customer": event.customer,
            "anonymous_customer_id": event.anonymous_customer_id,
            "feature_json": canonical(payload["feature_json"]), "activity_id": event.activity_id,
            "revenue_impact": payload["revenue_impact"], "link": event.link,
            "_recorded_at": recorded, "_source": source, "_actor": actor, "_lane": lane,
            "_schema_hash": catalog.digest}


class StreamReader:
    def __init__(self, path: Path | str, *, binding=None):
        self.path = Path(path)
        self.binding = binding

    @contextmanager
    def connect(self):
        if not self.path.exists():
            raise ValidationError("stream does not exist; run tl init")
        conn = duckdb.connect(str(self.path), read_only=True)
        try:
            conn.execute("SET TimeZone='UTC'")
            from .binding import check_binding
            check_binding(conn, self.binding)
            yield conn
        finally:
            conn.close()

    def snapshot(self, *, asof: str, known_at: str | None = None, watermark: int | None = None,
                 include_provenance: bool = False, exclude_activity_ids=()):
        """Reporting input: filter knowledge before occurrence/next-event windows.

        The unfiltered activity_stream view is for inspection, not metric input.
        Reporting callers must freeze and retain a watermark for later replay.
        """
        end = datetime.combine(date.fromisoformat(asof) + timedelta(days=1), time(), UTC)
        cutoff = timestamp(known_at) if known_at else end
        predicate = "_recorded_at < ? AND ts < ?"
        params = [cutoff, end]
        # Bounded causal diagnostics only. Filter before temporal windows, as
        # with knowledge cutoffs. Production run_metrics never supplies this.
        if exclude_activity_ids:
            predicate += " AND activity_id NOT IN (SELECT unnest(?))"
            params.append(list(exclude_activity_ids))
        if watermark is not None:
            predicate += " AND _stream_position <= ?"
            params.append(watermark)
        with self.connect() as conn:
            query = logical_sql(predicate)
            if include_provenance:
                query = f"""SELECT q.*, s._recorded_at, s._stream_position, s._source,
                                   s._actor, s._lane, s._schema_hash
                            FROM ({query}) q JOIN stream.activity s USING (activity_id)"""
            return conn.execute(query + " ORDER BY 2, 1", params).fetch_arrow_table()

    def attach_snapshot(self, conn, *, asof, known_at=None, watermark=None):
        """Native, frozen view of this stream; shares the logical window contract.

        Only an attached read-only source and temporary views are created. The
        caller owns the connection. Bound/private streams require their context.
        """
        end = datetime.combine(date.fromisoformat(asof) + timedelta(days=1), time(), UTC)
        cutoff = timestamp(known_at) if known_at else end
        with self.connect() as checked:
            latest = checked.execute('SELECT coalesce(max(_stream_position),0) FROM stream.activity').fetchone()[0]
        watermark = latest if watermark is None else watermark
        if type(watermark) is not int or not 0 <= watermark <= latest:
            raise ValidationError('invalid committed stream watermark')
        path = str(self.path.resolve()).replace("'", "''")
        conn.execute(f"ATTACH '{path}' AS source_db (READ_ONLY)")
        # All literals below have been parsed and normalized, never interpolated
        # from unchecked query text. Pin visibility before temporal derivations.
        predicate = (f"_recorded_at < TIMESTAMPTZ '{iso(cutoff)}' AND "
                     f"ts < TIMESTAMPTZ '{iso(end)}' AND _stream_position <= {watermark}")
        conn.execute('CREATE TEMP VIEW _visible AS SELECT * FROM source_db.stream.activity WHERE '+predicate)
        conn.execute('CREATE TEMP VIEW _snapshot AS '+logical_sql(source='_visible', provenance=True))
        return dict(watermark=watermark, known_at=iso(cutoff), asof=asof)

    def export(self, destination: Path):
        """Canonical event bytes, independent of database page layout or row IDs."""
        import hashlib
        digest = hashlib.sha256()
        count = 0
        with self.connect() as conn, destination.open("xb") as handle:
            cursor = conn.execute("""SELECT activity_id, ts, customer, anonymous_customer_id, activity,
                                     feature_json::VARCHAR, revenue_impact, link, _recorded_at,
                                     _source, _actor, _lane, _schema_hash
                                     FROM stream.activity ORDER BY activity_id""")
            while rows := cursor.fetchmany(8192):
                for key, ts, customer, anonymous, activity, features, revenue, link, recorded, source, actor, lane, schema in rows:
                    line = (canonical({"activity_id": key, "ts": iso(ts), "customer": customer,
                                       "anonymous_customer_id": anonymous, "activity": activity,
                                       "feature_json": json.loads(features),
                                       "revenue_impact": None if revenue is None else format(revenue, ".2f"), "link": link,
                                       "_recorded_at": iso(recorded), "_source": source, "_actor": actor,
                                       "_lane": lane, "_schema_hash": schema}) + "\n").encode("utf-8")
                    handle.write(line)
                    digest.update(line)
                    count += 1
        return {"path": str(destination), "rows": count, "sha256": digest.hexdigest()}
