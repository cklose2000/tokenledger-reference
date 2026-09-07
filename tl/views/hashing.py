"""Versioned columnar fingerprints; historical JSONL fingerprints stay unchanged.

Each row is serialized and SHA-256 hashed in DuckDB. Python streams only ordered
fixed-width hexadecimal digests into the population hash. Replay scans the
source again; there is no unchecked cross-run fingerprint cache.
"""
import hashlib


def identifier(name):
    return '"'+name.replace('"','""')+'"'


def fingerprint(conn, query, keys):
    fields=conn.execute('DESCRIBE '+query).fetchall()
    columns=sorted((row[0],row[1]) for row in fields)
    parts=[]
    for name,kind in columns:
        q=identifier(name)
        if kind in ('DOUBLE','FLOAT','REAL'):
            value=f'CAST(CAST({q} AS DECIMAL(38,12)) AS VARCHAR)'
        elif kind=='TIMESTAMP WITH TIME ZONE':
            value=f"strftime({q},'%Y-%m-%dT%H:%M:%S.%fZ')"
        elif kind=='JSON':
            value=f'CAST({q} AS VARCHAR)'
        else:
            value=q
        parts.append(q+' := '+value)
    order=','.join(identifier(k)+' ASC NULLS FIRST' for k in keys)
    sql='SELECT sha256(to_json(struct_pack('+','.join(parts)+'))) AS row_sha FROM ('+query+') input ORDER BY '+order+',row_sha'
    cursor=conn.execute(sql);digest=hashlib.sha256();count=0
    while rows:=cursor.fetchmany(16384):
        digest.update(''.join(row[0] for row in rows).encode('ascii'));count+=len(rows)
    return dict(algorithm='duckdb-ordered-row-sha256/v1',sha256=digest.hexdigest(),rows=count,
                columns=[name for name,_ in columns],float_encoding='decimal(38,12) string; nonfinite values rejected')


def input_manifest(session):
    result=fingerprint(session.conn,'SELECT * FROM _visible',['activity_id'])
    low,high,first,last=session.conn.execute('SELECT min(_stream_position),max(_stream_position),min(activity_id),max(activity_id) FROM _visible').fetchone()
    return dict(**result,activity_count=result['rows'],input_row_range=[low,high],activity_id_min=first,activity_id_max=last,
                scope='complete fourteen-column knowledge-filtered physical stream; temporal derivation and query hashes pinned separately')


def table_fingerprint(conn, table, keys):
    conn.register('_fingerprint_input',table)
    try: return fingerprint(conn,'SELECT * FROM _fingerprint_input',keys)
    finally: conn.unregister('_fingerprint_input')
