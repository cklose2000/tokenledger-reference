"""Serialized synthetic replication through the shared validator and append API.

This laptop owns one transport state per binding. Cloud IAM and distributed
writer exclusion remain separate native acceptance gates, never inferred here.
"""
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import json
import os

import pyarrow as pa

from tl.bigquery.client import Client
from tl.bigquery.hashing import fingerprint
from tl.bigquery.schema import FIELDS
from tl.receipts.journal import ledger_lock
from tl.stream import Activity, StreamReader, ValidationError
from tl.stream.events import Catalog, canonical
from tl.stream.store import prepare_row


def validated_prefix(db):
    catalog=Catalog(Path('definitions/activities'))
    with StreamReader(db).connect() as conn:
        table=conn.execute('SELECT * FROM stream.activity ORDER BY _stream_position').to_arrow_table()
    if table.column_names!=[field[0] for field in FIELDS]: raise ValidationError('physical stream schema differs')
    position=0;ids=set()
    for batch in table.to_batches(max_chunksize=8192):
        for row in batch.to_pylist():
            position+=1
            if row['_stream_position']!=position or row['activity_id'] in ids:
                raise ValidationError('source is not a unique committed prefix')
            ids.add(row['activity_id'])
            if row['_lane']!='sim' or row['_schema_hash']!=catalog.digest:
                raise ValidationError('cloud scratch accepts only current-registry synthetic activities')
            event=Activity(**{key:(json.loads(row[key]) if key=='feature_json' else row[key]) for key,_,_ in FIELDS[:8]})
            expected=prepare_row(event,row['_recorded_at'],catalog,source=row['_source'],actor=row['_actor'],lane='sim')
            for key,value in expected.items():
                if key=='revenue_impact' and value is not None: value=Decimal(value)
                if row[key]!=value: raise ValidationError('noncanonical source row: '+key)
    return table


def proto_contract():
    from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
    descriptor=descriptor_pb2.DescriptorProto(name='Activity')
    for index,(name,kind,nullable) in enumerate(FIELDS,1):
        field=descriptor.field.add(name=name,number=index,label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL)
        field.type=(descriptor_pb2.FieldDescriptorProto.TYPE_INT64 if kind in ('TIMESTAMP','INT64')
                    else descriptor_pb2.FieldDescriptorProto.TYPE_STRING)
    file=descriptor_pb2.FileDescriptorProto(name='tokenledger_activity.proto',syntax='proto2')
    file.message_type.add().CopyFrom(descriptor)
    pool=descriptor_pool.DescriptorPool();pool.Add(file)
    return descriptor,message_factory.GetMessageClass(pool.FindMessageTypeByName('Activity'))


def serialized_rows(table, message_type):
    epoch=datetime(1970,1,1,tzinfo=timezone.utc)
    for row in table.to_pylist():
        result={}
        for name,value in row.items():
            if value is None: continue
            if isinstance(value,datetime):
                delta=value-epoch;value=(delta.days*86400+delta.seconds)*1000000+delta.microseconds
            elif isinstance(value,Decimal): value=format(value,'f')
            result[name]=value
        yield message_type(**result).SerializeToString()


def _save(path,state):
    temporary=Path(str(path)+'.tmp')
    with temporary.open('w',encoding='utf-8',newline='\n') as handle:
        handle.write(canonical(state)+'\n');handle.flush();os.fsync(handle.fileno())
    os.replace(temporary,path)


def append(binding, db, state_path, *, client=None, storage=None):
    from google.cloud import bigquery_storage_v1
    from google.cloud.bigquery_storage_v1 import types, writer
    table=validated_prefix(db)
    client=client or Client(binding)
    job_start=len(client.jobs)
    state_path=Path(state_path)
    with ledger_lock(state_path):
        existing,_=client.query(f'SELECT * FROM `{binding.table}` ORDER BY _stream_position',label='append_prefix')
        # Compare normalized JSON and exact cents, never merely the latest ID.
        if len(existing)>len(table): raise ValidationError('cloud source is ahead of the submitted local stream')
        cloud=fingerprint(existing,['activity_id'],source=True)
        if cloud!=fingerprint(table.slice(0,len(existing)),['activity_id'],source=True):
            raise ValidationError('cloud source conflicts with the validated local prefix')
        if len(existing)==len(table):
            return dict(status='already_present',inserted=0,watermark=len(table),inputs=cloud,
                        binding_identity=binding.identity,jobs=client.jobs[job_start:],iam_negative_tests='not_run',authority_mode=binding.authority_mode)
        from tl.bigquery.auth import credentials
        storage=storage or bigquery_storage_v1.BigQueryWriteClient(credentials=credentials(binding,'writer'))
        target=f'projects/{binding.project}/datasets/{binding.dataset}/tables/activity'
        if state_path.exists():
            state=json.loads(state_path.read_text(encoding='utf-8'))
            if state['binding_identity']!=binding.identity or not state['stream'].startswith(target+'/streams/'):
                raise ValidationError('append state belongs to another binding')
            base=state['base_position']
            if len(existing)<base: raise ValidationError('cloud committed prefix disappeared')
        else:
            stream=storage.create_write_stream(parent=target,write_stream=types.WriteStream(type_=types.WriteStream.Type.COMMITTED))
            base=len(existing)
            state=dict(schema_version='tokenledger-bigquery-append/v1',binding_identity=binding.identity,
                       stream=stream.name,base_position=base)
            _save(state_path,state)
        descriptor,message_type=proto_contract()
        template=types.AppendRowsRequest(write_stream=state['stream'],
                    proto_rows=types.AppendRowsRequest.ProtoData(writer_schema=types.ProtoSchema(proto_descriptor=descriptor)))
        stream=writer.AppendRowsStream(storage,template)
        added=0
        try:
            # Offsets are per application-created stream. Retrying from the exact
            # cloud prefix skips accepted chunks without resubmitting duplicates.
            for start in range(len(existing),len(table),4096):
                rows=list(serialized_rows(table.slice(start,4096),message_type))
                if sum(map(len,rows))>18_000_000: raise ValidationError('append request exceeds bounded transport size')
                request=types.AppendRowsRequest(offset=start-base,
                    proto_rows=types.AppendRowsRequest.ProtoData(rows=types.ProtoRows(serialized_rows=rows)))
                response=stream.send(request).result(timeout=120)
                if response.error.code: raise ValidationError('Storage Write API rejected append: '+response.error.message)
                if response.append_result.offset!=start-base: raise ValidationError('Storage Write offset differs')
                added+=len(rows)
        finally: stream.close()
        actual,_=client.query(f'SELECT * FROM `{binding.table}` ORDER BY _stream_position',label='append_recheck')
        expected=fingerprint(table,['activity_id'],source=True)
        if fingerprint(actual,['activity_id'],source=True)!=expected:
            raise ValidationError('cloud append did not reproduce the validated source')
        return dict(status='appended',inserted=added,watermark=len(table),inputs=expected,
                    binding_identity=binding.identity,jobs=client.jobs[job_start:],iam_negative_tests='not_run',authority_mode=binding.authority_mode)
