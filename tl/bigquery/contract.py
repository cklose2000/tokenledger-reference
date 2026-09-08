"""Exact source metadata contract, independent of SDK/REST representation."""
import re

from tl.bigquery.schema import FIELDS
from tl.stream import ValidationError


def _field(raw):
    result=dict(raw)
    # REST reports INTEGER; GoogleSQL and SchemaField constructors accept INT64.
    # Do not widen to FLOAT, STRING, BIGNUMERIC or another storage type.
    result['type']={'INTEGER':'INT64'}.get(result.get('type'),result.get('type'))
    result.setdefault('mode','NULLABLE')  # The documented REST default.
    for key in ('precision','scale'):
        if key not in result: continue
        value=result[key]
        if type(value) is int:
            continue
        if isinstance(value,str) and re.fullmatch(r'0|[1-9][0-9]*',value):
            result[key]=int(value)
        else:
            raise ValidationError('stream numeric metadata is not an integer: '+key)
    return result


def validate_source_table(table):
    """Validate metadata only. Never update schema, retention or IAM policy."""
    raw=table.to_api_repr()
    partition=raw.get('timePartitioning') or {}
    # The REST wrapper distinguishes absent expiry from a configured value.
    # Zero is invalid for partition expiry, so it is not accepted as no expiry.
    if raw.get('expirationTime') is not None:
        raise ValidationError('stream table expiration is configured; no table alteration attempted')
    if partition.get('expirationMs') is not None:
        raise ValidationError('stream partition expiration is configured; no table alteration attempted')
    expected=[]
    for name,kind,nullable in FIELDS:
        field=dict(name=name,type='NUMERIC' if kind.startswith('NUMERIC') else kind,
                   mode='NULLABLE' if nullable else 'REQUIRED')
        if kind.startswith('NUMERIC'): field.update(precision=18,scale=2)
        expected.append(field)
    actual=[_field(field) for field in raw.get('schema',{}).get('fields',[])]
    if (actual!=expected or partition.get('field')!='ts' or partition.get('type')!='DAY'
            or raw.get('clustering',{}).get('fields')!=['customer','activity']
            or raw.get('tableConstraints') is not None or raw.get('rangePartitioning') is not None):
        raise ValidationError('existing stream physical contract differs; no table alteration attempted')
