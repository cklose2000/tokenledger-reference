"""Cross-engine logical JSON encoding, separate from historical DuckDB hashes."""
from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
import math

from tl.stream import ValidationError
from tl.stream.events import canonical, iso


def encode(value):
    # JSON numbers have one logical numeric domain; quoted prices stay strings.
    if value is None or isinstance(value, (str, bool)): return value
    if isinstance(value, datetime): return {'timestamp':iso(value)}
    if isinstance(value, date): return {'date':value.isoformat()}
    if isinstance(value, (int,float,Decimal)):
        number=Decimal(str(value))
        if not number.is_finite(): raise ValidationError('nonfinite cloud source or result')
        text=format(number,'f').rstrip('0').rstrip('.') if '.' in format(number,'f') else format(number,'f')
        return {'number':'0' if number==0 else text}
    if isinstance(value, (list,tuple)): return [encode(item) for item in value]
    if isinstance(value, dict): return {key:encode(item) for key,item in value.items()}
    raise ValidationError('unsupported cross-engine evidence type: '+type(value).__name__)


def fingerprint(table, keys, *, source=False):
    rows=[];ids=set();low=high=first=last=None
    for batch in table.to_batches(max_chunksize=8192):
        for row in batch.to_pylist():
            if source:
                row['feature_json']=json.loads(row['feature_json']) if isinstance(row['feature_json'],str) else row['feature_json']
                if row['_lane']!='sim': raise ValidationError('BigQuery scratch accepts only synthetic source rows')
                key=row['activity_id'];position=row['_stream_position']
                if key in ids: raise ValidationError('duplicate cloud source activity')
                ids.add(key);low=position if low is None else min(low,position);high=position if high is None else max(high,position)
                first=key if first is None else min(first,key);last=key if last is None else max(last,key)
            order=canonical({key:encode(row[key]) for key in keys})
            rows.append((order,hashlib.sha256(canonical(encode(row)).encode()).hexdigest()))
    whole=hashlib.sha256()
    for _,sha in sorted(rows): whole.update(sha.encode('ascii'))
    result=dict(algorithm='cross-engine-logical-json/v1',sha256=whole.hexdigest(),rows=len(rows),columns=sorted(table.column_names))
    if source: result.update(activity_id_min=first,activity_id_max=last,input_row_range=[low,high],activity_count=len(rows))
    return result
