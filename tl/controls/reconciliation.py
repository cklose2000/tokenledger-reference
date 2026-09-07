"""Distinct recognized entries bridged to a separately captured synthetic ERP file."""
from collections import defaultdict
from decimal import Decimal
import csv
import hashlib
import io
import json
from pathlib import Path
import yaml

from tl.scaffolds.invariants import verify
from tl.stream import ValidationError

POLICY=Path('definitions/controls/v1.yaml')


def cents(value):
    number=Decimal(value)*100
    if not number.is_finite() or number!=number.to_integral_value():
        raise ValidationError('ERP amount must be exact cents')
    return int(number)


def bridge(session,raw,manifest,*,start,end):
    policy=yaml.safe_load(POLICY.read_bytes())
    if (manifest.get('schema_version')!='tokenledger-synthetic-erp/v1' or manifest.get('synthetic') is not True
            or manifest.get('presentation')!='gross_revenue_separate_fees'
            or manifest.get('sha256')!=hashlib.sha256(raw).hexdigest()):
        raise ValidationError('ERP origin, presentation or content hash mismatch')
    errors=[]
    if manifest['coverage_start']>start or manifest['coverage_end']<end:
        errors.append(dict(kind='partial_erp_coverage'))
    verify(session)
    expected={}
    reader=csv.DictReader(io.StringIO(raw.decode('utf-8')))
    if reader.fieldnames!=['period','account','source','amount_usd']:
        raise ValidationError('unexpected ERP columns')
    for row in reader:
        if not start<=row['period']<end:
            continue
        key=(row['period'],row['account'],row['source'])
        if key in expected:
            raise ValidationError('duplicate ERP period/account/source')
        expected[key]=cents(row['amount_usd'])
    actual=defaultdict(lambda:dict(net_cents=0,fee_addback_cents=0,activity_ids=[]))
    invoices={}
    for row in session.snapshot.to_pylist():
        if row['activity']=='usage_invoiced':
            f=json.loads(row['feature_json'])
            invoices[(row['_source'],f['invoice_id'])]=(row['activity_id'],cents(f['channel_fee_usd']))
    recognized=session.rows('SELECT * FROM report.revenue_ledger ORDER BY activity_id')
    invoice_periods=defaultdict(set)
    for row in recognized:
        if row['family']=='consumption':
            invoice_periods[(row['_source'],row['source_document'])].add(row['month'])
    for invoice,periods in invoice_periods.items():
        if len(periods)>1 and invoices[invoice][1]:
            raise ValidationError('multi-period invoice fees require a versioned allocation policy; activity_id: '+invoices[invoice][0])
    seen=set()
    for row in recognized:
        period=row['month'].isoformat()
        if not start<=period<end:
            continue
        key=(period,policy['accounts'][row['family']],row['_source'])
        total=actual[key];total['net_cents']+=row['revenue_cents'];total['activity_ids'].append(row['activity_id'])
        invoice=(row['_source'],row['source_document'])
        if row['family']=='consumption' and invoice not in seen:
            activity_id,fee=invoices[invoice]
            total['fee_addback_cents']+=fee;total['activity_ids'].append(activity_id);seen.add(invoice)
    rows=[]
    for key in sorted(expected.keys()|actual.keys()):
        present=key in actual
        values=actual[key]
        delta=values['net_cents']+values['fee_addback_cents']-expected.get(key,0)
        matched=key in expected and present and delta==0
        if not matched:
            errors.append(dict(kind='revenue_variance',period=key[0],account=key[1],source=key[2],delta_cents=delta,
                               activity_ids=values['activity_ids']))
        rows.append(dict(period=key[0],account=key[1],source=key[2],**values,erp_cents=expected.get(key),delta_cents=delta))
    if not rows:
        errors.append(dict(kind='no_erp_or_recognition_population'))
    return dict(passed=not errors,policy_version=policy['version'],presentation=policy['revenue_presentation'],
                rows=rows,exceptions=errors)
