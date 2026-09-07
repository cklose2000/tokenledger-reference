"""Independent spine recognition from economic documents, in integer cents.

No generated recognition amount, invoice funding assertion, or expiry balance
is used in the calculation. Derived rows are disposable analytical outputs.
"""
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal
import json

import pyarrow as pa

from tl.generate.synthetic import month_add
from tl.stream.events import ValidationError


def cents(value):
    amount = Decimal(str(value)) * 100
    if amount != int(amount):
        raise ValidationError('accounting input must be exact cents')
    return int(amount)


def half_up(n, d):
    if d <= 0:
        raise ValidationError('nonpositive service interval')
    return (abs(n) * 2 + d) // (2 * d) * (1 if n >= 0 else -1)


def periods(start, end):
    current = start.replace(day=1)
    while current < end:
        stop = month_add(current, 1)
        yield current, max(start, current), min(end, stop), (stop-current).days
        current = stop


def spread(amount, start, end):
    """Cumulative entitlement avoids a rounding residual on the last period."""
    total = (end-start).days
    return [(month, half_up(amount*(hi-start).days,total)-half_up(amount*(lo-start).days,total))
            for month,lo,hi,_ in periods(start,end)]


RECOGNITION_SCHEMA = pa.schema([
    ('activity_id',pa.string()),('_source',pa.string()),('customer',pa.string()),
    ('month',pa.date32()),('source',pa.string()),('recognition_kind',pa.string()),
    ('source_document',pa.string()),('channel',pa.string()),('revenue_cents',pa.int64()),
    ('upstream_ids',pa.list_(pa.string()))])
JOURNAL_SCHEMA = pa.schema([
    ('activity_id',pa.string()),('_source',pa.string()),('customer',pa.string()),
    ('month',pa.date32()),('account',pa.string()),('amount_cents',pa.int64()),
    ('upstream_ids',pa.list_(pa.string()))])


def recognize(snapshot, *, period_end):
    groups=defaultdict(list)
    for batch in snapshot.to_batches(max_chunksize=8192):
        for row in batch.to_pylist():
            if row['activity'] in ('revenue_recognized','tokens_processed','capacity_consumed'):
                continue
            row['f']=json.loads(row['feature_json'])
            groups[row['activity']].append(row)
    invoices={}
    # Iterate invoices separately without retaining the full token world in Python.
    for row in groups['usage_invoiced']:
        key=(row['_source'],row['f']['invoice_id'])
        if key in invoices:
            raise ValidationError('duplicate invoice document identity')
        invoices[key]=row
    contracts={}
    for row in groups['contract_signed']+groups['contract_renewed']:
        key=(row['_source'],row['f']['contract_id'])
        if key in contracts:
            raise ValidationError('duplicate contract document identity')
        contracts[key]=row
    for kind,field in [('credit_issued','credit_id'),('contract_expired','contract_id')]:
        identities=[(r['_source'],r['f'][field]) for r in groups[kind]]
        if len(identities)!=len(set(identities)):
            raise ValidationError('duplicate '+kind+' document identity')
    recognized=[]
    journal=[]
    def post(row,month,account,amount,ids=None):
        if amount:
            journal.append(dict(activity_id=row['activity_id'],_source=row['_source'],customer=row['customer'],
                month=month,account=account,amount_cents=amount,upstream_ids=sorted(ids or [row['activity_id']])))
    def earn(row,month,source,kind,document,channel,amount,ids=None):
        if amount and month<period_end:
            recognized.append(dict(activity_id=row['activity_id']+':'+source+':'+kind+':'+month.isoformat(),
                _source=row['_source'],customer=row['customer'],month=month,source=source,
                recognition_kind=kind,source_document=document,channel=channel,revenue_cents=amount,
                upstream_ids=sorted(ids or [row['activity_id']])))
    remaining={key:cents(row['f']['commit_usd']) for key,row in contracts.items()}
    for key,row in contracts.items():
        month=row['ts'].date().replace(day=1)
        post(row,month,'1100',remaining[key]);post(row,month,'2100',-remaining[key])
    ordered=sorted(invoices.values(),key=lambda r:(r['f']['period_end'],r['activity_id']))
    last_service_by_contract={}
    for row in ordered:
        f=row['f'];start=date.fromisoformat(f['period_start']);end=date.fromisoformat(f['period_end'])
        if end<=start:
            raise ValidationError('nonpositive invoice service interval')
        net,fee=cents(f['net_usd']),cents(f['channel_fee_usd'])
        if net<0 or not 0<=fee<=net or cents(f['gross_usd'])-cents(f['discount_usd'])!=net:
            raise ValidationError('invoice gross, discount, net or fee mismatch')
        contract=(row['_source'],f.get('contract_id'))
        if f.get('contract_id') and contract not in contracts:
            raise ValidationError('invoice has no visible contract')
        if contract in contracts:
            terms=contracts[contract]['f']
            if start<date.fromisoformat(terms['start']) or end>date.fromisoformat(terms['end']):
                raise ValidationError('invoice service outside contracted term')
            last_service_by_contract[contract]=max(end,last_service_by_contract.get(contract,end))
        draw=min(remaining.get(contract,0),net)
        if draw and fee:
            raise ValidationError('fee-bearing prepaid marketplace commitments need a separate policy')
        if contract in remaining:
            remaining[contract]-=draw
        ids=[row['activity_id']]+([contracts[contract]['activity_id']] if contract in contracts else [])
        amounts={name:dict(spread(value,start,end)) for name,value in [('net',net),('fee',fee),('draw',draw)]}
        for month,n in amounts['net'].items():
            if month>=period_end:
                continue
            d=amounts['draw'][month];cost=amounts['fee'][month]
            post(row,month,'2100',d,ids);post(row,month,'1100',n-d,ids)
            post(row,month,'4000',-n,ids);post(row,month,'4095',cost,ids)
            # Net fee settlement reduces the receivable, without changing usage quantity.
            post(row,month,'1100',-cost,ids)
            earn(row,month,'commit_drawdown','usage',f['invoice_id'],f['channel'],d,ids)
            earn(row,month,'usage','usage',f['invoice_id'],f['channel'],n-cost-d,ids)
    for row in groups['contract_expired']:
        key=(row['_source'],row['f']['contract_id'])
        if key not in contracts:
            raise ValidationError('expiry has no visible contract')
        if last_service_by_contract.get(key,row['ts'].date())>row['ts'].date():
            raise ValidationError('invoice service after explicit contract expiry')
        amount=remaining[key];remaining[key]=0;month=row['ts'].date().replace(day=1)
        ids=[contracts[key]['activity_id'],row['activity_id']]
        post(row,month,'2100',amount,ids);post(row,month,'4020',-amount,ids)
        earn(row,month,'commit_drawdown','commit_expiry',key[1],contracts[key]['f']['channel'],amount,ids)
    for row in groups['credit_issued']:
        f=row['f'];invoice=invoices.get((row['_source'],f['invoice_id']))
        if invoice is None:
            raise ValidationError('credit has no visible original invoice')
        amount=cents(f['amount_usd']);month=date.fromisoformat(f['period'])
        ids=[row['activity_id'],invoice['activity_id']]
        post(row,month,'4090',amount,ids);post(row,month,'1100',-amount,ids)
        earn(row,month,'usage','credit',f['credit_id'],invoice['f']['channel'],-amount,ids)
    subscriptions=defaultdict(list)
    priority={'subscription_upgraded':10,'subscription_downgraded':10,'seat_added':20,'seat_removed':20,
              'subscription_cancelled':30}
    for kind in ('subscription_started','subscription_renewed',*priority):
        for row in groups[kind]:
            subscriptions[(row['_source'],row['customer'],row['f']['subscription_id'])].append(row)
    for key,rows in subscriptions.items():
        rows.sort(key=lambda r:(r['ts'],priority.get(r['activity'],0),r['activity_id']))
        month_amounts=defaultdict(int);month_ids=defaultdict(set);anchors={}
        price=0;service_end=period_end
        for index,row in enumerate(rows):
            f=row['f'];kind=row['activity'];start=row['ts'].date()
            if f.get('billing_period','monthly')!='monthly':
                raise ValidationError('only monthly subscription price basis is supported')
            if 'price_per_seat' in f:
                price=cents(f['price_per_seat'])
            if 'period_end' in f:
                service_end=date.fromisoformat(f['period_end'])
            seats=0 if kind=='subscription_cancelled' else f.get('seats_after',f['seats'])
            stop=min(service_end,period_end,rows[index+1]['ts'].date() if index+1<len(rows) else period_end)
            for month,lo,hi,days in periods(start,stop):
                month_amounts[month]+=price*seats*(hi-lo).days
                month_ids[month].add(row['activity_id']);anchors.setdefault(month,row)
        for month,numerator in month_amounts.items():
            amount=half_up(numerator,(month_add(month,1)-month).days)
            row=anchors[month];ids=sorted(month_ids[month])
            # Stable subscription/month business key on both implementations.
            row={**row,'activity_id':'subscription:'+key[2]+':'+month.isoformat()}
            post(row,month,'1100',amount,ids);post(row,month,'4010',-amount,ids)
            earn(row,month,'subscription','subscription',key[2],'direct',amount,ids)
    # An invoice can post two receivable movements: merge before output identity.
    totals=defaultdict(int);line_ids=defaultdict(set)
    for row in journal:
        key=tuple(row[k] for k in ('activity_id','_source','customer','month','account'))
        totals[key]+=row['amount_cents'];line_ids[key].update(row['upstream_ids'])
    journal=[dict(zip(('activity_id','_source','customer','month','account'),key),
                  amount_cents=value,upstream_ids=sorted(line_ids[key])) for key,value in totals.items() if value]
    return pa.Table.from_pylist(recognized,schema=RECOGNITION_SCHEMA),pa.Table.from_pylist(journal,schema=JOURNAL_SCHEMA)
