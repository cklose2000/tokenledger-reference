"""Open-book fixtures and independently recomputed answers, never agent self-grading."""
import json
from pathlib import Path
import uuid

import pyarrow.parquet as pq

from tl.compare.engine import artifacts
from tl.demo.engine import ASOF, CURRENT, ORIGINAL, run as demo
from tl.demo.yield_bridge import calculate as calculate_yield
from tl.metrics.engine import replay_receipts, runtime
from tl.receipts.metrics import (append_receipts, code_revision, digest, normalized,
                                read_receipts, receipt_id)
from tl.stream import ValidationError
from tl.stream.events import canonical
from tl.challenge import close

SCHEMA = 'tokenledger-challenge/v1'
VERSION = 'open-book-reporting/v1'
TASKS = ('july_yield', 'late_invoice', 'nrr_definition', 'failed_close')


def write(path, value):
    Path(path).write_text(canonical(value)+'\n', encoding='utf-8', newline='\n')


def read(path):
    return json.loads(Path(path).read_bytes())


def native_key(root, run, name):
    records = read_receipts(Path(root)/'metrics.jsonl')
    return next(k for k in run['receipt_ids'] if records[k]['schema_version']=='tokenledger-views/v1'
                and records[k]['result']['output']==name)


def roots(root, family):
    base = Path(root)/family
    return dict(db=base/'world.duckdb', output_root=base/'runs', ledger=base/'metrics.jsonl')


def nrr_rows(root, key):
    record = read_receipts(Path(root)/'business/metrics.jsonl')[key]
    table = pq.read_table(Path(root)/'business/runs'/record['run_id']/'consumption_nrr.parquet')
    return {row['lens']: {k:normalized(v) for k,v in row.items() if k not in ('receipt_id','lens')}
            for row in table.to_pylist()}


def derive(root, manifest, *, reperform=True):
    """Goldens come from checked source/receipt populations, never expected.json."""
    root = Path(root); parents = manifest['parents']
    if reperform:
        replay_receipts(parents['business'], **roots(root, 'business'))
        replay_receipts(parents['close'], **roots(root, 'close'))
    from tl.demo.bridge import hashed
    for name, expected_hash in manifest['retained_close_hashes'].items():
        if hashed(root/'close'/name)!=expected_hash:
            raise ValidationError('retained close evidence changed: '+name)
    keys = manifest['keys']
    business = calculate_yield(keys['yield_old'], keys['yield_new'], keys['account_bridge'],
                               output_root=root/'business/runs', ledger=root/'business/metrics.jsonl')
    old, new = [nrr_rows(root, keys[k]) for k in ('nrr_old','nrr_new')]
    records = read_receipts(root/'business/metrics.jsonl')
    for field in ('asof','known_at','watermark','inputs'):
        if records[keys['nrr_old']][field] != records[keys['nrr_new']][field]:
            raise ValidationError('NRR definition challenge must freeze source and cutoffs')
    if records[keys['nrr_old']]['definition_version']!='accounting/v1;nrr/v2' or records[keys['nrr_new']]['definition_version']!='accounting/v1;nrr/v3':
        raise ValidationError('NRR challenge requires released v2 and v3')
    changed = sorted(k for k in old if old[k]!=new[k])
    unchanged = sorted(k for k in old if old[k]==new[k])
    if len(changed)!=1 or len(unchanged)!=3 or old[changed[0]]['cohort_customers']==new[changed[0]]['cohort_customers']:
        raise ValidationError('fixture does not exercise a meaningful isolated NRR floor change')
    frozen = manifest['failed_source']
    failed = close.gate(root/'close', frozen['failed']['watermark'])
    recovered = close.gate(root/'close', frozen['recovered']['watermark'])
    if failed['inputs']!=frozen['failed']['inputs'] or recovered['inputs']!=frozen['recovered']['inputs']:
        raise ValidationError('failed-close frozen source changed')
    if failed['status']!='held' or recovered['status']!='ready':
        raise ValidationError('failed-close recovery no longer reconciles')
    actual_missing = sorted(r['activity_id'] for r in failed['reconciliation']['exceptions'] if r['kind']=='missing_activity')
    if actual_missing!=['cash'] or frozen['retry']['ingestion']['inserted']!=0:
        raise ValidationError('missing source or idempotent recovery proof differs')
    saved = read(root/'close/publication.json')
    if saved['receipt_id']!=keys['close'] or saved['inputs']!=recovered['inputs']:
        raise ValidationError('published close does not use the recovered source')
    def totals(decision):
        return {key:sum(r[key] for r in decision['reconciliation']['periods']) for key in
                ('expected_count','actual_count','expected_cents','actual_cents')}
    return dict(
        july_yield=dict(values=business['original'], units=business['units'],
                       revenue_basis=business['revenue_basis'], denominator=business['denominator'],
                       original_receipt_id=keys['yield_old'], replay_status='reproduced'),
        late_invoice=dict(original=business['original'], current=business['current'], delta=business['delta'],
                          units=business['units'], unchanged=business['unchanged'],
                          responsible_activity_ids=business['responsible_activity_ids'],
                          original_known_at=business['original_known_at'], current_known_at=business['current_known_at'],
                          account_bridge_receipt_id=keys['account_bridge'], yield_bridge_receipt_id=keys['yield_bridge'],
                          attribution_status='isolated_source_append'),
        nrr_definition=dict(original_definition='v2', current_definition='v3', original_floor_usd=10000,
                            current_floor_usd=100000, original=old, current=new,
                            changed_lenses=changed, unchanged_lenses=unchanged,
                            original_receipt_id=keys['nrr_old'], current_receipt_id=keys['nrr_new'],
                            old_replay_status='reproduced', source_and_cutoffs='identical',
                            units='revenue amounts in USD cents; NRR and shares are ratios; cohorts are counts'),
        failed_close=dict(before_status='held', after_status='ready', missing_activity_ids=actual_missing,
                          before=totals(failed), after=totals(recovered),
                          retry_inserted=frozen['retry']['ingestion']['inserted'], retry_publication='already_published',
                          retry_basis='retained writer observation; identical frozen source and publication on retry',
                          units='amounts in USD cents; source observations and inserted activities are counts',
                          publication_receipt_id=keys['close'], source_manifest_sha256=manifest['source_manifest_sha256']))


def prepare(destination):
    from tl.views.engine import profile
    from tl.demo.bridge import hashed
    root = Path(destination).resolve()
    if root.exists() and any(root.iterdir()):
        raise ValidationError('challenge requires a new or empty directory')
    root.mkdir(parents=True, exist_ok=True)
    pins = artifacts(); revision = code_revision(pins)
    result = demo(root/'business')
    business = result['yield_bridge']
    demo_manifest = read(root/'business/demo.json')
    records = read_receipts(root/'business/metrics.jsonl')
    old = next(k for k,r in records.items() if r['run_id']==demo_manifest['original_run_id']
               and r['schema_version']=='tokenledger-views/v1' and r['result']['output']=='consumption_nrr')
    # Use the original source and knowledge cutoff: this changes policy only.
    current = profile(root/'business/world.duckdb', asof=ASOF, known_at=ORIGINAL,
                      watermark=records[old]['watermark'], names=['consumption_nrr'], version='v3',
                      output_root=root/'business/runs', ledger=root/'business/metrics.jsonl')
    new = native_key(root/'business', current, 'consumption_nrr')
    failed_source = close.prepare(root/'close')
    published = read(root/'close/publication.json')
    keys = dict(yield_old=business['original_receipt_id'], yield_new=business['current_receipt_id'],
                yield_bridge=business['receipt_id'], account_bridge=business['account_bridge_receipt_id'],
                nrr_old=old, nrr_new=new, close=published['receipt_id'])
    run_id = uuid.uuid4().hex; run_root = root/'runs'/run_id; run_root.mkdir(parents=True)
    manifest = dict(schema_version=SCHEMA, run_id=run_id, definition_version=VERSION, synthetic=True,
        execution_artifacts=pins, execution_hash=digest(pins), runtime=runtime(), **revision,
        asof=ASOF, known_at=CURRENT, watermark=records[business['current_receipt_id']]['watermark'],
        query_hash=digest(pins), keys=keys, failed_source=failed_source,
        parents=dict(business=[business['receipt_id'],old,new], close=[keys['close']]),
        source_manifest_sha256=hashed(root/'close/capture/manifest.json'),
        retained_close_hashes={name:hashed(root/'close'/name) for name in ('publication.json','failed-close.json')},
        inputs=dict(business_original=records[business['original_receipt_id']]['inputs'],
                    business_current=records[business['current_receipt_id']]['inputs'],
                    close_before=failed_source['failed']['inputs'], close_after=failed_source['recovered']['inputs']),
        scoring=dict(exact='integer cents, token quantities, counts, statuses, units, IDs and nulls',
                     ratio_absolute_tolerance='0.000001', public_expected_outputs=True,
                     held_out_benchmark=False, agent_attribution='unobserved', agent_token_usage=None),
        fixture=dict(seed=42, customers=28, months=30, reporting_date=ASOF,
                     original_known_at=ORIGINAL, current_known_at=CURRENT,
                     late_activity_id='demo:late-july:invoice', missing_source_activity_id='cash'))
    tasks = derive(root, manifest)
    if artifacts()!=pins: raise ValidationError('challenge implementation changed during preparation')
    record = {k:manifest[k] for k in ('schema_version','run_id','definition_version','query_hash','git_sha',
                                     'execution_hash','inputs','asof','known_at','watermark')}
    record['result'] = tasks
    manifest['fixture_id'] = digest(dict(fixture=manifest['fixture'], inputs=manifest['inputs']))
    record['manifest_sha256'] = digest(manifest)
    record['receipt_id'] = receipt_id(record)
    manifest['receipt_id'] = record['receipt_id']
    write(run_root/'run.json', manifest)
    append_receipts(root/'metrics.jsonl', [record])
    expected = dict(schema_version='tokenledger-challenge-answer/v1', fixture_id=manifest['fixture_id'], tasks=tasks)
    write(root/'expected.json', expected)
    write(root/'challenge.json', dict(run_id=run_id, receipt_id=record['receipt_id'],
                                     manifest_sha256=hashed(run_root/'run.json'),
                                     expected_sha256=hashed(root/'expected.json')))
    instructions = ['# Open-book reporting challenge', '',
        'The expected answers are visible. This is a reproduction exercise, not held-out agent evaluation.', '',
        f"Fixture: `{manifest['fixture_id']}`. Code: `{manifest['git_sha']}`.", '',
        f"Proof receipt: `{record['receipt_id']}`.", '',
        'Use the relative paths below from this directory, with `tl` run from the installed release repository.', '',
        '| Task | Required proof |', '|---|---|',
        '| July yield | Sum recognized usage cents and raw tokens; reproduce the original receipt. |',
        '| Late invoice | Exact original/current/delta, the named invoice, unchanged tokens and later cutoff. |',
        '| NRR definition | Released v2 to v3 at identical source/cutoffs; one changed headline and three unchanged lenses. |',
        '| Failed close | Missing source holds publication; captured source recovery and retry do not duplicate rows. |', '',
        'All exact receipt IDs, values, nulls, units and status fields are in `expected.json`. Definitions and cutoffs are in `runs/'+run_id+'/run.json`.', '',
        'Only these two synthetic sandbox databases may be mutated. The late invoice is already appended by preparation; the failed source is already recovered. Frozen watermarks reproduce both earlier states.', '',
        'From the release root:', '', '```text',
        'tl challenge run --directory <new-directory> --json',
        'tl challenge verify <prepared-directory> --json',
        'tl challenge task <prepared-directory> july_yield --json',
        'tl challenge task <prepared-directory> late_invoice --json',
        'tl challenge task <prepared-directory> nrr_definition --json',
        'tl challenge task <prepared-directory> failed_close --json',
        'tl challenge grade <prepared-directory> --answer <your-answer.json> --output <new-score-directory> --json',
        f"tl challenge close <prepared-directory> --watermark {failed_source['failed']['watermark']} --json",
        'tl challenge close <prepared-directory> --recover --json', '```', '',
        'The historical close command exits 2 and leaves the accepted publication untouched. `grade` returns nonzero for incorrect or incomplete answers and replays evidence before scoring. A supplied PASS is ignored.', '',
        'The scorecard records actual grader elapsed time separately from deterministic correctness. Agent identity and tokens remain unobserved unless a separate measured harness supplies them.']
    (root/'TASKS.md').write_text('\n'.join(instructions)+'\n', encoding='utf-8', newline='\n')
    return dict(status='prepared', path=str(root), fixture_id=manifest['fixture_id'],
                receipt_id=record['receipt_id'], manifest_sha256=hashed(run_root/'run.json'))


def load(root):
    from tl.demo.bridge import hashed
    root = Path(root); pointer = read(root/'challenge.json')
    run_id = pointer['run_id']
    if not isinstance(run_id,str) or len(run_id)!=32 or any(c not in '0123456789abcdef' for c in run_id):
        raise ValidationError('invalid challenge run identity')
    path = root/'runs'/run_id/'run.json'
    if hashed(path)!=pointer['manifest_sha256'] or hashed(root/'expected.json')!=pointer['expected_sha256']:
        raise ValidationError('challenge manifest or expected answers changed')
    manifest = read(path); record = read_receipts(root/'metrics.jsonl')[pointer['receipt_id']]
    if manifest['schema_version']!=SCHEMA or manifest['definition_version']!=VERSION:
        raise ValidationError('unsupported challenge contract')
    for field in ('run_id','receipt_id','definition_version','query_hash','git_sha','execution_hash','inputs','asof','known_at','watermark'):
        if manifest[field]!=record[field]: raise ValidationError('challenge receipt contract differs')
    if digest({k:v for k,v in manifest.items() if k!='receipt_id'})!=record['manifest_sha256']:
        raise ValidationError('challenge manifest differs from its receipt')
    if digest(manifest['execution_artifacts'])!=manifest['execution_hash']:
        raise ValidationError('challenge implementation hash differs')
    if hashed(root/'close/capture/manifest.json')!=manifest['source_manifest_sha256']:
        raise ValidationError('challenge source capture changed')
    return manifest, record


def verify(root):
    manifest, record = load(root)
    verified = replay([record['receipt_id']], output_root=Path(root)/'runs', ledger=Path(root)/'metrics.jsonl')
    return dict(status='verified', fixture_id=manifest['fixture_id'], receipt_id=record['receipt_id'],
                recomputed=verified[0]['recomputed'])


def replay(ids, *, output_root, ledger, db=None, python=None):
    root = Path(output_root).parent
    manifest, record = load(root)
    if ids != [record['receipt_id']]: raise ValidationError('challenge receipt is not the selected fixture')
    if python or artifacts()!=manifest['execution_artifacts'] or runtime()!=manifest['runtime']:
        from tl.receipts.archive import historical_replay
        # Historical runner keeps output_root and the relative two-source layout.
        return historical_replay(ids, manifest=manifest, db=db or root/'business/world.duckdb',
                                 output_root=output_root, ledger=ledger, python=python)
    actual = derive(root, manifest)
    if actual!=record['result'] or read(root/'expected.json')!=dict(
            schema_version='tokenledger-challenge-answer/v1', fixture_id=manifest['fixture_id'], tasks=actual):
        raise ValidationError('challenge answers differ from replayed evidence')
    return [dict(receipt_id=record['receipt_id'], verified=True, recomputed=True, result=actual)]
