"""Deterministic structure/value scoring after fresh re-performance of all goldens."""
from decimal import Decimal, InvalidOperation
from pathlib import Path
from time import perf_counter

from tl.challenge.engine import TASKS, load, read, verify, write
from tl.receipts.metrics import append_receipts, digest, read_receipts, receipt_id
from tl.receipts.journal import ledger_lock
from tl.stream import ValidationError

RATIOS = {'nrr','expansion_share','contraction_share','churn_share','cohort_revenue_coverage',
          'net_of_discounts_and_fees_per_mtok','net_of_discounts_per_mtok'}


def differences(expected, actual, path=''):
    if isinstance(expected, dict):
        if not isinstance(actual,dict): return [path+': expected object']
        errors = [path+': missing '+k for k in expected.keys()-actual.keys()]
        errors += [path+': unexpected '+k for k in actual.keys()-expected.keys()]
        for key in sorted(expected.keys() & actual.keys()):
            errors.extend(differences(expected[key], actual[key], path+'.'+key))
        return sorted(errors)
    if isinstance(expected,list):
        return [] if actual==expected else [path+': list differs']
    if expected is None:
        return [] if actual is None else [path+': expected explicit null']
    if path.split('.')[-1] in RATIOS and '.units.' not in path:
        try:
            if isinstance(actual,bool): raise InvalidOperation
            value = Decimal(str(actual)); target = Decimal(str(expected))
            if value.is_finite() and abs(value-target)<=Decimal('0.000001'): return []
        except (InvalidOperation,ValueError): pass
        return [path+': ratio exceeds absolute tolerance 0.000001']
    if type(actual) is not type(expected) or actual!=expected:
        return [path+': exact value or type differs']
    return []


def score(expected, answer):
    global_errors = []
    if not isinstance(answer,dict): answer = {}; global_errors.append('answer must be an object')
    for key in ('schema_version','fixture_id'):
        if answer.get(key)!=expected[key]: global_errors.append(key+' differs')
    if set(answer)-{'schema_version','fixture_id','tasks'}: global_errors.append('unexpected answer fields')
    supplied = answer.get('tasks',{})
    if not isinstance(supplied,dict): supplied={}; global_errors.append('tasks must be an object')
    if set(supplied)-set(TASKS): global_errors.append('unexpected task')
    tasks = {}
    for task in TASKS:
        errors = differences(expected['tasks'][task], supplied.get(task), task)
        tasks[task] = dict(status='fail' if errors else 'pass', differences=errors)
    return dict(status='fail' if global_errors or any(v['status']=='fail' for v in tasks.values()) else 'pass',
                tasks=tasks, differences=global_errors)


def make_card(manifest,proof,answer):
    result=score(dict(schema_version='tokenledger-challenge-answer/v1',fixture_id=manifest['fixture_id'],tasks=proof['result']),answer)
    expected=dict(tasks=proof['result'])
    card = dict(schema_version='tokenledger-challenge-score/v1', fixture_id=manifest['fixture_id'],
                git_sha=manifest['git_sha'], proof_receipt_id=proof['receipt_id'],
                answer_sha256=digest(answer), replay_status='reproduced', **result,
                values={key:expected['tasks'][key] for key in TASKS},
                scope='open-book reproduction; no held-out accuracy or agent efficiency claim',
                agent='unobserved', model='unobserved', token_usage=None)
    return card


def grade(directory, answer_path, destination):
    started = perf_counter(); root = Path(directory); destination = Path(destination)
    if destination.exists(): raise ValidationError('score destination must be new; prior scorecards are retained')
    if Path(answer_path).stat().st_size>4*1024*1024: raise ValidationError('answer exceeds the bounded challenge size')
    answer = read(answer_path)
    verify(root)
    manifest, proof = load(root)
    from tl.compare.engine import artifacts
    from tl.metrics.engine import runtime
    if artifacts()!=manifest['execution_artifacts'] or runtime()!=manifest['runtime']:
        raise ValidationError('grade from the fixture original executable checkout and runtime; replay remains available')
    # The scorer's statement is content-addressed and reperformable. Elapsed time
    # is separately retained as an observation, not a repeatable business metric.
    card = make_card(manifest,proof,answer)
    record = {k:proof[k] for k in ('run_id','definition_version','query_hash','git_sha','execution_hash','inputs',
                                  'asof','known_at','watermark')}
    record['run_id']=digest(dict(proof=proof['receipt_id'],answer=answer))[:32]
    record.update(schema_version='tokenledger-challenge-score/v1', result=card, answer=answer)
    record['receipt_id'] = receipt_id(record)
    observation = dict(schema_version='tokenledger-challenge-score-observation/v1',
                       result=dict(elapsed_seconds=perf_counter()-started, scope='grader process function, including all evidence re-performance'),
                       **{k:record[k] for k in ('run_id','definition_version','query_hash','git_sha','execution_hash','inputs','asof','known_at','watermark')})
    observation['receipt_id'] = receipt_id(observation)
    # Equal answers have one deterministic correctness receipt. Each invocation
    # can still retain its own elapsed observation and render to a new directory.
    # Serialize graders without weakening the shared ledger's duplicate-ID guard.
    with ledger_lock(root/'score-retention'):
        retained=read_receipts(root/'metrics.jsonl'); pending=[]
        for item in (record,observation):
            old=retained.get(item['receipt_id'])
            if old is not None and old!=item:
                raise ValidationError('retained score receipt differs from this re-performance')
            if old is None: pending.append(item)
        if pending: append_receipts(root/'metrics.jsonl',pending)
    card['receipt_id'] = record['receipt_id']
    card['timing'] = dict(**observation['result'], receipt_id=observation['receipt_id'])
    destination.mkdir(parents=True)
    write(destination/'scorecard.json',card)
    lines = ['# Reporting challenge: '+card['status'].upper(), '',
             'Open-book reproduction. Agent identity and token use were not observed.', '',
             f"Fixture `{card['fixture_id']}`; code `{card['git_sha']}`", '',
             '| Task | Grader result |', '|---|---|']
    for task,item in card['tasks'].items(): lines.append('| '+task+' | '+item['status']+' |')
    business = card['values']['late_invoice']
    field = 'net_of_discounts_and_fees_per_mtok'
    lines += ['', '| July recognized usage yield | USD/Mtok |', '|---|---:|',
              '| Original | '+business['original'][field]+' |',
              '| Currently known | '+business['current'][field]+' |',
              '| Delta | '+business['delta'][field]+' |', '',
              'Raw token count unchanged. One late invoice explains the revenue change.', '',
              'Source and all required receipt populations freshly replayed. Complete values, nulls and grading differences are in `scorecard.json`.', '',
              f"Score receipt: `{record['receipt_id']}`.", '',
              f"Grader elapsed: {card['timing']['elapsed_seconds']:.3f} seconds; observation `{observation['receipt_id']}`."]
    (destination/'scorecard.md').write_text('\n'.join(lines)+'\n', encoding='utf-8', newline='\n')
    return card


def replay(ids, *, output_root, ledger, db=None, python=None):
    from tl.receipts.metrics import read_receipts
    root = Path(output_root).parent
    manifest, proof = load(root)
    from tl.compare.engine import artifacts
    from tl.metrics.engine import runtime
    if python or artifacts()!=manifest['execution_artifacts'] or runtime()!=manifest['runtime']:
        from tl.receipts.archive import historical_replay
        return historical_replay(ids,manifest=manifest,db=db or root/'business/world.duckdb',
                                 output_root=output_root,ledger=ledger,python=python)
    verify(root)
    records = read_receipts(ledger); answers=[]
    for key in ids:
        record = records[key]
        if record['schema_version']=='tokenledger-challenge-score-observation/v1':
            answers.append(dict(receipt_id=key,verified=True,recomputed=False,result=record['result'])); continue
        actual = make_card(manifest,proof,record['answer'])
        card = record['result']
        if card!=actual or record['run_id']!=digest(dict(proof=proof['receipt_id'],answer=record['answer']))[:32]:
            raise ValidationError('score re-performance differs')
        for field in ('definition_version','query_hash','git_sha','execution_hash','inputs','asof','known_at','watermark'):
            if record[field]!=proof[field]: raise ValidationError('score receipt contract differs')
        answers.append(dict(receipt_id=key,verified=True,recomputed=True,result=card))
    return answers
