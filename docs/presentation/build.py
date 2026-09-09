"""Deterministic presentation only. Reads retained evidence; never runs cloud SQL.

Run from a pinned editable install: python docs/presentation/build.py [--check]
"""
import argparse
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import html
import json
from pathlib import Path
import re

import yaml

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def emit(path, value, check):
    raw = value.encode('utf-8')
    if check:
        if not path.exists() or path.read_bytes() != raw:
            raise SystemExit('Stale presentation: ' + str(path.relative_to(ROOT)))
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)


def money(cents, signed=False):
    amount = Decimal(cents) / 100
    return ('+' if signed and amount > 0 else '') + f'${amount:,.2f}'


def rate(value, signed=False):
    amount = Decimal(value).quantize(Decimal('.001'), rounding=ROUND_HALF_UP)
    return ('+' if signed and amount > 0 else '') + f'${amount:,.3f}'


def demo(check):
    from tl.receipts.metrics import receipt_id
    ledger = ROOT / 'data/demo/recorded/metrics.jsonl'
    records = {r['receipt_id']: r for r in map(json.loads, ledger.read_text().splitlines())}
    manifest = json.loads((ROOT / 'data/demo/recorded/demo.json').read_bytes())
    y = records[manifest['yield_bridge_receipt_id']]
    a = records[manifest['bridge_receipt_id']]
    for record in [y, a]:
        if receipt_id(record) != record['receipt_id']:
            raise SystemExit('Receipt seal differs')
    result = y['result']
    original, current, delta = [result[k] for k in ('original', 'current', 'delta')]
    if current['tokens'] != original['tokens'] or delta['tokens'] != 0:
        raise SystemExit('This presentation requires the unchanged-denominator fixture')
    policy_path = ROOT / 'definitions/accounting/v1.yaml'
    accounts = yaml.safe_load(policy_path.read_text())['accounts']
    labels = {str(v): k.replace('_', ' ') for k, v in accounts.items()}
    rows = [
        ['Recognized usage revenue', money(original['net_revenue_cents']), money(current['net_revenue_cents']), money(delta['net_revenue_cents'], True)],
        ['Raw processed tokens', f"{original['tokens']:,}", f"{current['tokens']:,}", '0'],
        ['Revenue per million tokens', rate(original['net_of_discounts_and_fees_per_mtok']), rate(current['net_of_discounts_and_fees_per_mtok']), rate(delta['net_of_discounts_and_fees_per_mtok'], True)],
    ]
    postings = []
    for row in a['result']['rows']:
        postings.append(f"{money(abs(row['delta_cents']))} {'debit' if row['delta_cents'] > 0 else 'credit'} to {labels[row['account']]} ({row['account']})")
    summary = {
        'schema_version': 'tokenledger-recorded-summary/v1',
        'scope': 'Presentation of the retained recorded run; not live progress or a new computation',
        'source_ledger': ledger.relative_to(ROOT).as_posix(), 'source_ledger_sha256': sha(ledger.read_bytes()),
        'account_mapping_sha256': sha(policy_path.read_bytes()),
        'receipt_id': y['receipt_id'], 'account_bridge_receipt_id': a['receipt_id'],
        'result': result, 'account_bridge': a['result'],
        'display_rounding': 'USD cents unchanged; USD/Mtok three decimal places half up; exact values retained',
    }
    emit(ROOT / 'docs/assets/demo-summary.json', json.dumps(summary, indent=2) + '\n', check)
    table = '<table><thead><tr><th scope="col">Measure</th><th scope="col">Original July</th><th scope="col">With late evidence</th><th scope="col">Change</th></tr></thead><tbody>'
    for cells in rows:
        table += '<tr><th scope="row">' + html.escape(cells[0]) + '</th>' + ''.join('<td>'+html.escape(s)+'</td>' for s in cells[1:]) + '</tr>'
    table += '</tbody></table>'
    exact = json.dumps({'yield_receipt_id': y['receipt_id'], 'yield': result, 'account_receipt_id': a['receipt_id'], 'accounts': a['result']}, indent=2)
    panel = f'''<section class="summary" aria-labelledby="summary-title">
<div class="eyebrow">Summary of the recorded run</div>
<h2 id="summary-title">Same tokens. One invoice. An explained change.</h2>
<p>Recognized usage revenue in USD, net of discounts and marketplace fees. Display rounding only; the exact values and both fee presentations are below.</p>
<div class="table-wrap">{table}</div>
<p class="posting">{html.escape('; '.join(postings))}.</p>
<p class="notes">The original result reproduced after the append. This summary is derived from the saved receipts and is available before playback.</p>
<details><summary>Exact values, cutoffs and receipts</summary><p>{html.escape(result['denominator'])}.</p>
<p>Full signed journal cents are retained. A credit to usage revenue is an increase in revenue.</p>
<button id="copy-evidence" type="button">Copy exact evidence</button><span id="copy-status" role="status"></span>
<pre id="exact-evidence" tabindex="0">{html.escape(exact)}</pre>
<p><a href="assets/demo-summary.json">Download summary and source bindings</a> · <a href="demo.md">Reproduce these receipts</a></p></details>
</section>'''
    template = (HERE / 'demo.template.html').read_text(encoding='utf-8')
    captured = json.loads(re.search(r'<script type="application/json" id="captured-output">(.*?)</script>', template, re.S).group(1))
    cast = [json.loads(line) for line in (ROOT/'docs/assets/recording/demo.cast').read_text().splitlines()]
    transcript = ''.join(row[2] for row in captured).replace('\r\n', '\n')
    if captured != cast[1:] or transcript != (ROOT/'docs/assets/recording/demo.txt').read_text():
        raise SystemExit('Template events differ from the original capture')
    if template.count('<!-- RECORDED_SUMMARY -->') != 1:
        raise SystemExit('Missing unique summary marker')
    emit(ROOT / 'docs/demo.html', template.replace('<!-- RECORDED_SUMMARY -->', panel), check)
    md = '\n'.join(['| Measure | Original July | With late evidence | Change |', '|---|---:|---:|---:|'] + ['| ' + ' | '.join(row) + ' |' for row in rows])
    md += '\n\n' + '; '.join(postings) + '.\n\n'
    md += f"Derived from yield receipt `{y['receipt_id']}` and account receipt `{a['receipt_id']}`. [Exact values and source bindings](assets/demo-summary.json).\n"
    page = (ROOT / 'docs/demo.md').read_text(encoding='utf-8')
    updated, count = re.subn(r'<!-- SUMMARY_START -->.*?<!-- SUMMARY_END -->', '<!-- SUMMARY_START -->\n' + md + '<!-- SUMMARY_END -->', page, flags=re.S)
    if count != 1:
        raise SystemExit('Missing Markdown summary markers')
    emit(ROOT / 'docs/demo.md', updated, check)


def challenge_contract(check):
    # Derive field structure only. No fixture answers are filled in the blank.
    expected = json.loads((ROOT / 'data/challenge/rc5/expected.json').read_bytes())
    def blank(value, path=''):
        if isinstance(value, dict):
            return {k: blank(v, path+'.'+k) for k, v in value.items()}
        if isinstance(value, list):
            return ['__REPLACE_WITH_OBSERVED_LIST_ITEMS__']
        if path == '.schema_version':
            return value
        return '__REPLACE__'
    def schema(value):
        if isinstance(value, dict):
            return dict(type='object', required=list(value), additionalProperties=False,
                        properties={k: schema(v) for k, v in value.items()})
        if isinstance(value, list):
            return dict(type='array', items=schema(value[0]) if value else {})
        if value is None:
            return dict(type='null')
        if isinstance(value, bool):
            return dict(type='boolean')
        if isinstance(value, int):
            return dict(type='integer')
        if isinstance(value, float):
            return dict(type='number')
        return dict(type='string', pattern='^(?!__REPLACE).*$', minLength=1)
    contract = schema(expected)
    contract.update({'$schema': 'https://json-schema.org/draft/2020-12/schema',
                     'title': 'Frozen rc5 reporting exercise answer structure',
                     'description': 'Structure only. Correctness is decided by tl challenge grade after source re-performance. Decimal ratios use strings; integer cents and counts use JSON integers. Retain observed explicit nulls; __REPLACE__ is never a null.',
                     'x-source': 'data/challenge/rc5/expected.json field structure; values omitted'})
    contract['properties']['schema_version'] = {'const': expected['schema_version']}
    emit(ROOT / 'docs/challenge/answer.blank.json', json.dumps(blank(expected), indent=2)+'\n', check)
    emit(ROOT / 'docs/challenge/answer.schema.json', json.dumps(contract, indent=2)+'\n', check)


def architecture(check):
    base = (HERE / 'archify-upstream.html').read_bytes()
    # Frozen original export: new renderer output needs a deliberate rebase.
    if sha(base) != '1105835279de1a29a23e436fb51d73dabf9f3b054973634aeb8a8640950c6d01':
        raise SystemExit('Unexpected Archify upstream bytes')
    css = (HERE / 'architecture-reader.css').read_text(encoding='utf-8')
    js = (HERE / 'architecture-reader.js').read_text(encoding='utf-8')
    spec = json.loads((ROOT / 'docs/architecture/architecture.json').read_bytes())
    source = base.decode('utf-8')
    source = source.replace('</head>', '<style id="tokenledger-reader-style">\n'+css+'\n</style></head>', 1)
    source = source.replace('</body>', '<script type="application/json" id="tokenledger-reader-spec">'+json.dumps(spec).replace('<', '\\u003c')+'</script>\n<script>\n'+js+'\n</script></body>', 1)
    emit(ROOT / 'docs/architecture/index.html', source, check)
    binding = {'schema_version': 'tokenledger-presentation-transform/v1',
               'scope': 'Repeatable viewer-only transform; unchanged authored graph and original Archify export',
               'upstream_sha256': sha(base), 'output_sha256': sha(source.encode('utf-8')),
               'specification_sha256': sha((ROOT / 'docs/architecture/architecture.json').read_bytes()),
               'inputs': {p.name: sha(p.read_bytes()) for p in [Path(__file__), HERE/'architecture-reader.css', HERE/'architecture-reader.js']},
               'upstream_artifact_receipt': 'artifact-receipt.json',
               'current_browser_evidence': 'reader-check.json',
               'distinction': 'Original Archify delivery receipt covers the upstream export. Browser checks bind this transformed artifact separately.'}
    emit(ROOT / 'docs/architecture/reader-receipt.json', json.dumps(binding, indent=2)+'\n', check)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    demo(args.check)
    challenge_contract(args.check)
    architecture(args.check)
    print('Presentation matches retained inputs.' if args.check else 'Presentation rebuilt from retained inputs.')
