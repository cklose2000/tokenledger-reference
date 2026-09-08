"""Isolated entry point for the pinned dbt environment; no ambient project."""
import argparse
import json
from pathlib import Path

from tl.bigquery.config import Binding
from tl.stream.events import canonical


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--request', type=Path, required=True)
    parser.add_argument('--result', type=Path, required=True)
    args = parser.parse_args()
    request = json.loads(args.request.read_text(encoding='utf-8'))
    if set(request) != {'binding', 'baseline_directory', 'native_directory', 'directory',
                        'baseline_sha256', 'native_sha256', 'threads', 'architecture_order'}:
        raise ValueError('unexpected native baseline worker request fields')
    from tl.bigquery.baseline_runner import run
    result = run(Binding(**request.pop('binding')), **request)
    with args.result.open('x', encoding='utf-8', newline='\n') as out:
        out.write(canonical(result) + '\n')
    print(canonical(result))
    return 0 if result['status'] == 'equivalent' else 1


if __name__ == '__main__':
    raise SystemExit(main())
