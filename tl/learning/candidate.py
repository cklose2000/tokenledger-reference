"""Locate nonauthoritative last-counter discrepancies without changing usage."""
import json
from pathlib import Path

import duckdb
from tl.receipts.metrics import digest

VERSION='last-counter-locators/v1'
SPEC=Path('definitions/learning/last-counter-locators-v1.json')


def diagnose(packet):
    # A typed projection inside the query, not a stored staging layer. Source
    # admission remains the U1 importer's responsibility; evaluation verifies
    # that these exact observations were admitted into its frozen snapshot.
    with duckdb.connect() as conn:
        raw=conn.execute('''WITH typed AS (
          SELECT from_json(value, '{"session_id":"VARCHAR","source_line":"BIGINT",
            "ts":"VARCHAR","counters":{"last_token_usage":{"total_tokens":"HUGEINT",
            "input_tokens":"HUGEINT","output_tokens":"HUGEINT"},
            "total_token_usage":{"total_tokens":"HUGEINT","input_tokens":"HUGEINT",
            "output_tokens":"HUGEINT"}}}') AS r FROM json_each(?)
        ), readings AS (
          SELECT r.session_id, r.source_line, r.ts AS observed_at,
            r.counters.last_token_usage AS last, r.counters.total_token_usage AS cumulative
          FROM typed
        ) SELECT session_id,source_line,observed_at,last.total_tokens AS reported_total,
            last.input_tokens+last.output_tokens AS expected_total,
            last.total_tokens-last.input_tokens-last.output_tokens AS residual,
            CASE WHEN cumulative.total_tokens IS NULL OR cumulative.input_tokens IS NULL
                 OR cumulative.output_tokens IS NULL THEN 'incomplete'
                 WHEN cumulative.total_tokens=cumulative.input_tokens+cumulative.output_tokens
                 THEN 'reconciled' ELSE 'mismatch' END AS cumulative_status
          FROM readings WHERE last.total_tokens IS NOT NULL AND last.input_tokens IS NOT NULL
            AND last.output_tokens IS NOT NULL AND last.total_tokens<>last.input_tokens+last.output_tokens
          ORDER BY session_id,source_line''',[json.dumps(packet['observations'])])
        names=[c[0] for c in raw.description]
        rows=[dict(zip(names,row)) for row in raw.fetchall()]
    for row in rows:
        scope='codex:'+digest(dict(source='codex-local-token-count/v1',session=row['session_id']))[:32]
        observation=f'{scope}:{row["source_line"]:012d}:interval_total:total'
        row['activity_id']='usage:'+digest(dict(account=packet['account_alias'],observation=observation))
        row['next_action']='Inspect this retained observation; preserve last-counter flag and cumulative-only usage policy'
    return rows
