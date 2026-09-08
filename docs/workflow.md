# Learn the reporting workflow

Use the ordinary reporting and replay interfaces to construct an answer. This
is an open-book exercise: its reference solution is visible in a separate
[verification route](assess.md#verify-the-reference). Record if you consult it.
A passing score establishes reproduction of this fixture, not unseen reasoning
accuracy or independent validation of the recognition policy.

After [installation](run.md), run commands from the repository root. Examples
use `python` and `tl` in the activated environment. On Windows, you can use
`.\.venv\Scripts\python.exe` and `.\.venv\Scripts\tl.exe` instead.

## 1. Copy the fixture and find its bindings

Keep the retained fixture unchanged. Use a new attempt name on a second run.

```sh
python -c "import shutil; shutil.copytree('data/challenge/rc5', 'data/challenge/my-attempt'); shutil.copyfile('docs/challenge/answer.blank.json', 'data/challenge/my-attempt/answer.json')"
tl challenge verify data/challenge/my-attempt --json
```

Verification freshly re-performs the prepared evidence. It does not grade your
answer. The [blank submission](challenge/answer.blank.json) names every required
field; the [JSON schema](challenge/answer.schema.json) describes their types.
Replace every `__REPLACE__` marker with an observed value. Integers stay JSON
integers, ratios stay decimal strings, and observed nulls stay `null`. An
unanswered field is never represented by a valid null.

Inspect the manifest without opening `expected.json`:

```sh
python -c "import json; from pathlib import Path; p=Path('data/challenge/my-attempt'); h=json.loads((p/'challenge.json').read_bytes()); m=json.loads((p/'runs'/h['run_id']/'run.json').read_bytes()); print(json.dumps({k:m[k] for k in ['fixture_id','fixture','keys','asof','known_at','watermark','scoring']},indent=2))"
```

`keys` locates the yield, account bridge, NRR and close receipts. The business
source is `business/world.duckdb`; its ledger is `business/metrics.jsonl`.
The close exercise has its own source and ledger under `close/`. Historical
absolute paths describe the producer's machine. Use these working-copy paths
when replaying.

For a receipt, replace `RECEIPT_ID` in the following command with the selected
manifest key. It prints the source/cutoff binding and the retained result:

```sh
python -c "import json; from pathlib import Path; key='RECEIPT_ID'; r=next(json.loads(s) for s in Path('data/challenge/my-attempt/business/metrics.jsonl').read_text().splitlines() if json.loads(s)['receipt_id']==key); print(json.dumps(r,indent=2))"
tl --db data/challenge/my-attempt/business/world.duckdb --artifact-root data/challenge/my-attempt/business receipt RECEIPT_ID --json
```

Reading a receipt is inspection. The second command is re-performance. State
which you did. Each population's Parquet is under
`business/runs/<receipt.run_id>/<receipt.result.output>.parquet`.

## 2. Report July and explain the invoice

Three boundaries travel together: `--asof` fixes the reporting period,
`--known-at` fixes evidence arrival time, and `--watermark` fixes the admitted
stream prefix. Read them from the selected receipt before running a query.
The original July example deliberately knows evidence through October 1;
this is a synthetic late-close fixture.

The following command uses the original bindings in this release. Confirm them
against `keys.yield_old` before executing:

```sh
tl --db data/challenge/my-attempt/business/world.duckdb --artifact-root data/workflow/my-original profile --asof 2026-07-31 --known-at 2026-10-01T00:00:00Z --watermark 2763 --population net_rev_per_mtok --definition v2 --json
```

Use the returned `run_id` to inspect that population:

```sh
python -c "import json; import pyarrow.parquet as pq; rows=pq.read_table('data/workflow/my-original/runs/RUN_ID/net_rev_per_mtok.parquet').to_pylist(); july=[r for r in rows if str(r['month'])=='2026-07-01']; print(json.dumps(july[:8],default=str,indent=2))"
```

Replace `RUN_ID` with your run, not an example from another machine. Inspect
column names and the definition before summing. The population retains multiple
months: filter to `month = 2026-07-01` first. The example displays eight rows;
aggregate **all** selected July rows, not just that preview. Sum disjoint revenue
cents and raw tokens, then divide; averaging per-cut yields gives a different question.
Include all four token types at unit weight and distinguish revenue net of
discounts from revenue net of discounts **and** marketplace fees.

Replay `keys.yield_old`, `keys.yield_new`, `keys.yield_bridge` and
`keys.account_bridge`. The bridge's result supplies units, both fee bases,
original/current cutoffs and the responsible activity IDs. Inspect the actual
invoice in the physical source by that ID:

```sh
python -c "import duckdb; c=duckdb.connect('data/challenge/my-attempt/business/world.duckdb',read_only=True); print(c.execute('SELECT activity_id, ts, _recorded_at, _stream_position, feature_json FROM stream.activity WHERE activity_id = ?', ['ACTIVITY_ID']).fetchall())"
```

This is a single-event inspection, not a replacement for a knowledge-filtered
metric query. The native `tl profile` path handles the snapshot. Re-run the
population at `keys.yield_new`'s recorded cutoff and watermark, then explain the
signed revenue difference and whether raw tokens changed. Replay the original
again after observing the later evidence.

## 3. Change a definition while fixing the source

Read [v2](../definitions/metrics/consumption_nrr/v2.yaml) and
[v3](../definitions/metrics/consumption_nrr/v3.yaml). Select the new immutable
definition; do not edit the old file. Use identical source, reporting cutoff,
knowledge cutoff and watermark on both runs:

```sh
tl --db data/challenge/my-attempt/business/world.duckdb --artifact-root data/workflow/my-nrr-v2 profile --asof 2026-07-31 --known-at 2026-10-01T00:00:00Z --watermark 2763 --population consumption_nrr --definition v2 --json
tl --db data/challenge/my-attempt/business/world.duckdb --artifact-root data/workflow/my-nrr-v3 profile --asof 2026-07-31 --known-at 2026-10-01T00:00:00Z --watermark 2763 --population consumption_nrr --definition v3 --json
```

Inspect each `consumption_nrr.parquet` using the previous population-reading
recipe. Match rows by `lens`. Keep every measure, including explicit nulls;
remove only the `receipt_id` and use `lens` as the answer's dictionary key.
List changed and unchanged lenses. Explain which population moved and why.
Replay the fixture's `keys.nrr_old` and `keys.nrr_new` receipts. Your new run
has new IDs; the submitted fixture receipt IDs come from its manifest.

## 4. Investigate a held close

Run the fixture's original failed-source boundary. This release records
`failed_source.failed.watermark = 4`; omitting it selects the already-recovered
current source, which is a different question:

```sh
tl challenge close data/challenge/my-attempt --watermark 4 --json
```

Exit **2** means publication is held. Read `reconciliation.exceptions`, the
period counts and cent totals, and the missing activity ID. Compare the
`close/capture/manifest.json` control population with the source prefix recorded
under `failed_source.failed` in the challenge manifest.

Then retry the retained bounded recovery:

```sh
tl challenge close data/challenge/my-attempt --recover --json
```

The prepared fixture already retains the recovery. This command exercises its
idempotent retry; it is not a newly collected source. Keep the held report and
its cause. Verify `close/publication.json` stays byte-identical on another retry.
For the answer's before/after totals, sum each count/cent field across the
corresponding `reconciliation.periods`. Copy exact units and contract status
strings from the evidence and [task contract](../tl/challenge/engine.py).

## 5. Submit observations and retain rejection evidence

Fill `answer.json` using the observed populations, receipts and close result.
Keep a separate short note explaining your cutoff and policy choices, commands,
failures, any solution exposure and unknown measurements. The strict answer
file contains only its schema fields; do not put narrative in it.

```sh
tl challenge grade data/challenge/my-attempt --answer data/challenge/my-attempt/answer.json --output data/score/my-attempt --json
```

The grader first re-performs source evidence, then checks values. It appends a
timing observation to your working copy. Read `scorecard.md` and the detailed
differences in `scorecard.json`. A reference PASS can be obtained through the
[separately labeled solution route](assess.md#verify-the-reference).

On a separate attempt copy, change one submitted revenue cent and grade again:
expect a FAIL scorecard identifying that field. Omit a task and expect FAIL.
Altering source evidence should be rejected **before** a correctness receipt or
scorecard exists. Never change the retained fixture, lower tolerances or repair
a failure by copying over its evidence. Report unsuccessful tasks as observed.
