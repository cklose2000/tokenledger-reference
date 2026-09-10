# Open-book reporting challenge

The expected answers are visible. This is a reproduction exercise, not held-out agent evaluation.

Fixture: `058168fe58cab620a36e54f2d03ea6b5086fcba385d2b54e3b4d19da34cd164f`. Code: `933cd94525e20d651c81b192b36cd06d9a552ef2`.

Proof receipt: `mr-b1d47db5df4352e704f228ebd57d80c2`.

Use the relative paths below from this directory, with `tl` run from the installed release repository.

| Task | Required proof |
|---|---|
| July yield | Sum recognized usage cents and raw tokens; reproduce the original receipt. |
| Late invoice | Exact original/current/delta, the named invoice, unchanged tokens and later cutoff. |
| NRR definition | Released v2 to v3 at identical source/cutoffs; one changed headline and three unchanged lenses. |
| Failed close | Missing source holds publication; captured source recovery and retry do not duplicate rows. |

All exact receipt IDs, values, nulls, units and status fields are in `expected.json`. Definitions and cutoffs are in `runs/31351255c58c45eeb7b7a7b892e3d034/run.json`.

Only these two synthetic sandbox databases may be mutated. The late invoice is already appended by preparation; the failed source is already recovered. Frozen watermarks reproduce both earlier states.

From the release root:

```text
tl challenge run --directory <new-directory> --json
tl challenge verify <prepared-directory> --json
tl challenge task <prepared-directory> july_yield --json
tl challenge task <prepared-directory> late_invoice --json
tl challenge task <prepared-directory> nrr_definition --json
tl challenge task <prepared-directory> failed_close --json
tl challenge grade <prepared-directory> --answer <your-answer.json> --output <new-score-directory> --json
tl challenge close <prepared-directory> --watermark 4 --json
tl challenge close <prepared-directory> --recover --json
```

The historical close command exits 2 and leaves the accepted publication untouched. `grade` returns nonzero for incorrect or incomplete answers and replays evidence before scoring. A supplied PASS is ignored.

The scorecard records actual grader elapsed time separately from deterministic correctness. Agent identity and tokens remain unobserved unless a separate measured harness supplies them.
