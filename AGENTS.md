# Reproduce and explain the reporting result

Work only on synthetic fixtures created under a new local data directory. Never
read local personal accounts, configure cloud access or publish this repository.

Install the pinned package from this checkout, then run `tl demo --json`.
Run `tl challenge --help` for the release's exact preparation and grading
commands. The generated challenge manifest supplies concrete source/receipt
identities, reporting and knowledge cutoffs, units and visible expected outputs.

Then run `tl learning walkthrough --json` once. It rehearses the governed
reporting-learning loop on a fresh synthetic report and needs `ssh-keygen` on
PATH. Read `docs/learning.md` for what each label means. It is a synthetic
rehearsal, never a trial result or an approval; do not describe its `improved`
status as an efficiency gain. `tl learning replay-walkthrough DIR --json`
re-validates a walkthrough directory.

Complete every registered task:

1. Reproduce July recognized usage revenue per million tokens. State the fee
   basis, the raw token denominator and the original receipt.
2. Explain the late invoice with its exact signed revenue delta, responsible
   activity and unchanged token count. Replay the original after the append.
3. Select immutable NRR v3, which raises the headline floor to USD 100,000.
   Identify changed and unchanged lenses at the same source/knowledge cutoffs,
   then reproduce the old version. Never edit a released definition in place.
4. Demonstrate the failed-source close, its held publication and bounded source
   recovery. Keep the discrepancy visible; do not relax the check.

Submit the required answer fields to the application grader and print its
scorecard. Your own PASS statement is not verification. Wrong answers, missing
tasks or changed evidence must fail. The exercise is open-book; it makes no
claim of independent QA or held-out agent accuracy. Keep unknown token usage
unknown rather than estimating it.

## Frozen release assignment

Start with [Follow the workflow](docs/workflow.md) and the
[blank submission](docs/challenge/answer.blank.json) to construct an answer
through reporting, population inspection and replay. Explain your source,
cutoff and definition choices in a separate note. Record solution exposure.

The alternative below is **visible reference verification**. It uses
`data/challenge/rc6/TASKS.md`, exact-answer task output and `expected.json`.
Fixture `058168fe58cab620a36e54f2d03ea6b5086fcba385d2b54e3b4d19da34cd164f`; proof `mr-b1d47db5df4352e704f228ebd57d80c2`.

Make a working copy first. Grading appends its elapsed-time observation to the
working fixture's receipt ledger. Keep the retained release fixture unchanged;
use a new working directory for each separate attempt.

```sh
python -c "import shutil; shutil.copytree('data/challenge/rc6', 'data/challenge/your-attempt')"
tl challenge verify data/challenge/your-attempt --json
tl challenge task data/challenge/your-attempt july_yield --json
tl challenge task data/challenge/your-attempt late_invoice --json
tl challenge task data/challenge/your-attempt nrr_definition --json
tl challenge task data/challenge/your-attempt failed_close --json
tl challenge grade data/challenge/your-attempt --answer data/challenge/your-attempt/expected.json --output data/score/your-attempt --json
```

The last command is the visible reference answer. Copy and edit the answer
for your own submission; the grader checks every required field. The saved
scorecard is `data/challenge/rc6/reference-score/scorecard.md`.
