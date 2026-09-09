# Assess the implementation

The [agent entry file](../AGENTS.md) points to four concrete tasks in a retained
synthetic fixture. Give an evaluator the release and that entry file; the task
contracts contain the required artifacts, quantities and pass/fail conditions.
The current assignment is `data/challenge/rc5/TASKS.md`. The older `first`
fixture remains historical replay evidence; use the refreshed assignment for
grading against this release's executable.

1. Reproduce July recognized revenue per million tokens, stating its fee basis.
2. Explain the late invoice's signed bridge and unchanged token count.
3. Select the immutable USD 100,000 NRR floor definition and retain the earlier
   definition's replay.
4. Diagnose the incomplete-source close and demonstrate its bounded recovery.

## Learn the workflow

Follow the [ordinary reporting and replay interfaces](workflow.md) to construct
a submission from source bindings, cutoffs and observed populations. Start with
the [blank answer](challenge/answer.blank.json), its
[field schema](challenge/answer.schema.json) and
[submission vocabulary](challenge/vocabulary.md). This route supplies commands and
field locations without filling the business values for you.

Use a copy of the blank answer for your submission. Run the application
grader; an agent's own PASS statement is not evidence. Keep failed or incomplete
tasks in the resulting scorecard.

Follow the working-copy command in `AGENTS.md` before grading. The grader
retains a new timing observation in that copy's receipt ledger, so your attempt
does not modify the retained release fixture. A repeated identical answer has
the same correctness receipt; its elapsed-time observation is separate.

## Verify the reference

This second route deliberately reveals the solution. `tl challenge task`
re-performs the evidence and prints the exact required facts for a task.
`expected.json` is the complete visible reference answer. Grading it verifies
the reference path; it does not show that you constructed an answer yourself.

```sh
python -c "import shutil; shutil.copytree('data/challenge/rc5', 'data/challenge/my-reference')"
tl challenge task data/challenge/my-reference july_yield --json
tl challenge grade data/challenge/my-reference --answer data/challenge/my-reference/expected.json --output data/score/my-reference --json
```

Other task names are `late_invoice`, `nrr_definition` and `failed_close`.
Retain a wrong-cent and missing-task FAIL on separate attempts. Invalid source
evidence must be rejected before a scorecard exists. The
[workflow guide](workflow.md#5-submit-observations-and-retain-rejection-evidence)
explains these distinct boundaries.

This is an open-book reproduction and usability exercise with visible expected
answers. Copying those answers can demonstrate that the grader runs; it cannot
establish unseen-task reasoning accuracy. No comparative agent-efficiency result
is claimed by this release.

For architecture assessment, [inspect the BigQuery comparison](evidence.md).
It preserves both the lower billed bytes and higher slot time. The optional
population package lets you re-perform retained comparisons locally; that is
distinct from rerunning the warehouse workload.

A first-time evaluation should record what you understood, where setup stalled,
which task you completed without assistance and one concrete limitation. Record
observed time and failures. Do not infer identity, employer or token consumption
from a self-reported scorecard.
