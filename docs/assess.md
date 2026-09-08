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

Use a copy of the answer template for your submission. Run the application
grader; an agent's own PASS statement is not evidence. Keep failed or incomplete
tasks in the resulting scorecard.

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
