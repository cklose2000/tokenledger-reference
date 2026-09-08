# Try it without a guided tour

This is a first-time usability and comprehension exercise, separate from
engineering QA and the future matched-agent benchmark. Use the repository's
own instructions. The builder should observe and record where they fail to
help you, without supplying answers during the attempt.

## Short reading attempt

Start at the README. After roughly one minute, describe in your own words:

1. Which business number the example reports.
2. What changed, and what stayed fixed.
3. What reproducing the original receipt establishes.
4. One thing that the receipt or comparison does not establish.

Keep your first interpretation before reading further. It is evidence of the
document's clarity, rather than a test of your prior reporting knowledge.

## Hands-on attempt

From a clean checkout, follow the installation instructions and run the demo.
Then choose one task in `AGENTS.md`, operate its specified evidence and submit
the required answer to the deterministic grader. Report one concrete limitation.
The challenge is open-book: record whether you used the visible reference answer.
A copied reference answer can verify the grading path; it does not demonstrate
unseen-task reasoning.

Record setup start and readiness times, the environment, completed commands,
errors and the point at which you needed help or stopped. If installation blocks
the task, preserve that result. Do not replace it with a guided successful run.
Use only the supplied synthetic fixtures. Provider credentials are unnecessary.

## What the observer records

| Field | Record |
|---|---|
| Candidate | Exact Git commit and release tag, if one exists |
| First-time status | Whether this person previously saw or helped build the project |
| Environment | OS, Python version, install path chosen |
| Interpretation | The evaluator's actual words, with consent to retain them |
| Setup | Observed start/readiness or stopping point; failures retained |
| Task | Selected task, commands, scorecard and whether expected answers were used |
| Assistance | What help was requested or supplied, and when |
| Limitation | A concrete limitation the evaluator identified |
| Repair | The observed confusion, bounded change and affected check repeated |

Use participant aliases in shared notes and keep contact details separate.
Unknown timings and token consumption stay unknown. Record actual outcomes;
this empty protocol supplies no evaluator results, success rate or endorsement.
