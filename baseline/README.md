# Independent conventional baseline

The baseline independently calculates recognition from the same commercial
activities, with normal shared macros, variables and dependency selection.
It does not consume the spine's recognized outputs. The output registry
defines the exact grains, units and tolerances used by the comparison.

Install `baseline/requirements-views-lock.txt` in an isolated
`.venv-baseline-views` environment. Generate a synthetic source with `tl generate`
and use `tl compare --help` for native comparison and suite options. Both arms
use the declared source and runtime; baseline dbt is outside the spine's path.
Large comparisons and their timing are separate from the small evaluator demo.
