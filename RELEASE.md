# Reference release candidate

Prepared September 8, 2026 as **v0.2.0-rc.5**. This is the separate public-only
candidate, extending the prior candidate at `29c1cf3`. Its initial commits
assembled reviewable groups from an existing implementation; they do not claim
an earlier public development timeline. Intermediate assembly commits are not
individually runnable releases. Original private Git objects are excluded.

The reference contains generic runtime, definitions, SQL, the independently
implemented baseline, selected regression tests and synthetic demonstration
fixtures. It excludes personal usage, accounts, signing material, private
application configuration and hiring strategy. The active engine repository
remains private.

## What is pinned

- The source, recognition policy, metric definitions and shared receipt engine
  are versioned in this Git history. Retained local receipts point to execution
  revisions available here, rather than inaccessible private revisions.
- The new console recording and its retained source execute public commit
  `825696cf31d4c2892078512e612840d791fba2ba`. Later documentation and packaging
  commits retain that revision. The original receipt replays through the
  explicit relocated source and artifact paths in `docs/demo.md`.
- The refreshed challenge is `data/challenge/rc5`, generated and reference-graded
  under the same public executable. The older `data/challenge/first` fixture
  remains available for historical replay. Its grader correctly refuses a
  changed executable; the refresh preserves that guard rather than relaxing it.
- The optional cloud projection preserves 104 original synthetic Parquets and
  selected job counters. Its archive is about 387 MiB and is separate from
  installation and the small demo. Original cloud operations, later independent
  restoration and the public packaging check remain distinct evidence scopes.
- The base image pins Python 3.11.5; the package pins the reporting dependencies.
  The optional SSH devcontainer feature supports hosted CLI verification. Its
  provisioning result is recorded separately from the reporting runtime.

The [release status](release-status.json) records actual acceptance. Successful
local checks do not imply a completed hosted Codespaces session, first-time
evaluator observation, publication approval or comparative agent result.

## Claims and release gates

Keep the cloud findings together: **51.47% fewer billed bytes; 14.96 times median
slot time**. Preserve the slower first pair and the earlier failed tenfold
hypothesis. See `docs/evidence.md` for units, clocks, limitations and original
versus current verification scopes.

Grok's original-package review is agent-assisted engineering QA, not an
accounting audit. Receipts establish reproducibility; source completeness,
policy approval and operating control effectiveness are separate questions.
General production admission and comparative agent efficiency remain outside
this release's acceptance. L1 completed inconclusive with no promotion.

Publication requires Chandler's approval of the exact candidate and destination
after actual first-time evaluator observations and resulting repairs. No
invitations, posts or endorsements are implied by these prepared materials.
Until that approval, the tag and asset URL in the manifest are proposed release
coordinates; they are not a claim that a public download already exists.
