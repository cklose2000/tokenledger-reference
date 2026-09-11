# Reporting learning walkthrough (synthetic rehearsal)

One synthetic business, one receipted July NRR report, and the complete governed feedback loop run through the same gateway, admission rules, candidate SQL, oracle and receipt engine as the native application. The warehouse is an in-memory stream and DuckDB. Nothing here is a real trial, a cloud execution or Chandler's approval.

| Step | What happened | Receipt or record |
|---|---|---|
| Report | July NRR, 4 lenses dated 2026-07-31 | `mr-7d9045ba2c349988855e54a1b2916737` |
| Wrong-date inspection | requested 2026-07-01: 0 rows, no explanation | baseline.json |
| Finding and verification | one confirmed finding, one refuted claim kept visible | events.json |
| Candidate evaluation | 11 frozen cases, DuckDB candidate equals scalar oracle | `mr-3a344c906fb8f0e07849e4056c8cc3c5` |
| Approval boundary | disposable synthetic signer; expanded, altered and misrouted approvals refused | plan.json, plan.json.sig |
| Next inspection | first inspection after authorization, requested 2026-07-01 | `mr-bc68ce6af5764e4df5dd9fbf7c0d824f` |
| Outcome | `improved`: The candidate explained a builder-chosen synthetic wrong-date inspection. Not a measured operating gain. | `mr-d2cbb62f1660212f7789899fee4f38d2` |
| Refusals | 11 refused attempts; stream unchanged after each | refusals.json |
| Preservation | original report reproduced; 0 learning rows in the business stream | walkthrough.json |

Workflow events admitted: 11. Observed wall time: 6.799 seconds (an observation, not a benchmark).

## What the labels mean

- Approval authority: Disposable synthetic signer generated in this directory. Not Chandler, not enrolled trust, not a real trial authorization.
- Outcome `improved` on a builder-chosen synthetic inspection means the candidate produced an explanation. It is not an efficiency measurement.
- The one real signed native trial (fabd91a) completed **inconclusive**: the operator requested 2026-07-31 and all four rows were selected. No promotion or ongoing activation occurred. Its private receipts are not reproduced here.

## Replay

From the repository checkout that produced this directory:

```sh
tl learning replay-walkthrough "C:\tokenledger-demo\rc6-933cd94\data\learning-walkthrough\recording" --json
tl --db "C:\tokenledger-demo\rc6-933cd94\data\learning-walkthrough\recording\world.duckdb" --artifact-root "C:\tokenledger-demo\rc6-933cd94\data\learning-walkthrough\recording" receipt mr-7d9045ba2c349988855e54a1b2916737 --json
tl --artifact-root "C:\tokenledger-demo\rc6-933cd94\data\learning-walkthrough\recording" receipt mr-3a344c906fb8f0e07849e4056c8cc3c5 --json
```

The first command rebuilds workflow state from the retained event stream, re-verifies the detached signature against the synthetic trust, re-performs the candidate and oracle, reproduces the original report and fails on any changed byte.
