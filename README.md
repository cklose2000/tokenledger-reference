# tokenledger: one activity stream, every reported number reproducible.

**A runnable reference implementation for data and finance engineering teams.**
Report a business metric, explain a change and reproduce the original result.

Built from the SOX-controlled subscriber-metrics work I ran at SiriusXM for
19 years. For AI businesses that meter tokens and seats.

[Watch the July number change, then reproduce the original →](docs/demo.md)

**[Run it](docs/run.md)** · **[Understand it](docs/understand.md)** · **[Assess it](docs/assess.md)** · **[The learning loop](docs/learning.md)**

![One physical activity stream, query-time projections, versioned definitions, a reported result and its receipt](docs/assets/one-number.svg)

A late invoice changes July's recognized revenue per million tokens. The token
count stays fixed. The accounting bridge explains the change, and the original
receipt still reproduces the earlier number. Run that whole example:

```sh
python -m pip install -e ".[dev]"
tl demo
```

Use Python 3.11.5 from this checkout. The fixture is synthetic, the local engine
is DuckDB, and the demo requires no model, billing or cloud credentials.
[Installation and environment options](docs/run.md).

Reuse the activity envelope, validated writer, snapshots and receipts. Applying
them to another business requires its source contracts, customer identities,
accounting policy and accountable reporting owners. The
[adoption map](docs/adoption.md) shows the integration work and a one-output
migration with reconciliation and rollback.

## How reporting behavior changes

A reader asked the July NRR report for July 1 and got nothing back. That
mistake became a recorded finding, an independent verification, a bounded
diagnostic candidate evaluated against frozen cases, a signed one-run approval,
one next inspection and a recorded outcome. The original report still
reproduces afterwards. Run the whole loop on a fresh synthetic report:

```sh
tl learning walkthrough
```

[Watch the recorded walkthrough and read what each label means](docs/learning.md).
The walkthrough is a credential-free synthetic rehearsal with a disposable
signer. The one actual signed native trial completed **inconclusive** with no
promotion; that honest result is retained, not improved upon.

## What the comparison found

Thirteen output populations matched an independently implemented baseline in
four native BigQuery pairs: one warmup and three measured pairs. Native used
**51.47% fewer billed bytes** and **14.96× median slot time**. It was slower in
the first measured pair. These results belong together.

[Read the measurement methods, individual pairs and limitations](docs/evidence.md).
The [earlier local tenfold hypothesis failed](docs/evidence.md#the-hypothesis-that-failed):
82 versus 20 relations, with faster baseline builds. That report is preserved.
The large retained populations are an optional, checksum-pinned download;
they are separate from the small demo. Their verification recalculates retained
comparisons and fingerprints without submitting cloud jobs.

## Give it to your agent

Start with [AGENTS.md](AGENTS.md). [Follow the workflow](docs/workflow.md) with a
blank submission, or [verify the visible reference](docs/assess.md#verify-the-reference).
Reproduce July's token yield, explain the late
invoice, change the NRR definition while retaining the old result, and diagnose
the incomplete-source close. Submit the specified evidence to the deterministic
grader and keep failures visible.

This is an **open-book reproduction exercise**. Comparative agent accuracy and
token efficiency remain unmeasured. A reproducible result alone does not prove
source completeness, accounting-policy approval or SOX operating effectiveness.

Built by [Chandler Klose](https://github.com/cklose2000).
[Apache-2.0](LICENSE) · [Cite this work](CITATION.cff) · [Release scope](RELEASE.md)
