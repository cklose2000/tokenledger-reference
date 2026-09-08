# tokenledger: one activity stream, every reported number reproducible.

**The data model agents actually work on.**

Built from the SOX-controlled subscriber-metrics work I ran at SiriusXM for
19 years. For AI businesses that meter tokens and seats.

[Watch the July number change, then reproduce the original →](docs/demo.md)

**[Run it](docs/run.md)** · **[Understand it](docs/understand.md)** · **[Assess it](docs/assess.md)**

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

Start with [AGENTS.md](AGENTS.md). Reproduce July's token yield, explain the late
invoice, change the NRR definition while retaining the old result, and diagnose
the incomplete-source close. Submit the specified evidence to the deterministic
grader and keep failures visible.

This is an **open-book reproduction exercise**. Comparative agent accuracy and
token efficiency remain unmeasured. A reproducible result alone does not prove
source completeness, accounting-policy approval or SOX operating effectiveness.

Built by [Chandler Klose](https://github.com/cklose2000).
[Apache-2.0](LICENSE) · [Cite this work](CITATION.cff) · [Release scope](RELEASE.md)
