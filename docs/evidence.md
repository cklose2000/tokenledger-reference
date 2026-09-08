# What the comparison establishes

Thirteen output populations matched an independently calculated baseline in
four native BigQuery pairs. Across the three measured pairs, native used
**51.47% fewer billed bytes** and **14.96 times median slot time**. Native was
slower in the first measured pair. This is a bounded synthetic experiment,
with three repetitions, rather than a general performance ranking.

## Population and method

Both implementations consumed the same fourteen-column stream: 50,000
synthetic customers, thirty months, seed 42, 2,996,024 physical activities and
2,991,747 visible activities. Reporting date was July 31, 2026; knowledge
cutoff was August 1; the watermark was 2,996,024. NRR used immutable v2.
The [release manifest](../evidence/bigquery/manifest.json) pins the source
fingerprint, cutoffs, output contracts, population hashes and job counters.

The conventional dbt baseline and native SQL independently calculate
recognition from economic events. Generated recognition assertions do not
supply their answer. Both retain source checks, complete output contracts and
provenance. The native reporting path has no persisted reporting intermediates.
This does not eliminate query work, temporary execution state or retained
result artifacts.

Each pair compares all thirteen populations, including journal lines,
recognized revenue, close reports, metric families and cohort membership.
Comparison checks missing and extra keys, duplicates and nulls before values.
Integer counts and cents must agree exactly; other numeric outputs use the
registered tolerance of 1e-6. One warmup pair is excluded from performance
statistics; architecture order alternates through three measured pairs.

## Keep the clocks separate

| Measured pair | First path | Baseline accounted seconds | Native accounted seconds | Native versus paired baseline |
|---|---|---:|---:|---:|
| 1 | Native | 2,308.777 | 2,433.487 | 5.40% higher |
| 2 | Baseline | 1,937.913 | 1,865.344 | 3.74% lower |
| 3 | Native | 2,217.324 | 2,078.570 | 6.26% lower |

| Measurement | Baseline median | Native median |
|---|---:|---:|
| Accounted wall seconds | 2,217.324 | 2,078.570 |
| Slot milliseconds | 964,981 | 14,432,701 |
| Billed bytes per pair | 27,940,356,096 | 13,560,184,832 |

Accounted wall sums disjoint observed intervals. It is not contiguous elapsed
time. It includes source checking, reporting, population hashing and receipt
work. Shared admission, comparison and postflight work remain separate;
final observation sealing is outside the timed intervals. Native serialization
is inside native time, while some baseline export setup is shared. No estimated
subtraction makes these scopes identical.

The ratio of median accounted times is 0.937423. The median of within-pair
ratios is 0.962553. Those answer different questions. Server intervals can
overlap; slot time is distributed work, not visitor waiting time. The operator
laptop, OS cache and background workload were not isolated. These observations
establish neither statistical significance nor a stable speed advantage.

There are 520 actual jobs across all four pairs: 110 baseline and 20 native
per pair. Warmup is included in total usage and excluded from the medians.
The manifest retains observed counters; missing usage is never replaced with
zero. Billed bytes are a resource observation, not an invoice. They exclude
storage, earlier attempts, writes, deployment costs, credits and adjustments.
Slot milliseconds alone do not establish capacity-priced dollar cost.

## Re-perform the retained comparison, offline

The small demo does not need this download. The optional release asset contains
104 original synthetic Parquets, totaling 405,537,539 bytes. The ZIP is
405,553,549 bytes (about 387 MiB); allow at least 1 GiB of additional free disk
for the archive and verification artifacts, beyond the installed environment.
This is a planning allowance, not a measured peak-memory guarantee. The verifier
reads the closed archive directly and does not extract a second copy.

After downloading `bigquery-populations-v0.2.0-rc.5.zip` from this release's
assets, run from its checkout:

```sh
tl release evidence verify --archive bigquery-populations-v0.2.0-rc.5.zip --manifest evidence/bigquery/manifest.json --output data/cloud-review/first --json
```

The output directory must be new. The command checks the release-pinned archive
hash and member inventory, original row receipt identities and every retained
logical fingerprint. It recalculates all 52 comparisons and derives resource
ratios from the selected job counters. Network access is disabled during that
verification. No credentials or warehouse jobs are needed.

Use the emitted receipt ID to inspect the resulting observation:

```sh
tl --artifact-root data/cloud-review/first receipt RECEIPT_ID --json
```

Receipt inspection verifies the saved observation seal and reports
`recomputed: false`. Running `tl release evidence verify` again into a new
directory actually re-performs the retained population checks. Neither command
reruns cloud SQL or regenerates the source stream.

The original operational evidence and the public projection have different
scopes. Grok's independent, agent-assisted engineering review restored the
original sealed package and re-performed 52 comparisons and 104 fingerprints
with credentials unset. The public projection preserves every included Parquet
byte but omits account bindings, local paths and operational IAM records. Its
own verification is recorded in [release status](../release-status.json).
The original review is not an accounting audit or a review of newly packaged
public bytes. Original aggregate anchor:
`mr-d459790a0463f149158787b12f872228`.

The [public projection check](../evidence/bigquery/public-verification.json)
subsequently passed all 52 comparisons and 104 fingerprints in 1,924.213 seconds
on the builder's laptop. Other local checks overlapped parts of that run.
That is a verification-time observation, not a new cloud measurement or a
visitor runtime guarantee. The release includes its small observation seal:

```sh
tl --artifact-root evidence/bigquery/verification receipt mr-3d66e52715aa14f49be17975a4c4c26a --json
```

This quick check reports `verified: true, recomputed: false`. It does not require
the optional archive. The manifest's initial `projection_verification` field is
its packaging-time status; the linked later check records completed verification
without rewriting that manifest's pinned bytes.

## The hypothesis that failed

The first local comparison counted 82 baseline relations versus 20 spine
relations, a 4.1-times reduction. The tenfold hypothesis failed. The baseline
completed the recorded local builds faster. That implementation rebuilt six
caches even for a definition change; its full-July median build times were
162.997 seconds for the baseline and 261.524 seconds for the spine.

The [unchanged historical report](evidence/history/phase-3b-report.txt) retains
its inventory, workload and slower cases. It predates native execution and
the cloud experiment; its `BigQuery: not_run` label describes that historical
checkpoint. Its receipt IDs are historical anchors, not claims that the original
private receipt archive is bundled in this release. See its
[provenance](evidence/history/README.md).

The subsequent native path computes reporting logic at query time. Its cloud
resource trade-off is the one measured above. Fewer physical objects did not
mean less compute. A different workload or a controlled incremental experiment
could change the choice; incremental cloud performance remains unmeasured.

## Boundaries that remain

A receipt proves reproducibility, not population completeness or approval of
an accounting policy. The failed-source challenge tests a publication hold and
bounded recovery. A separately reviewed gateway fixture exercised denied
mutations and finalization; general production operation, access review and
segregation of duties remain separate acceptance work.

The public agent challenge is open-book. Comparative agent accuracy and actual
token efficiency are unmeasured. The private learning trial completed with an
inconclusive result and no promotion. Successful self-improvement is outside
this release's claim.
