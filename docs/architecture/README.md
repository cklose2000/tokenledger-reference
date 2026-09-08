# Follow one number through the activity model

Open `docs/architecture/index.html` from your checkout in a browser. GitHub
displays the [HTML source](index.html), rather than hosting that viewer. The
[machine-readable specification](architecture.json) is directly inspectable.
The HTML is a standalone
export: no account, localhost refresh service or private configuration is
required. The [compact overview](../assets/one-number.svg) follows the same
reported number.

| Component | What it holds or does | Implementation |
|---|---|---|
| Activity stream | Canonical economic events and provenance; one physical source | [Physical schema](../../tl/stream/schema.sql), [validated writer and snapshot](../../tl/stream/store.py) |
| Logical projections | Typed values, knowledge filtering and temporal joins calculated at query time | [Scaffolds](../../scaffolds), [metric implementation](../../tl/metrics) |
| Versioned definitions | Population, formula, inclusions, exclusions and units | [Definitions](../../definitions/metrics), [recognition policy](../../definitions/policies/recognition/v1.yaml) |
| Reported result | Recognized usage USD per million tokens, with the reporting period and fee basis | [Metric engine](../../tl/metrics) |
| Receipt | Input identity, definition, cutoffs, method and execution version | [Receipt and replay engine](../../tl/receipts) |
| Conventional baseline | Independently calculated recognition and reporting from the same economic events | [dbt baseline](../../baseline) |

The arrow from definitions governs calculation; it is not a second activity
write path. Results and receipts are retained artifacts. One physical source
does not mean one query, no temporary execution state or no logical dependencies.

## Fourteen physical columns

The [DDL](../../tl/stream/schema.sql) is authoritative. The eight ActivitySchema
core fields are `activity_id`, `ts`, `customer`, `anonymous_customer_id`,
`activity`, `feature_json`, `revenue_impact` and `link`. Six provenance fields
are `_recorded_at`, `_stream_position`, `_source`, `_actor`, `_lane` and
`_schema_hash`.

Event time and recorded time serve different purposes. A snapshot applies its
knowledge cutoff and watermark before deriving `activity_occurrence` and
`activity_repeated_at`. Those two calculated fields do not require updating
older physical events. Typed features and temporal relationships live in
rebuildable calculation logic.

## The late invoice

The demo first calculates July's recognized usage revenue and token denominator
at the original knowledge cutoff. A designated late invoice raises eligible
revenue at the later cutoff. Tokens stay fixed. The account bridge explains the
signed movements, and the original receipt still reproduces its earlier result.
Both the before-fee and after-fee presentations stay labeled.

## What is measured

Thirteen output populations matched the independent baseline in four BigQuery
pairs. Native used **51.47% fewer billed bytes** and **14.96 times median slot
time** across three measured pairs. The first pair was slower. Read the
[complete evidence and scope](../evidence.md).

The diagram shows the implemented reporting path. Incremental cloud performance,
general production admission and comparative agent efficiency are subsequent
experiments. They are not additional green paths on this measured map. The
earlier cache-based relation inventory remains historical evidence.

## Export provenance

This export uses [Archify](https://github.com/tt-a1i/archify), with its
[upstream MIT license](ARCHIFY-LICENSE.txt). The fresh public specification
omits private infrastructure and operator metadata. Its
[artifact receipt](artifact-receipt.json) distinguishes deterministic validation,
automated browser checks and image-based visual review. Those checks establish
the diagram's artifact and presentation quality, not accounting correctness.
