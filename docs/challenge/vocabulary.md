# Submission vocabulary

Use this contract with the [workflow](../workflow.md) and
[blank answer](answer.blank.json). Field paths below are relative to `tasks`.
The grader compares status, policy and unit strings exactly, including case and
punctuation. These spellings explain how to report an observation; they do not
establish that the observation occurred. Leave an uncompleted task visible and
retain its FAIL instead of claiming a successful check.

## Values, dates and identities

- Keep `schema_version` as `tokenledger-challenge-answer/v1`. Get `fixture_id`
  and fixture receipt IDs from the manifest, not from your new profile runs.
- Revenue cents, token quantities, cohort counts and inserted counts are JSON
  integers. Ratios are decimal strings; the grader allows an absolute difference
  of `0.000001` for ratios. Explicit missing measures stay JSON `null`, never
  zero or the string `"null"`. An unanswered placeholder is not an observed null.
- Yield rows identify a calendar month by its first day. NRR rows identify the
  reporting date. Keep each population's `month` as its `YYYY-MM-DD` string.
  Use the NRR-specific reader in the workflow to check all four lenses.
- Preserve knowledge timestamp strings from the receipts, including their UTC
  suffix and fractional seconds. The grader compares these strings exactly.
- NRR `original` and `current` objects are keyed by `lens`. Remove `lens` and
  `receipt_id` from each row, but retain every other field and observed null.
  Lens identifiers are `base_t12m`, `floor_100k`, `subscription_inclusive` and
  `t3m_annualized`. Determine changed and unchanged lists by comparing the rows;
  sort those lists and responsible or missing activity IDs lexicographically.
- Definition names are the selected YAML versions, such as `v2` and `v3`.
  Cohort floors come from those definitions in USD, not cents. Do not change a
  released definition to construct your answer.

## Yield and invoice units

Use the bridge receipt's `result.units` for both `july_yield.units` and
`late_invoice.units`. They are objects, with the following exact values:

| Unit field | Exact string |
|---|---|
| `tokens` | `tokens` |
| `net_revenue_cents`, `channel_fee_cents`, `revenue_before_channel_fees_cents` | `USD cents` |
| `net_of_discounts_and_fees_per_mtok`, `net_of_discounts_per_mtok` | `USD/Mtok` |

`july_yield.revenue_basis` is the bridge's complete policy label:

```text
Recognized usage revenue, net of discounts and marketplace fees; alternative before marketplace fees also shown
```

`july_yield.denominator` is the bridge's complete denominator label:

```text
Raw processed tokens: input, output, cache_read and cache_write, each counted once with unit weight
```

Sum the observed cents and raw tokens before calculating each yield. The labels
do not replace that calculation or the receipt replay.

## Replay, attribution and NRR

| Answer field | Exact string | Evidence required |
|---|---|---|
| `july_yield.replay_status`, `nrr_definition.old_replay_status` | `reproduced` | The selected original receipt re-performs successfully, with `verified: true` and `recomputed: true`; reading a saved seal is insufficient. |
| `late_invoice.attribution_status` | `isolated_source_append` | The controlled bridge and inspected source append identify the responsible activity, its signed effect and the unchanged token denominator. This is the exercise's attribution label, not a literal CLI status. |
| `nrr_definition.source_and_cutoffs` | `identical` | The two NRR receipts have identical `inputs`, `asof`, `known_at` and `watermark`; only the selected NRR definition changes. |

`nrr_definition.units` is one string:

```text
revenue amounts in USD cents; NRR and shares are ratios; cohorts are counts
```

## Close status and retry

The historical gate and the retry report different kinds of status. Read the
gate's decision for readiness and the retry's publication result for idempotence.

| Answer field | Exact string | Evidence required |
|---|---|---|
| `failed_close.before_status` | `held` | The original watermark fails source reconciliation; publication is held (CLI exit 2). |
| `failed_close.after_status` | `ready` | The recovered source passes the gate. This is the gate decision, not the publication result. |
| `failed_close.retry_publication` | `already_published` | The bounded retry reports an existing publication; compare the publication bytes and inspect its inserted count. |

`failed_close.retry_basis` describes the retained observation behind that retry:

```text
retained writer observation; identical frozen source and publication on retry
```

`failed_close.units` is one string:

```text
amounts in USD cents; source observations and inserted activities are counts
```

Sum `expected_count`, `actual_count`, `expected_cents` and `actual_cents` across
the corresponding gate's `reconciliation.periods` for `before` and `after`.
Get missing IDs from its exceptions. The fixture already contains the recovered
source; an idempotent retry does not establish a new collection.

These are the frozen `tokenledger-challenge-answer/v1` spellings, checked against
the [task contract](../../tl/challenge/engine.py) and
[yield bridge](../../tl/demo/yield_bridge.py). You do not need to read those
implementations to submit through the learning route. The
[visible solution route](../assess.md#verify-the-reference) remains separately
labeled; record any use of it.
