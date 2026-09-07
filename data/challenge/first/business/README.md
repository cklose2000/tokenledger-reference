# July recognized usage revenue per million tokens

Recognized usage revenue, net of discounts and marketplace fees; alternative before marketplace fees also shown.

Raw processed tokens: input, output, cache_read and cache_write, each counted once with unit weight.

| Measure | Original | Currently known | Delta | Unit | Receipt |
|---|---:|---:|---:|---|---|
| tokens | 19210131257 | 19210131257 | 0 | tokens | mr-0717aa61198b3e3b48ee8eb6bd4ae39d |
| net_revenue_cents | 3054374 | 3354374 | 300000 | USD cents | mr-0717aa61198b3e3b48ee8eb6bd4ae39d |
| channel_fee_cents | 3607 | 3607 | 0 | USD cents | mr-0717aa61198b3e3b48ee8eb6bd4ae39d |
| revenue_before_channel_fees_cents | 3057981 | 3357981 | 300000 | USD cents | mr-0717aa61198b3e3b48ee8eb6bd4ae39d |
| net_of_discounts_and_fees_per_mtok | 1.589980807074 | 1.746148402176 | 0.156167595102 | USD/Mtok | mr-0717aa61198b3e3b48ee8eb6bd4ae39d |
| net_of_discounts_per_mtok | 1.591858462126 | 1.748026057228 | 0.156167595102 | USD/Mtok | mr-0717aa61198b3e3b48ee8eb6bd4ae39d |

Sum recognized revenue and sum tokens across disjoint cuts; never average per-cut yields. 12 decimal places, half up; delta computed from exact revenue change and unchanged token denominator.

The tokens were already present. One newly admitted invoice increases recognized revenue. Debit and credit entries explain that single revenue change; they are not two changes in revenue.

This directory owns its source database, saved populations, compiled queries, receipts and original disclosure pack. No credentials or private application data were used.

The original July report is a deliberately delayed close, known through October 1. The synthetic missing invoice arrives October 2. That choice isolates its effect from the generator's ordinary late arrivals.

## Account bridge

Changed account balances only; omitted accounts are unchanged. Debit positive, credit negative.

| Month | Account | Before cents | After cents | Delta cents | Receipt |
|---|---|---:|---:|---:|---|
| 2026-07-01 | 1100 | 2336949 | 2636949 | 300000 | mr-e578e74a79235bd4e4bd8e89bde28a51 |
| 2026-07-01 | 4000 | -3057981 | -3357981 | -300000 | mr-e578e74a79235bd4e4bd8e89bde28a51 |

Responsible activity: `demo:late-july:invoice`. Identical policy and economic cutoff; full source-prefix equality; exactly one newly visible invoice.

All original KPI populations and the original close were freshly reproduced after the late append. Timing is an observation, not a reproducible financial result.

Workflow observation: 9.585 seconds; receipt `mr-ee7b35736990f94477e6ac25c017ea1a`.

Replay from the repository with the retained source and artifact root:

```powershell
tl --db '/workspace/release/data/challenge/first/business/world.duckdb' --artifact-root '/workspace/release/data/challenge/first/business' receipt mr-0717aa61198b3e3b48ee8eb6bd4ae39d --json
tl --db '/workspace/release/data/challenge/first/business/world.duckdb' --artifact-root '/workspace/release/data/challenge/first/business' receipt mr-26b0456b253fc635ea910ce4d21db2af --json
```

The original disclosure pack is under `pack/`. All reporting populations, including the current July, are under `runs/`. This small functional demo supplies no new architecture speed ranking, agent-efficiency claim or BigQuery result.
