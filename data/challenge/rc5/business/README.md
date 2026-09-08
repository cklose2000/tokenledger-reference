# July recognized usage revenue per million tokens

Recognized usage revenue, net of discounts and marketplace fees; alternative before marketplace fees also shown.

Raw processed tokens: input, output, cache_read and cache_write, each counted once with unit weight.

| Measure | Original | Currently known | Delta | Unit | Receipt |
|---|---:|---:|---:|---|---|
| tokens | 19210131257 | 19210131257 | 0 | tokens | mr-02d2a7a2231d6a46e0291a046704763e |
| net_revenue_cents | 3054374 | 3354374 | 300000 | USD cents | mr-02d2a7a2231d6a46e0291a046704763e |
| channel_fee_cents | 3607 | 3607 | 0 | USD cents | mr-02d2a7a2231d6a46e0291a046704763e |
| revenue_before_channel_fees_cents | 3057981 | 3357981 | 300000 | USD cents | mr-02d2a7a2231d6a46e0291a046704763e |
| net_of_discounts_and_fees_per_mtok | 1.589980807074 | 1.746148402176 | 0.156167595102 | USD/Mtok | mr-02d2a7a2231d6a46e0291a046704763e |
| net_of_discounts_per_mtok | 1.591858462126 | 1.748026057228 | 0.156167595102 | USD/Mtok | mr-02d2a7a2231d6a46e0291a046704763e |

Sum recognized revenue and sum tokens across disjoint cuts; never average per-cut yields. 12 decimal places, half up; delta computed from exact revenue change and unchanged token denominator.

The tokens were already present. One newly admitted invoice increases recognized revenue. Debit and credit entries explain that single revenue change; they are not two changes in revenue.

This directory owns its source database, saved populations, compiled queries, receipts and original disclosure pack. No credentials or private application data were used.

The original July report is a deliberately delayed close, known through October 1. The synthetic missing invoice arrives October 2. That choice isolates its effect from the generator's ordinary late arrivals.

## Account bridge

Changed account balances only; omitted accounts are unchanged. Debit positive, credit negative.

| Month | Account | Before cents | After cents | Delta cents | Receipt |
|---|---|---:|---:|---:|---|
| 2026-07-01 | 1100 | 2336949 | 2636949 | 300000 | mr-d83e9f8e5685b082c84ba7480625c9ca |
| 2026-07-01 | 4000 | -3057981 | -3357981 | -300000 | mr-d83e9f8e5685b082c84ba7480625c9ca |

Responsible activity: `demo:late-july:invoice`. Identical policy and economic cutoff; full source-prefix equality; exactly one newly visible invoice.

All original KPI populations and the original close were freshly reproduced after the late append. Timing is an observation, not a reproducible financial result.

Workflow observation: 21.330 seconds; receipt `mr-aacd1b7aa3e96671fa56dc63b279392a`.

Replay from the repository with the retained source and artifact root:

```powershell
tl --db 'C:\tokenledger-demo\rc5-825696c\data\challenge\rc5\business\world.duckdb' --artifact-root 'C:\tokenledger-demo\rc5-825696c\data\challenge\rc5\business' receipt mr-02d2a7a2231d6a46e0291a046704763e --json
tl --db 'C:\tokenledger-demo\rc5-825696c\data\challenge\rc5\business\world.duckdb' --artifact-root 'C:\tokenledger-demo\rc5-825696c\data\challenge\rc5\business' receipt mr-002588b5125d5f0817519246fef0e818 --json
```

The original disclosure pack is under `pack/`. All reporting populations, including the current July, are under `runs/`. This small functional demo supplies no new architecture speed ranking, agent-efficiency claim or BigQuery result.
