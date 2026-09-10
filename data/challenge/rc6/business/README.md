# July recognized usage revenue per million tokens

Recognized usage revenue, net of discounts and marketplace fees; alternative before marketplace fees also shown.

Raw processed tokens: input, output, cache_read and cache_write, each counted once with unit weight.

| Measure | Original | Currently known | Delta | Unit | Receipt |
|---|---:|---:|---:|---|---|
| tokens | 19210131257 | 19210131257 | 0 | tokens | mr-2981baeceb28ceb01296108b43dcd725 |
| net_revenue_cents | 3054374 | 3354374 | 300000 | USD cents | mr-2981baeceb28ceb01296108b43dcd725 |
| channel_fee_cents | 3607 | 3607 | 0 | USD cents | mr-2981baeceb28ceb01296108b43dcd725 |
| revenue_before_channel_fees_cents | 3057981 | 3357981 | 300000 | USD cents | mr-2981baeceb28ceb01296108b43dcd725 |
| net_of_discounts_and_fees_per_mtok | 1.589980807074 | 1.746148402176 | 0.156167595102 | USD/Mtok | mr-2981baeceb28ceb01296108b43dcd725 |
| net_of_discounts_per_mtok | 1.591858462126 | 1.748026057228 | 0.156167595102 | USD/Mtok | mr-2981baeceb28ceb01296108b43dcd725 |

Sum recognized revenue and sum tokens across disjoint cuts; never average per-cut yields. 12 decimal places, half up; delta computed from exact revenue change and unchanged token denominator.

The tokens were already present. One newly admitted invoice increases recognized revenue. Debit and credit entries explain that single revenue change; they are not two changes in revenue.

This directory owns its source database, saved populations, compiled queries, receipts and original disclosure pack. No credentials or private application data were used.

The original July report is a deliberately delayed close, known through October 1. The synthetic missing invoice arrives October 2. That choice isolates its effect from the generator's ordinary late arrivals.

## Account bridge

Changed account balances only; omitted accounts are unchanged. Debit positive, credit negative.

| Month | Account | Before cents | After cents | Delta cents | Receipt |
|---|---|---:|---:|---:|---|
| 2026-07-01 | 1100 | 2336949 | 2636949 | 300000 | mr-eae0fe52703fd981caf99edb1a260ebc |
| 2026-07-01 | 4000 | -3057981 | -3357981 | -300000 | mr-eae0fe52703fd981caf99edb1a260ebc |

Responsible activity: `demo:late-july:invoice`. Identical policy and economic cutoff; full source-prefix equality; exactly one newly visible invoice.

All original KPI populations and the original close were freshly reproduced after the late append. Timing is an observation, not a reproducible financial result.

Workflow observation: 11.786 seconds; receipt `mr-4fa64f3d50efd38de3ed7e2f3dd7e65c`.

Replay from the repository with the retained source and artifact root:

```powershell
tl --db 'C:\tokenledger-demo\rc6-933cd94\data\challenge\rc6\business\world.duckdb' --artifact-root 'C:\tokenledger-demo\rc6-933cd94\data\challenge\rc6\business' receipt mr-2981baeceb28ceb01296108b43dcd725 --json
tl --db 'C:\tokenledger-demo\rc6-933cd94\data\challenge\rc6\business\world.duckdb' --artifact-root 'C:\tokenledger-demo\rc6-933cd94\data\challenge\rc6\business' receipt mr-df756c6c3ed30dc9a1a189b490030e63 --json
```

The original disclosure pack is under `pack/`. All reporting populations, including the current July, are under `runs/`. This small functional demo supplies no new architecture speed ranking, agent-efficiency claim or BigQuery result.

## Next: the feedback loop inside reporting

Reports like this one can be inspected wrongly. `tl learning walkthrough` rehearses the governed learning loop on a fresh synthetic July NRR report: a wrong-date inspection becomes a recorded finding, an independent verification, a bounded diagnostic candidate evaluated against frozen cases, a signed one-run approval boundary, the first next inspection and its recorded outcome, with the original report reproduced afterwards. The walkthrough is synthetic and credential-free; the one real native trial completed inconclusive with no promotion.
