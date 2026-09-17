# Apply the reporting discipline to another business

tokenledger is a runnable reference implementation for data and finance
engineering teams. The synthetic example demonstrates a reporting process and
its evidence. An adopting team supplies the source contracts and business policy
that make that process authoritative in its own setting.

| Reuse | Adapt to the business | Accountable owner |
|---|---|---|
| Fourteen-column activity envelope and validated append path | Source identifiers, idempotency keys, activity definitions and admission checks | Source-system owner and integration engineer |
| Knowledge-filtered snapshots | Event dates, arrival dates, reporting cutoffs and correction policy | Reporting owner |
| Customer timelines | Legal entities, billing accounts, workspaces, end customers, parent history and mergers | Commercial-data owner |
| Versioned calculations and definitions | Revenue policy, account mapping, exclusions, cohorts and denominator rules | Accounting-policy owner and metric owner |
| Receipts, bridges and original-result replay | Retention, access, source reconciliation and publication approval | Reporting publisher and control owner |

The [physical schema](../tl/stream/schema.sql) stays common. Provider-specific
facts belong in validated features with their actual meaning. A workspace is
not automatically a legal customer; a provisioned seat is not automatically a
paid seat. Define those relationships before assigning revenue or cohorts.

## Admit a source before relying on its numbers

For each source, agree the period covered, unique event identity, expected count
and amount, evidence retention and handling of missing or corrected records.
Record both event time and arrival time. Reconcile the received population to
the source control total. Keep a discrepancy visible and hold publication until
its owner resolves it or follows an explicitly approved exception policy.

The [failed-close exercise](workflow.md#4-investigate-a-held-close) demonstrates
that boundary on synthetic source evidence. A receipt reproduces admitted facts
under a pinned policy. It does not establish source accuracy or approve the
policy. Production identity, permissions, independent approval and source
coverage must be tested in the adopting environment.

## Replace one output with a reversible migration

Start with one monthly output, such as recognized usage revenue by account.
Keep the incumbent publication path active during parallel operation.

1. **Agree the contract.** The reporting owner fixes grain, units, cutoffs,
   reconciliation tolerances and close deadline. The accounting owner approves
   the recognition policy; the engineer records source mappings.
2. **Run both paths.** Admit the same bounded source population and compare every
   output row. Reconcile totals and explain differences by activity ID. Include
   a late arrival and a policy-version change in the trial.
3. **Hold on an unexplained difference.** Assign the discrepancy to a named
   owner. Escalate before the agreed deadline. The incumbent remains the
   publication source while the discrepancy is unresolved.
4. **Accept one output.** The reporting owner reviews the tie-out, original
   replay and recovery evidence. Record the exact versions and approval before
   switching that output's publication route.
5. **Keep rollback practical.** Retain the incumbent output and input cutoffs.
   Revert the publication route if reconciliation or access controls fail.
   Append corrections; preserve the held package and prior publication.

An engineer should be able to rebuild from a clean checkout; an analyst should
be able to find the discrepancy; a reporting owner should be able to accept or
hold the package. Measure their operating time and the maintenance burden in
the pilot. This example does not estimate an integration schedule for an
unexamined estate.

## Recognition scope

The [synthetic accounting policy](../definitions/accounting/v1.yaml) exercises
commit drawdown, subscriptions, marketplace fees and credits. It is not an
accounting opinion. Contract modifications, standalone selling-price allocation,
variable consideration, tax, foreign exchange and collections need appropriate
business-specific treatment before adoption.

The [comparison evidence](evidence.md) supports equivalence for the registered
synthetic outputs. BigQuery used **51.47% fewer billed bytes** and **14.96 times
median slot time**. Test a representative workload before making a performance
or cost decision. Comparative agent efficiency remains unmeasured.

[Run the example](run.md) Â· [Follow the workflow](workflow.md) Â· [Inspect the architecture](architecture/README.md)


Comparison qualification: warmup is excluded from performance statistics; native
was slower in the first measured pair. Accounted wall sums disjoint intervals,
not contiguous latency. List-price arithmetic is not an invoice. Grok QA is
agent-assisted engineering review, not an accounting audit.
