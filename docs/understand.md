# Follow one number

The example reports July recognized usage revenue per million tokens. It shows
the numerator in USD cents, the raw token denominator and two fee presentations:
net of discounts, and net of discounts plus marketplace fees. Recognition policy
determines which economic events supply that revenue.

The fourteen-column activity stream preserves event time and recorded time
separately. A July invoice arriving later can change what is currently known
about July while the earlier reporting snapshot remains reproducible. The writer
validates the event; the snapshot applies its knowledge cutoff before deriving
occurrence and repeated-at values.

Typed logical projections and time-based joins connect usage, invoices,
contracts, customer relationships and compute. The measured native reporting
path computes this logic at query time. One physical source does not mean one
unit of calculation, one SQL operation or no logical dependencies.

Versioned definitions fix the population, formula, exclusions and units. A
receipt records the definition version, source identity, cutoffs, query hash and
execution version. Replaying it means using that recorded information again.

The late invoice raises the eligible recognized-revenue numerator without
changing the demo's token denominator. Its bridge also shows the corresponding
accounting movements. The final replay returns the original result even though
later evidence now exists.

Receipts establish a reproducible calculation. Source reconciliation and close
controls address different questions. The fourth challenge deliberately holds a
close when its source totals disagree, preserves the discrepancy and requires a
bounded recovery before publication.

[Read the architecture field guide](architecture/README.md), which includes
the standalone HTML opening instructions and [JSON](architecture/architecture.json).
The detailed guide labels measured scope and proposed extensions separately.

A report can also be inspected wrongly. [The learning loop](learning.md) shows
how such a finding becomes a verified case, a bounded candidate, a signed
one-run trial and a recorded outcome without editing a definition, a number or
a control.

Next: [operate the example](run.md) or [inspect its evidence](evidence.md).
