# tokenledger: one activity stream, every reported number reproducible.

The data model agents actually work on.

Built from the SOX-controlled subscriber-metrics work I ran at SiriusXM for 19 years.
For AI businesses that meter tokens and seats.

This is a locally prepared release candidate. Publication, a matched-agent
accuracy benchmark and native BigQuery acceptance have separate gates.

```sh
python -m pip install -e ".[dev]"
tl demo
```

The demo generates an isolated synthetic business, reports July recognized
usage revenue per million tokens, admits a late invoice, prints the change and
reproduces the original result. It states both fee bases and the raw token
denominator. Exact amounts and receipt IDs come from your run.

Give this repo to your agent. Reproduce a number, explain a late-event bridge,
change a definition, and print the scorecard. Start with [AGENTS.md](AGENTS.md).

[Open in GitHub Codespaces](https://codespaces.new/cklose2000/tokenledger?ref=v0.2.0-rc.4)

The launch link requires a published repository/tag and GitHub access. The
prepared environment pins its image and engine dependencies. Installation and
hosted provisioning are separate from the credential-free demo runtime.

## Limits and evidence

One canonical source table does not mean one unit of logical work. Native
reporting uses typed projections, joins, windows and query-local buffers;
it stores no derived reporting tables. The independently implemented dbt
baseline is included so equivalent outputs and actual work can be measured.

Run the challenge to generate a receipt-backed scorecard. This is an open-book
reproduction exercise with visible expected outputs, not an unseen-task agent
accuracy benchmark. Model efficiency is unmeasured until paired trials with
actual token accounting are published. Synthetic results do not establish
any named company's reporting quality. Native cloud results require real jobs.

Retained receipts establish reproducibility, not source completeness, accounting
policy approval, authenticated role separation or an audit opinion. The failed
close exercise shows why a source check can hold publication.

No personal usage data, credentials, private receipt ledgers or original private
Git history are included. Generic source adapters in the implementation are
inactive without an explicitly bound application; they are not public feature
claims. See [RELEASE.md](RELEASE.md) for preparation provenance and verification.
