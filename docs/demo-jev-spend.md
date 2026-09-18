# Jev token economics demonstration

`tl demo-jev` admits one OpenRouter Decisions call's **rates and token mix**, then a worked September close the native engine can express in integer cents.

The live probe is **$0.000012894** (307 input tokens at $0.042/Mtok, 20 output tokens free). That is 0.0012894 USD cents, so the close uses **1,000,000 input tokens** at the same prices ($42.00 list). A later invoice bills the same batch at **$0.084/Mtok** ($84.00). Tokens do not change. The original receipt still reproduces.

```sh
tl demo-jev
```

The destination is a new `data/demo-jev/<run>/` directory. It never writes the repository metric ledger or any API key. Probe facts live in [`examples/jev-openrouter-probe.json`](examples/jev-openrouter-probe.json) without account identifiers.

This is the same late-evidence pattern as [`demo.md`](demo.md): economic period September, invoice known in October. No model, billing or TypeSafe console credentials are required. A live OpenRouter call is optional and separate.
