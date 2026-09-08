# Hosted Codespaces verification

September 8, 2026. Builder observation, not a first-time evaluator session or
independent QA. Tested public candidate
`e41438dae69ca15e8d05de6f75f80e804e89d685`; later documentation revisions preserve
that executable and its retained history.

An actual GitHub Codespace provisioned the checked-in container in US East on
two cores, 8 GB RAM and 32 GB storage. Python was 3.11.5; installed reporting
packages matched the release pins. The CLI connection used a dedicated test
SSH key. No learning approval key or provider credentials were used for the
application checks.

| Check | Result |
|---|---|
| Demo | Passed; late invoice, unchanged tokens, signed accounting bridge and original replay |
| Historical challenge | Verified with fresh re-performance |
| Current challenge | Verified with fresh re-performance |
| Visible-answer grading | All four tasks passed; same deterministic correctness receipt as the retained reference |
| Recording's original number | Verified by recomputing the retained population |
| Saved cloud observation | Seal verified; `recomputed: false`, no large download or cloud SQL |
| Resource cleanup | Codespace stopped; GitHub API confirmed `Shutdown` |

The demo's workflow took **6.942 seconds**; its process including startup took
**7.613 seconds**. These are one hosted run's observations after installation,
not a provisioning time, repeatability guarantee or comparison with another
machine. The [structured observation](codespaces.json) contains command clocks,
output hashes, runtime pins and reviewed outcomes.

## Observations retained

GitHub CLI's `create --status` helper and its log helper could not authenticate
with their automatically selected SSH identity. The environment itself was
available. An explicit dedicated test key connected and all application checks
passed. No end-to-end provisioning-time claim is made from that failed helper.

Grading directly in the retained fixture appended a timing receipt to
`data/challenge/rc5/metrics.jsonl`. This is the grader's documented append-only
observation behavior. The agent instructions now make a working copy first;
that recipe was separately tested and preserved the original fixture ledger.
The application and grading rules were unchanged.

The setup check uses the hosted container through GitHub CLI. The web-editor
interaction and actual uncoached participant observations are separate from
this check. General production admission, comparative agent efficiency and
successful learning promotion are not established here.
