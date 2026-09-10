# Maintain the public presentation

From the pinned editable installation, run:

```sh
python docs/presentation/build.py
python docs/presentation/build.py --check
```

This builder changes presentation files only. It performs no data ingestion,
learning promotion or warehouse job, and does not mutate the retained fixtures.

`demo.template.html` contains the original captured console events and a marker
for a recorded-run summary. The builder verifies the yield/account receipt
seals, derives display values from their retained results, loads account labels
from the accounting policy, and generates `docs/demo.html`, the Markdown table
and `docs/assets/demo-summary.json`. Decimal formatting affects display only.
Replay commands in `docs/demo.md` perform the underlying calculations.

`archify-upstream.html` preserves the exact original Archify export, covered by
`docs/architecture/artifact-receipt.json` and the adjacent upstream MIT license.
The builder requires that export's hash before applying `architecture-reader.css`
and `architecture-reader.js`. The transform changes viewer controls and their
layout. It preserves the authored specification, SVG geometry, topology and
canonical export machinery. A separate `reader-receipt.json` binds its inputs
and final HTML; the upstream receipt is not claimed as validation of new bytes.

The blank answer and JSON schema derive **field structure only** from the frozen
reference contract. Placeholder strings are deliberately invalid; no business
values are prefilled. This adds a guided workflow route (the reader exercise, not
the reporting-learning loop) without changing the executable
grader, its answer visibility or its tolerances.

After a presentation edit, use a real browser. Check playback, seek-back from
the final frame, copy/fallback, exact transcript, chapter text, node selection,
theme and keyboard dismissal. Reproduce the scrolled 1280×720 finder at 125%
diagram zoom. Check desktop and narrow-window containment. Bind observations
to the generated HTML hash, and distinguish automated measurements from visual
inspection. The original capture and prior review stay in Git history.
