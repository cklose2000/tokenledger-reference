# July demo, frozen audience execution

Execution: `d83e34f27cdb0f4b19593e0711fd11b897da92c2`, successor of unpublished
rc.6 `3272f68`. [GIF](demo.gif), [asciinema](demo.cast), [transcript](demo.txt),
[capture metadata](recording.json), [render hashes](render.json).

The silent 54.99-second GIF renders actual timestamped stdout/stderr with no
speed-up, narration or overlays. Process time was 14.453 seconds; workflow
time was 10.741 seconds. A 40.547-second final reading hold is included. GIF
durations are quantized to centiseconds. This is pipe capture, not a PTY.
Python 3.11.5; dependency install 46.676 seconds, separately measured. Other
regression work shared the host. These are observations, not a cold-start SLA.

The released command generates, reports, appends the late invoice, verifies
both bridges, replays the original and renders its pack. The pack is rendered
after replay in the actual command; this recording does not rearrange stages.
The generic `C:\tl-audience-20260915` path is the dedicated synthetic checkout.

The retained run is `data/demo/audience`. From this checkout:

```sh
export PYTHONDONTWRITEBYTECODE=1 PYTHONHASHSEED=0
tl --db data/demo/audience/world.duckdb --artifact-root data/demo/audience receipt mr-16259514a4d2d12bcdc5b11ad84a7209 --json
tl --db data/demo/audience/world.duckdb --artifact-root data/demo/audience receipt mr-51c0e7c0f615971e1ffe691424d18695 --json
```

PowerShell: set `$env:PYTHONDONTWRITEBYTECODE='1'` and
`$env:PYTHONHASHSEED='0'` before those same `tl` commands. The first receipt
recomputes the original yield population; the second recomputes the business
bridge. Timing observation `mr-476fde1c5dcc249f9394cc0828e30e47` verifies a
retained measurement and does not reproduce elapsed seconds.
