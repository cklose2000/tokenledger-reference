# Run the reporting example

From a checkout of this release, create an isolated Python 3.11.5 environment.
Keep the checkout: definitions, SQL and retained execution history are part of
the runnable reference implementation.

Use a Git clone, rather than a source ZIP, so the retained receipt history is
available. For the published release:

```sh
git clone https://github.com/cklose2000/tokenledger-reference.git
cd tokenledger-reference
git checkout v0.2.0-rc.5
```

During candidate review, use the exact supplied candidate commit instead of
the release tag. Publication and hosted setup status are in
[release status](../release-status.json).

On Windows PowerShell:

```powershell
python --version
python -m venv .venv
.\.venv\Scripts\python.exe --version
.\.venv\Scripts\python.exe -m pip install -e '.[dev]'
.\.venv\Scripts\tl.exe demo
```

On macOS or Linux:

```sh
python3.11 -m venv .venv
. .venv/bin/activate
python --version
python -m pip install -e ".[dev]"
tl demo
```

Confirm Python **3.11.5** for the frozen historical receipts. `python` and
`python3.11` select an installed interpreter; they do not install or guarantee a
patch version. The checked-in devcontainer pins the reference interpreter and
base image. Hosted provisioning is a separately recorded acceptance check.

The command creates a new isolated synthetic database under `data/demo/`. It
prints July's recognized revenue per million tokens on both marketplace-fee
bases, admits designated late evidence, explains the signed accounting bridge
and replays the original receipt. It prints the receipt IDs and the location
of a run-specific README. Each invocation uses a fresh directory.

For machine-readable output, run `tl demo --json`.

Generation, reporting, the bridge and replay are included in the workflow time.
Installation and hosted provisioning are separate. The under-60-second target
concerns the installed reference runtime, not every laptop or a cold install.
The release recording identifies its actual environment and observations.

The demo needs no provider credentials and makes no model call. The optional
BigQuery evidence package has separate download and verification costs; running
this demo does not download it or submit cloud jobs.

Next: [follow the number](understand.md) or [run the agent challenge](assess.md).
