# Run the reporting example

From a checkout of this release, create an isolated Python 3.11.5 environment.
Keep the checkout: definitions, SQL and retained execution history are part of
the runnable reference implementation.

Use a Git clone, rather than a source ZIP, so the retained receipt history is
available. On Windows, follow the short-path clone below. On macOS or Linux,
for the private draft rc.6 base (3272f68):

```sh
git clone https://github.com/cklose2000/tokenledger-reference.git
cd tokenledger-reference
git checkout --detach 3272f680ff945daa7639bd3aad06b28659b5c9c7
```

For successor review, use the exact SHA in the approval packet. Draft rc.6 has
no published tag; these commands pin its retained base. Publication and hosted setup status are in
[release status](../release-status.json).

On Windows PowerShell, choose a new, short destination such as `C:\tl-reference`
and enable long paths for this clone before checkout. The retained receipt
history contains nested paths; a deeply nested workspace can otherwise fail
with `Filename too long`.

```powershell
git clone --config core.longpaths=true https://github.com/cklose2000/tokenledger-reference.git C:\tl-reference
Set-Location C:\tl-reference
git checkout --detach 3272f680ff945daa7639bd3aad06b28659b5c9c7
```

During private review, replace the tag with the supplied exact commit. The clone
option stores the setting in this repository before files are checked out; it
does not change global Git or Windows policy. Keep the path short for Python and
other tools too. See [Git for Windows' long-path guidance](https://gitforwindows.org/git-cannot-create-a-file-or-directory-with-a-long-path.html)
and [Git's clone configuration option](https://git-scm.com/docs/git-clone).
If a previous clone failed, preserve it and use a new destination for this retry.

Then install and run from Windows PowerShell:

```powershell
python --version
$env:PYTHONDONTWRITEBYTECODE='1'
$env:PYTHONHASHSEED='0'
python -m venv .venv
.\.venv\Scripts\python.exe --version
.\.venv\Scripts\python.exe -m pip install -e '.[dev]'
.\.venv\Scripts\tl.exe demo
```

On macOS or Linux:

```sh
python3.11 -m venv .venv
. .venv/bin/activate
export PYTHONDONTWRITEBYTECODE=1 PYTHONHASHSEED=0
python --version
python -m pip install -e ".[dev]"
tl demo
```

Confirm Python **3.11.5** for the frozen historical receipts. `python` and
`python3.11` select an installed interpreter; they do not install or guarantee a
patch version. The checked-in devcontainer pins the reference interpreter and
base image. Hosted provisioning is a separately recorded acceptance check.

## Use GitHub Codespaces

**Successor candidate: not_run.** The linked hosted observation below applies
to `e41438d`, not rc.6 or this successor. An actual instance must install and
operate the exact frozen successor before this gate passes.

On the repository's `main` branch, choose **Code > Codespaces > Create codespace**.
The two-core machine is sufficient for this example. Wait for the post-creation
package installation to finish, then use the terminal:

```sh
python --version
tl demo
```

The checked-in container supplies Python 3.11.5 and installs the package.
[Actual hosted verification](verification/codespaces.md) passed the demo,
challenge and retained receipt replay on a two-core, 8 GB Codespace in US East.
GitHub login and access to this repository are required; the review candidate
remains private. GitHub displays who pays for the environment during creation.
[GitHub's creation instructions](https://docs.github.com/en/codespaces/developing-in-a-codespace/creating-a-codespace-for-a-repository).

When finished, stop the environment from [Your codespaces](https://github.com/codespaces).
Closing a browser tab leaves it running until its idle timeout.
[GitHub's stop instructions](https://docs.github.com/en/codespaces/developing-in-a-codespace/stopping-and-starting-a-codespace).

## What the command does

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

## Run the feedback loop

```sh
tl learning walkthrough
tl learning replay-walkthrough data/learning-walkthrough/<run> --json
```

The first command creates `data/learning-walkthrough/<run>/`, reports July NRR
with a receipt, records a wrong-date finding and runs the governed loop to its
recorded outcome in a few seconds. It needs OpenSSH's `ssh-keygen` on PATH to
create and verify its disposable signer; Windows, macOS and Linux ship it. The
second command re-validates that directory and fails on any changed byte.
[What the labels mean](learning.md).

Next: [follow the number](understand.md) or [run the agent challenge](assess.md).
