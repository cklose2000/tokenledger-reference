"""Offline, allowlisted release preparation. Never publishes or copies Git history."""
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
import hashlib, json, os, re, subprocess

import click

from tl.stream import ValidationError
from tl.stream.events import canonical

IMAGE = 'python:3.11.5-slim-bookworm@sha256:edaf703dce209d774af3ff768fc92b1e3b60261e7602126276f9ceb0e3a96874'
TESTS = {'conftest.py','test_stream.py','test_generator.py','test_allocation_binding.py',
         'test_demo.py','test_challenge.py','test_reporting_learning_diagnostic.py',
         'test_reporting_learning_walkthrough.py'}


def selected(name):
    path = PurePosixPath(name)
    if path.name in {'profiles.yml','profiles.yaml'} or path.name.startswith(('credentials','service-account','.env')):
        return False
    if name in {'LICENSE','pyproject.toml','.gitignore'}:
        return True
    if path.parts[0] in {'tl','definitions','scaffolds','requirements','controls'}:
        return path.suffix in {'.py','.sql','.yaml','.yml','.json','.txt'}
    if path.parts[0] == 'baseline':
        return path.suffix in {'.sql','.yaml','.yml','.txt'} and not set(path.parts)&{'target','logs','dbt_packages'}
    return path.parts[0] == 'tests' and len(path.parts) == 2 and path.name in TESTS


def group(name):
    if name in {'LICENSE','pyproject.toml','.gitignore','tl/__init__.py','tl/__main__.py'} or name.startswith(('tl/stream/','definitions/activities/')):
        return 0
    if name.startswith(('tl/generate/','definitions/')):
        return 1
    if name.startswith(('baseline/','tl/compare/')):
        return 3
    if name.startswith(('tests/','tl/demo/','tl/challenge/')) or name == 'tl/release.py':
        return 4
    return 2


def git(args, *, cwd=None, data=None):
    result = subprocess.run(['git',*args], cwd=cwd, input=data, capture_output=True, check=False)
    if result.returncode:
        raise ValidationError('release Git operation failed: '+result.stderr.decode('utf-8',errors='replace').strip())
    return result.stdout


def templates(repository, tag, day):
    owner_repo = repository.removeprefix('https://github.com/')
    readme = f'''# tokenledger: one activity stream, every reported number reproducible.

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

[Open in GitHub Codespaces](https://codespaces.new/{owner_repo}?ref={tag})

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
'''
    agents = '''# Reproduce and explain the reporting result

Work only on synthetic fixtures created under a new local data directory. Never
read local personal accounts, configure cloud access or publish this repository.

Install the pinned package from this checkout, then run `tl demo --json`.
Run `tl challenge --help` for the release's exact preparation and grading
commands. The generated challenge manifest supplies concrete source/receipt
identities, reporting and knowledge cutoffs, units and visible expected outputs.

Complete every registered task:

1. Reproduce July recognized usage revenue per million tokens. State the fee
   basis, the raw token denominator and the original receipt.
2. Explain the late invoice with its exact signed revenue delta, responsible
   activity and unchanged token count. Replay the original after the append.
3. Select immutable NRR v3, which raises the headline floor to USD 100,000.
   Identify changed and unchanged lenses at the same source/knowledge cutoffs,
   then reproduce the old version. Never edit a released definition in place.
4. Demonstrate the failed-source close, its held publication and bounded source
   recovery. Keep the discrepancy visible; do not relax the check.

Submit the required answer fields to the application grader and print its
scorecard. Your own PASS statement is not verification. Wrong answers, missing
tasks or changed evidence must fail. The exercise is open-book; it makes no
claim of independent QA or held-out agent accuracy. Keep unknown token usage
unknown rather than estimating it.
'''
    dockerfile = f'''FROM {IMAGE}
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates && rm -rf /var/lib/apt/lists/*
ENV PYTHONUTF8=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /workspace
'''
    return {
        '.gitattributes':'''* text=auto eol=lf
*.parquet binary
*.duckdb binary
*.png binary
*.pdf binary
# Frozen source and receipt artifacts retain exact bytes on every platform.
data/challenge/** -text
''',
        'README.md':readme,'AGENTS.md':agents,
        'RELEASE.md':f'''# Release preparation

Prepared {day} as candidate `{tag}`. These commits reconstruct reviewable groups
from an existing implementation at preparation time; they do not claim an
earlier public development timeline. Intermediate assembly commits are not
individually runnable releases. No original private Git objects are copied.

The allowlist includes generic runtime, definitions, SQL, independent baseline
and selected synthetic regression tests. It excludes all original ledgers,
generated reports, personal evidence, private application configuration, hiring
strategy, private module plans and working development instructions.

Source file bytes are selected from committed Git blobs, never arbitrary working
tree contents. Release receipt fixtures must be regenerated under these public
revisions, and then verified from an isolated clone of this repository. Original
private receipt IDs and private SHAs are not public reproduction evidence.

The image pins the Python version used by retained local replay. Image changes
need fresh dependency, semantic and replay verification. See the image digest
in `.devcontainer/Dockerfile`; see the package for exact engine pins.

Verification statuses start as not_run. Populate a verification artifact from
actual commands before claiming a clean installation, hosted environment,
challenge score, timing, agent comparison or cloud acceptance. This preparation
command performs no publication, account access or cloud operation.
''',
        'CHANGELOG.md':f'# Changelog\n\n## {tag} ({day})\n\nLocally assembled release candidate with synthetic stream, native reporting,\nreceipts, independent baseline and evaluator workflow. Publication is pending.\n',
        'NOTICE':'tokenledger\nProject design: Chandler Klose.\nImplementation assistance: GPT-6, recorded as the model in source commits.\nApache-2.0; third-party dependencies retain their respective licenses.\n',
        'baseline/README.md':'''# Independent conventional baseline

The baseline independently calculates recognition from the same commercial
activities, with normal shared macros, variables and dependency selection.
It does not consume the spine's recognized outputs. The output registry
defines the exact grains, units and tolerances used by the comparison.

Install `baseline/requirements-views-lock.txt` in an isolated
`.venv-baseline-views` environment. Generate a synthetic source with `tl generate`
and use `tl compare --help` for native comparison and suite options. Both arms
use the declared source and runtime; baseline dbt is outside the spine's path.
Large comparisons and their timing are separate from the small evaluator demo.
''',
        'CITATION.cff':f'cff-version: 1.2.0\nmessage: "Please cite this software if you use its reporting methods or benchmark."\ntitle: tokenledger\ntype: software\nauthors:\n  - family-names: Klose\n    given-names: Chandler\nversion: "{tag}"\nrepository-code: "{repository}"\nlicense: Apache-2.0\n',
        '.devcontainer/Dockerfile':dockerfile,
        '.devcontainer/devcontainer.json':json.dumps(dict(name='tokenledger evaluator',
            build=dict(dockerfile='Dockerfile'),postCreateCommand='python -m pip install -e ".[dev]"',
            remoteEnv=dict(PYTHONUTF8='1')),indent=2)+'\n',
        'ledger/process.jsonl':'','ledger/metrics.jsonl':'',
        'release-status.json':canonical(dict(status='prepared_not_published',tag=tag,
            clean_install='not_run',challenge='not_run',codespaces='not_run',
            matched_agent_benchmark='not_run',bigquery='not_run'))+'\n',
    }


def prepare(destination, *, repository, tag, revision='HEAD'):
    root = Path(destination).absolute()
    if root.exists(): raise ValidationError('release destination must not exist')
    for parent in root.parents:
        if (parent/'.git').exists(): raise ValidationError('release must be outside any existing Git checkout')
    if not re.fullmatch(r'https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+',repository):
        raise ValidationError('release repository must be an explicit GitHub HTTPS repository URL')
    if not re.fullmatch(r'v[0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9.-]+)?',tag):
        raise ValidationError('release tag must be a version, optionally with a prerelease suffix')
    sha = git(['rev-parse','--verify','--end-of-options',revision+'^{commit}']).decode().strip()
    names = [n for n in git(['ls-tree','-r','--name-only',sha]).decode().splitlines() if selected(n)]
    if not {'LICENSE','pyproject.toml','tl/cli.py','tl/demo/engine.py','baseline/outputs.yaml'} <= set(names):
        raise ValidationError('selected revision lacks the required reporting runtime')
    source = {name:git(['show',sha+':'+name]) for name in names}
    # Real private evidence and host paths are never accepted through the code allowlist.
    for name,raw in source.items():
        if re.search(rb'(?:[A-Za-z]:[\\/](?:Users|tokenledger-private)[\\/]|/(?:Users|home)/[a-z0-9_.-]+/|[\\/][a-z0-9_-]*_gtm[\\/]|hermes[\\/])',raw,re.I):
            raise ValidationError('release allowlisted file contains a private host reference: '+name)
    root.mkdir(parents=True)
    git(['init','--initial-branch=main'],cwd=root)
    git(['config','user.name','GPT-6'],cwd=root)
    git(['config','user.email','noreply@localhost'],cwd=root)
    commits=[]
    titles=['Assemble ActivitySchema stream contract and writer','Assemble deterministic fixtures and definitions',
            'Assemble reporting, controls and reproducible receipts','Assemble independent conventional baseline',
            'Assemble demonstration and synthetic evaluator checks']
    for index,title in enumerate(titles):
        selected_names=[name for name in names if group(name)==index]
        if not selected_names: continue
        for name in selected_names:
            path=root/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(source[name])
        git(['add','--',*selected_names],cwd=root)
        git(['commit','-m',title],cwd=root)
        commits.append(git(['rev-parse','HEAD'],cwd=root).decode().strip())
    day=datetime.now(timezone.utc).date().isoformat()
    extra=templates(repository,tag,day)
    for name,content in extra.items():
        path=root/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text(content,encoding='utf-8',newline='\n')
    (root/'release-files.json').write_text(canonical(dict(schema_version='tokenledger-release-files/v1',
        preparation_date=day,files={name:hashlib.sha256(raw).hexdigest() for name,raw in sorted(source.items())},
        public_history='Reconstructed at preparation time; original private history is not imported'))+'\n',encoding='utf-8')
    git(['add','--',*extra,'release-files.json'],cwd=root)
    git(['commit','-m','Prepare evaluator instructions, environment and release metadata'],cwd=root)
    commits.append(git(['rev-parse','HEAD'],cwd=root).decode().strip())
    return dict(status='prepared_not_published',path=str(root),source_revision=sha,public_commits=commits,
                candidate_tag=tag,tag_created=False,remote_configured=False,verification='not_run')


def register(cli, options, output):
    @cli.group('release')
    def release():
        """Prepare an isolated allowlisted candidate; never publish."""

    @release.command('prepare')
    @click.argument('destination',type=click.Path(path_type=Path))
    @click.option('--repository',required=True)
    @click.option('--tag',required=True)
    @click.option('--revision',default='HEAD')
    @options
    def command(ctx,destination,repository,tag,revision,json_output):
        output(prepare(destination,repository=repository,tag=tag,revision=revision),json_output)

    from tl.public_evidence import register as register_evidence
    register_evidence(release, options, output)
