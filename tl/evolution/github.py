"""Read-only private PR inspection. A GitHub identity is not trial approval."""
import subprocess

from tl.evolution.admission import ACTIVITIES, ENABLING_FILES, strict_json
from tl.stream import ValidationError

REPO = 'cklose2000/tokenledger'


def inspect(number, *, api=None):
    if type(number) is not int or number < 1:
        raise ValidationError('a positive private PR number is required')
    def gh(path):
        result = subprocess.run(['gh', 'api', path], capture_output=True, check=False, timeout=60)
        if result.returncode:
            raise ValidationError('private GitHub metadata could not be read')
        return strict_json(result.stdout)
    api = api or gh
    repo = api('repos/' + REPO)
    pr = api(f'repos/{REPO}/pulls/{number}')
    if (repo.get('private') is not True or repo.get('full_name') != REPO
            or pr.get('base', {}).get('repo', {}).get('full_name') != REPO
            or pr.get('head', {}).get('repo', {}).get('full_name') != REPO
            or pr.get('state') not in ('open','closed')):
        raise ValidationError('candidate PR must stay in the private engine repository')
    if pr['state']=='closed' and not pr.get('merged'):
        raise ValidationError('withdrawn candidate PR cannot authorize a new trial')
    # This fixed first increment is smaller than one API page. No silent
    # truncation, renamed paths, source-policy edits or external forks.
    files = api(f'repos/{REPO}/pulls/{number}/files?per_page=100')
    names = [f['filename'] for f in files]
    allowed = ENABLING_FILES | {f'definitions/reporting-learning/activities/{name}.yaml' for name in ACTIVITIES}
    if (len(files) != pr.get('changed_files') or len(files) >= 100 or len(set(names)) != len(names)
            or any(f.get('status') not in ('added','modified') for f in files)
            or not set(names) <= allowed or 'tl/evolution/sql/nrr_date_diagnostic_v1.sql' not in names):
        raise ValidationError('PR changes exceed the fixed first-increment scope')
    checks = api(f'repos/{REPO}/commits/{pr["head"]["sha"]}/check-runs?per_page=100')
    runs = checks.get('check_runs', [])
    # Do not infer review from CI. Keep required build evidence and independent
    # review as separately visible facts in the trial packet.
    required = ('secrets','test (ubuntu-latest)','test (windows-latest)')
    outcomes = {name: [r.get('conclusion') for r in runs if r['name'] == name] for name in required}
    green = all(values and all(v == 'success' for v in values) for values in outcomes.values())
    return dict(pr_url=pr['html_url'], pr_head=pr['head']['sha'], base_sha=pr['base']['sha'],
                changed_files=sorted(names), private=True, merged=bool(pr.get('merged')),
                required_checks=outcomes, ci_passed=green, independent_review='Grok QA is separate',
                trial_approval=False)
