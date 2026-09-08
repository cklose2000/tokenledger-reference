"""Registration kept separate from the single-trial baseline commands."""
from pathlib import Path
import click


def register(group, options, output):
    @group.command('baseline-suite-run')
    @click.option('--plan', type=click.Path(exists=True, path_type=Path), required=True)
    @click.option('--plan-sha256', required=True, help='Externally retained SHA-256 of the exact approved plan bytes.')
    @click.option('--output', 'directory', type=click.Path(path_type=Path), required=True)
    @click.option('--worker-python', type=click.Path(exists=True, path_type=Path), required=True)
    @options
    def execute(ctx, plan, plan_sha256, directory, worker_python, json_output):
        from tl.bigquery.baseline_suite import run
        result = run(plan, plan_sha256=plan_sha256, output=directory, worker_python=worker_python)
        output(result, json_output)
        if result['status'] != 'measured': raise click.exceptions.Exit(1)

    @group.command('baseline-suite-report')
    @click.argument('directory', type=click.Path(exists=True, path_type=Path))
    @click.option('--plan-sha256', required=True)
    @click.option('--output', 'report_directory', type=click.Path(path_type=Path), required=True)
    @options
    def offline(ctx, directory, plan_sha256, report_directory, json_output):
        from tl.bigquery.baseline_suite import report
        result = report(directory, plan_sha256=plan_sha256, output=report_directory)
        output(result, json_output)
        if result['status'] != 'measured': raise click.exceptions.Exit(1)
