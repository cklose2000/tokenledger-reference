"""Durable SQL-job boundary for the in-process, pinned dbt BigQuery baseline.

This is a cooperative runner boundary, not a sandbox for arbitrary Python or a
replacement for IAM. All SDK query submissions, including dbt introspection and
adapter retries, pass through it. A reserved job never releases its ceiling:
unknown outcomes therefore cannot make room for another billable job.
"""
from __future__ import annotations

from contextlib import ExitStack
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import inspect
import json
import os
from pathlib import Path
import re
import threading
from unittest.mock import patch
import uuid

from tl.bigquery.config import Binding
from tl.receipts.journal import ledger_lock
from tl.stream import ValidationError


_ACTIVE = threading.Lock()


def _hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                     ensure_ascii=False).encode()).hexdigest()


def _utc():
    return datetime.now(timezone.utc).isoformat()


def _contains(expected, observed):
    """Service-added defaults/destination metadata may enrich a submitted config."""
    if isinstance(expected, dict):
        return isinstance(observed, dict) and all(
            key in observed and _contains(value, observed[key]) for key, value in expected.items())
    return expected == observed


def _configuration_matches(expected, observed):
    """The REST service omits an unset top-level reservation, serialized as null by SDK."""
    if (isinstance(expected, dict) and isinstance(observed, dict)
            and expected.get('reservation') is None and 'reservation' not in observed):
        expected = {key: value for key, value in expected.items() if key != 'reservation'}
    return _contains(expected, observed)


class JobBoundaryError(ValidationError):
    """Submission was refused, or retained evidence needs operator attention."""


class DbtJobRecorder:
    """Patch SDK submissions for exactly one in-process dbtRunner lifetime.

    ``ledger`` is an append-only JSONL reservation and observation log. Reusing
    the same path/run_id resumes its *remaining* allowance, never a fresh budget.
    Source/derived datasets must all be in the explicit bound project. SQL table
    references and metadata requests are constrained to that allowlist; writes
    through API destination fields are constrained to derived datasets. The
    caller must separately freeze/review the exported SQL and its write scope.
    """

    def __init__(self, binding: Binding, ledger, *, run_id: str,
                 source_datasets, derived_datasets):
        if binding.location != 'us-east4':
            raise JobBoundaryError('this registered baseline requires explicit us-east4')
        if not isinstance(run_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,100}', run_id):
            raise JobBoundaryError('an explicit safe run_id is required')
        self.binding, self.path, self.run_id = binding, Path(ledger), run_id
        self.sources = frozenset(source_datasets)
        self.derived = frozenset(derived_datasets)
        if not self.sources or not self.derived or self.sources & self.derived:
            raise JobBoundaryError('distinct nonempty source and derived dataset allowlists required')
        if any(not isinstance(x, str) or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,127}', x)
               for x in self.sources | self.derived):
            raise JobBoundaryError('dataset allowlists contain an invalid dataset')
        self.datasets = self.sources | self.derived
        self._mutex = threading.RLock()
        self._jobs, self._clients = {}, {}
        self._children, self._child_scans = {}, {}
        self._rejected = []
        self._stack = None
        self._sequence = 0
        self._header = dict(schema='tokenledger-dbt-jobs/v1', run_id=run_id,
                            binding_identity=binding.identity,
                            project=binding.project, location=binding.location,
                            source_datasets=sorted(self.sources),
                            derived_datasets=sorted(self.derived),
                            maximum_bytes_billed=binding.maximum_bytes_billed,
                            maximum_run_bytes_billed=binding.maximum_run_bytes_billed)

    def _append(self, event, **values):
        # Called under _mutex; the OS lock is held for the entire context.
        row = dict(sequence=self._sequence, recorded_at=_utc(), event=event, **values)
        with self.path.open('a', encoding='utf-8', newline='\n') as out:
            out.write(json.dumps(row, sort_keys=True, separators=(',', ':')) + '\n')
            out.flush()
            os.fsync(out.fileno())
        self._sequence += 1
        return row

    def _read(self):
        if not self.path.exists():
            self._append('boundary', **self._header)
            return
        raw = self.path.read_bytes()
        if not raw or not raw.endswith(b'\n'):
            raise JobBoundaryError('incomplete reservation ledger; preserve it for review')
        try:
            rows = [json.loads(line) for line in raw.splitlines()]
            if any(row['sequence'] != index for index, row in enumerate(rows)):
                raise ValueError('sequence')
            first = {k: v for k, v in rows[0].items()
                     if k not in {'event', 'sequence', 'recorded_at'}}
            if rows[0]['event'] != 'boundary' or first != self._header:
                raise ValueError('binding')
            for row in rows[1:]:
                if row['event'] == 'reserved':
                    if row['job_id'] in self._jobs:
                        raise ValueError('duplicate reservation')
                    self._jobs[row['job_id']] = dict(row, observation=None)
                elif row['event'] == 'observed':
                    self._jobs[row['job_id']]['observation'] = row
                elif row['event'] == 'child_observed':
                    self._children[row['job_id']] = row
                elif row['event'] == 'child_scan':
                    self._child_scans[row['job_id']] = row
                elif row['event'] == 'observation_rejected':
                    self._rejected.append(row)
            self._sequence = len(rows)
        except (ValueError, KeyError, TypeError) as exc:
            raise JobBoundaryError('invalid or differently bound reservation ledger') from exc
        if self.reserved_bytes > self.binding.maximum_run_bytes_billed:
            raise JobBoundaryError('retained reservations exceed the bound allowance')

    @property
    def reserved_bytes(self):
        return sum(x['reserved_bytes'] for x in self._jobs.values())

    def _reserve(self, job_id, payload):
        fingerprint = _hash(payload)
        ceiling = 0 if payload.get('dryRun') is True else self.binding.maximum_bytes_billed
        with self._mutex:
            if job_id in self._jobs:
                if self._jobs[job_id]['payload_sha256'] != fingerprint:
                    raise JobBoundaryError('stable job ID reused with changed SQL or configuration')
                self._append('resubmit', job_id=job_id, payload_sha256=fingerprint)
                return
            if self.reserved_bytes + ceiling > self.binding.maximum_run_bytes_billed:
                self._append('budget_refused', job_id=job_id, reserved_bytes=self.reserved_bytes)
                raise JobBoundaryError('aggregate query allowance exhausted before submission')
            row = self._append('reserved', job_id=job_id, payload_sha256=fingerprint,
                               query_sha256=_hash(payload['query']['query']),
                               reserved_bytes=ceiling,
                               configuration=payload)
            self._jobs[job_id] = dict(row, observation=None)

    def _scope_sql(self, sql):
        # The full-build trial has one query/CTAS per job. Script budget behavior
        # is deliberately not an assumption underlying its aggregate allowance.
        import sqlglot
        from sqlglot import exp
        try:
            trees = [tree for tree in sqlglot.parse(sql, read='bigquery',
                     error_level=sqlglot.ErrorLevel.RAISE) if tree is not None]
            if len(trees) != 1:
                raise JobBoundaryError('exactly one SELECT or CREATE AS query is allowed per job')
            for tree in trees:
                if not (isinstance(tree, exp.Query) or (
                        isinstance(tree, exp.Create) and tree.args.get('kind') in ('TABLE', 'VIEW')
                        and isinstance(tree.expression, exp.Query))):
                    raise JobBoundaryError('procedural, dynamic, or non-query SQL is outside the full-build trial')
                if isinstance(tree, exp.Create):
                    target = tree.this.this if isinstance(tree.this, exp.Schema) else tree.this
                    if (not isinstance(target, exp.Table) or target.catalog != self.binding.project
                            or target.db not in self.derived):
                        raise JobBoundaryError('CREATE target must explicitly name the bound project and a derived dataset')
                for table in tree.find_all(exp.Table):
                    project, dataset = table.catalog, table.db
                    if project and project != self.binding.project:
                        raise JobBoundaryError('SQL references an unbound project')
                    if dataset and dataset not in self.datasets:
                        raise JobBoundaryError('SQL references an unbound dataset')
        except sqlglot.errors.SqlglotError as exc:
            raise JobBoundaryError('SQL could not be checked against the registered namespaces') from exc

    def _config(self, client, config):
        from google.cloud.bigquery import QueryJobConfig
        from google.cloud.bigquery import _job_helpers
        if config is not None and not isinstance(config, QueryJobConfig):
            raise JobBoundaryError('SQL-only QueryJobConfig required')
        config = _job_helpers.job_config_with_defaults(deepcopy(config),
                                                      client.default_query_job_config)
        config = config or QueryJobConfig()
        config.use_query_cache = False
        config.use_legacy_sql = False
        config.maximum_bytes_billed = self.binding.maximum_bytes_billed
        if config.maximum_billing_tier not in (None, 1):
            raise JobBoundaryError('billing tier multipliers are outside the on-demand benchmark')
        if config.reservation:
            raise JobBoundaryError('slot reservation overrides are outside the on-demand benchmark')
        for field, allowed in [('default_dataset', self.datasets), ('destination', self.derived)]:
            reference = getattr(config, field)
            if reference and (reference.project != self.binding.project or reference.dataset_id not in allowed):
                raise JobBoundaryError(f'unbound query {field}')
        return config

    def _observe(self, job, phase, error=None, *, requested_job_id=None):
        job_id = requested_job_id or job.job_id
        if job_id not in self._jobs:
            raise JobBoundaryError('job observation has no prior reservation')
        expected = self._jobs[job_id]['configuration']
        dry_run = expected.get('dryRun') is True
        if (not dry_run and (job.job_id != job_id or job.project != self.binding.project
                             or job.location != self.binding.location)):
            self._reject_observation(job, job_id, phase, 'returned_identity_mismatch')
            raise JobBoundaryError('returned job identity does not match the boundary')
        # to_api_repr() is deliberately NOT used: QueryJob strips statistics there.
        props = deepcopy(job._properties)
        if not dry_run and not _configuration_matches(expected, props.get('configuration')):
            self._reject_observation(job, job_id, phase, 'returned_configuration_mismatch')
            raise JobBoundaryError('returned job configuration differs from reserved payload')
        observed = self._details(job, phase, error)
        observed.update(job_id=job_id, dry_run=dry_run)
        destination = observed['returned_destination']
        observed['destination_metadata_path'] = None
        if (not dry_run and job.state == 'DONE' and job.error_result is None
                and isinstance(destination, dict) and destination.get('projectId') == self.binding.project
                and all(isinstance(destination.get(key), str)
                        and re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,1023}', destination[key])
                        for key in ('datasetId', 'tableId'))):
            observed['destination_metadata_path'] = (
                f"/projects/{self.binding.project}/datasets/{destination['datasetId']}"
                f"/tables/{destination['tableId']}")
        with self._mutex:
            row = self._append('observed', **observed)
            self._jobs[job_id]['observation'] = row

    def _reject_observation(self, job, job_id, phase, reason):
        """Preserve returned evidence separately; it is never a verified observation."""
        props = deepcopy(job._properties)
        with self._mutex:
            row = self._append('observation_rejected', job_id=job_id, phase=phase,
                reason=reason, verified=False, reservation_retained=True,
                expected_payload_sha256=self._jobs[job_id]['payload_sha256'],
                returned_job_reference=props.get('jobReference'),
                returned_configuration=props.get('configuration'),
                returned_statistics=props.get('statistics'), returned_status=props.get('status'))
            self._rejected.append(row)

    @staticmethod
    def _details(job, phase, error=None):
        props = deepcopy(job._properties)
        stats = props.get('statistics') or {}
        query = stats.get('query') or {}
        def integer(name, source=query):
            value = source.get(name)
            return int(value) if value is not None else None
        return dict(job_id=job.job_id, returned_job_id=job.job_id,
                        phase=phase, project=job.project,
                        location=job.location, state=job.state,
                        error_type=type(error).__name__ if error else None,
                        error_result=job.error_result, errors=job.errors,
                        total_bytes_billed=integer('totalBytesBilled'),
                        total_bytes_processed=integer('totalBytesProcessed'),
                        slot_millis=integer('totalSlotMs'), cache_hit=query.get('cacheHit'),
                        statement_type=query.get('statementType'),
                        parent_job_id=stats.get('parentJobId'),
                        child_job_count=integer('numChildJobs', stats),
                        returned_destination=deepcopy(props.get('configuration', {}).get('query', {})
                                                      .get('destinationTable')),
                        statistics=stats)

    def _scan_children(self, client, parent):
        """Read child IDs as diagnostics; their cost is covered by the parent cap."""
        observed_ids = set()
        error_type = None
        try:
            for child in client.list_jobs(project=self.binding.project, parent_job=parent.job_id,
                                          max_results=1000, retry=None):
                detail = self._details(child, 'child_list')
                if (detail['parent_job_id'] != parent.job_id or child.project != self.binding.project
                        or child.location != self.binding.location or child.job_id in self._jobs):
                    raise JobBoundaryError('child job does not belong to the reserved parent')
                if child.job_id in observed_ids:
                    raise JobBoundaryError('duplicate child job in metadata enumeration')
                observed_ids.add(child.job_id)
                with self._mutex:
                    row = self._append('child_observed', **detail,
                        reservation_parent_job_id=parent.job_id, counted_in_billed_total=False)
                    self._children[child.job_id] = row
        except Exception as exc:
            error_type = type(exc).__name__
        expected = parent._properties.get('statistics', {}).get('numChildJobs')
        expected = int(expected) if expected is not None else None
        with self._mutex:
            row = self._append('child_scan', job_id=parent.job_id, expected_count=expected,
                observed_count=len(observed_ids), error_type=error_type,
                complete=error_type is None and expected is not None and len(observed_ids) == expected)
            self._child_scans[parent.job_id] = row

    def _failure(self, job_id, phase, exc):
        with self._mutex:
            self._append('uncertain', job_id=job_id, phase=phase,
                         error_type=type(exc).__name__, reservation_retained=True)

    def _api_guard(self, method, path, data, query_params):
        pieces = path.strip('/').split('/')
        if len(pieces) < 2 or pieces[0] != 'projects' or pieces[1] != self.binding.project:
            raise JobBoundaryError('unbound BigQuery API project/path')
        params = dict(query_params or {})
        if params.get('location', self.binding.location) != self.binding.location:
            raise JobBoundaryError('unbound BigQuery API location')
        if 'parentJobId' in params and params['parentJobId'] not in self._jobs:
            raise JobBoundaryError('unregistered parent job metadata enumeration')
        if method == 'GET' and len(pieces) == 6 and pieces[2] == 'datasets' and pieces[4] == 'tables':
            # dbt execute() reads SELECT's anonymous destination for num_rows.
            # Grant one exact metadata GET from a validated completed job, never
            # its dataset, table data, SQL references, or mutations. Rejections
            # revoke the capability even when a prior observation was valid.
            with self._mutex:
                rejected_ids = {row['job_id'] for row in self._rejected}
                if any(job_id not in rejected_ids and row['observation'] is not None
                       and row['observation'].get('destination_metadata_path') == path
                       for job_id, row in self._jobs.items()):
                    self._append('destination_metadata_read', path=path)
                    return
        if 'datasets' in pieces:
            idx = pieces.index('datasets')
            dataset = pieces[idx + 1] if len(pieces) > idx + 1 else None
            if dataset and dataset not in self.datasets:
                raise JobBoundaryError('unbound metadata dataset')
            if method != 'GET':
                proposed = dataset or (data or {}).get('datasetReference', {}).get('datasetId')
                if proposed not in self.derived:
                    raise JobBoundaryError('source metadata mutations are outside the baseline')
                if method == 'POST' and dataset is None:
                    reference = (data or {}).get('datasetReference', {})
                    if (reference.get('projectId') != self.binding.project
                            or (data or {}).get('location') != self.binding.location):
                        raise JobBoundaryError('dataset creation must explicitly bind project and region')
        if method == 'POST' and len(pieces) == 3 and pieces[2] == 'jobs':
            config = (data or {}).get('configuration') or {}
            reference = (data or {}).get('jobReference') or {}
            job_id = reference.get('jobId')
            with self._mutex:
                reserved = self._jobs.get(job_id)
                if (not reserved or reference.get('projectId') != self.binding.project
                        or reference.get('location') != self.binding.location
                        or _hash(config) != reserved['payload_sha256']
                        or any(k in config for k in ('load', 'copy', 'extract'))):
                    raise JobBoundaryError('unreserved or changed SQL job submission')
                self._append('submission', job_id=job_id)
        elif method != 'GET' and 'queries' in pieces:
            raise JobBoundaryError('jobs.query alternate submission is disabled')
        elif method != 'GET' and 'jobs' in pieces:
            # Cancellation never frees a reservation; it is safe to retain evidence.
            if pieces[-1] != 'cancel' or len(pieces) != 5 or pieces[3] not in self._jobs:
                raise JobBoundaryError('unregistered job operation')

    def __enter__(self):
        if not _ACTIVE.acquire(blocking=False):
            raise JobBoundaryError('a dbt job recorder is already active; nesting/overlap is forbidden')
        stack = ExitStack()
        stack.callback(_ACTIVE.release)
        try:
            from google.cloud.bigquery import Client, QueryJob
            from google.cloud.bigquery._http import Connection
            stack.enter_context(ledger_lock(self.path))
            self._read()
            originals = {name: getattr(Client, name) for name in ('query', 'get_job')}
            original_api = Connection.api_request
            query_signature = inspect.signature(originals['query'])
            api_signature = inspect.signature(original_api)
            get_signature = inspect.signature(originals['get_job'])
            recorder = self

            def query(client, *args, **kwargs):
                bound = query_signature.bind(client, *args, **kwargs)
                args = bound.arguments
                if client.project != recorder.binding.project or client.location != recorder.binding.location:
                    raise JobBoundaryError('SDK client must explicitly bind project and us-east4')
                for field, expected in [('project', recorder.binding.project), ('location', recorder.binding.location)]:
                    if args.get(field) not in (None, expected):
                        raise JobBoundaryError(f'unbound query {field}')
                    args[field] = expected
                if args.get('api_method', 'INSERT') != 'INSERT':
                    raise JobBoundaryError('jobs.query alternate submission is disabled')
                recorder._scope_sql(args['query'])
                config = recorder._config(client, args.get('job_config'))
                args['job_config'] = config
                args['job_id'] = args.get('job_id') or 'tl_dbt_' + uuid.uuid4().hex
                if not re.fullmatch(r'[A-Za-z0-9_-]{1,1024}', args['job_id']):
                    raise JobBoundaryError('invalid explicit query job ID')
                args['job_id_prefix'] = None
                args['retry'] = None
                args['job_retry'] = None
                args['api_method'] = 'INSERT'
                payload = config.to_api_repr()
                payload.setdefault('query', {})['query'] = args['query']
                recorder._reserve(args['job_id'], payload)
                recorder._clients[args['job_id']] = client
                try:
                    job = originals['query'](*bound.args, **bound.kwargs)
                    recorder._observe(job, 'submitted', requested_job_id=args['job_id'])
                    return job
                except Exception as exc:
                    recorder._failure(args['job_id'], 'submit', exc)
                    raise

            def api(connection, *args, **kwargs):
                bound = api_signature.bind(connection, *args, **kwargs)
                values = bound.arguments
                recorder._api_guard(values['method'], values['path'], values.get('data'),
                                    values.get('query_params'))
                return original_api(*bound.args, **bound.kwargs)

            def get_job(client, *args, **kwargs):
                bound = get_signature.bind(client, *args, **kwargs)
                values = bound.arguments
                job_id = values['job_id']
                if isinstance(job_id, QueryJob):
                    candidate = job_id
                    job_id = candidate.job_id
                    if not isinstance(job_id, str) or job_id not in recorder._jobs:
                        raise JobBoundaryError('only reserved query job objects may be attached')
                    if (candidate.project != recorder.binding.project
                            or candidate.location != recorder.binding.location
                            or not _configuration_matches(recorder._jobs[job_id]['configuration'],
                                                          candidate._properties.get('configuration'))):
                        raise JobBoundaryError('query job object identity or configuration differs from its reservation')
                    # SDK AsyncJob.reload supplies the object itself. Validate it
                    # before converting to a bound string ID; kwargs cannot repair
                    # a foreign or altered object into an authorized attachment.
                    values['job_id'] = job_id
                if not isinstance(job_id, str) or job_id not in recorder._jobs:
                    raise JobBoundaryError('only reserved job identities may be attached')
                for field, expected in [('project', recorder.binding.project), ('location', recorder.binding.location)]:
                    if values.get(field) not in (None, expected):
                        raise JobBoundaryError('unbound job attachment')
                    values[field] = expected
                values['retry'] = None
                try:
                    job = originals['get_job'](*bound.args, **bound.kwargs)
                    recorder._observe(job, 'attached', requested_job_id=job_id)
                    return job
                except Exception as exc:
                    recorder._failure(job_id, 'attach', exc)
                    raise

            def observer(original, phase):
                signature = inspect.signature(original)
                def call(job, *args, **kwargs):
                    bound = signature.bind(job, *args, **kwargs)
                    if job.job_id not in recorder._jobs:
                        raise JobBoundaryError('cannot wait on an unreserved query job')
                    if 'job_retry' in signature.parameters:
                        bound.arguments['job_retry'] = None
                    error = None
                    try:
                        return original(*bound.args, **bound.kwargs)
                    except Exception as exc:
                        error = exc
                        raise
                    finally:
                        recorder._observe(job, phase, error)
                return call

            stack.enter_context(patch.object(Client, 'query', query))
            stack.enter_context(patch.object(Client, 'get_job', get_job))
            stack.enter_context(patch.object(Connection, 'api_request', api))
            for name in ('result', 'reload'):
                stack.enter_context(patch.object(QueryJob, name, observer(getattr(QueryJob, name), name)))
            def disabled(*args, **kwargs):
                raise JobBoundaryError('only guarded SQL jobs.insert is permitted in this baseline')
            for name in ('query_and_wait', 'query_and_wait_arrow', 'load_table_from_file',
                         'load_table_from_uri', 'load_table_from_json', 'load_table_from_dataframe',
                         'copy_table', 'extract_table'):
                if hasattr(Client, name):
                    stack.enter_context(patch.object(Client, name, disabled))
            self._stack = stack
            return self
        except BaseException:
            stack.close()
            raise

    def __exit__(self, *exception):
        try:
            with self._mutex:
                self._append('context_closed', exceptional=exception[0] is not None,
                             reserved_bytes=self.reserved_bytes)
        finally:
            self._stack.close()
            self._stack = None

    def refresh(self, client=None):
        """Read registered job IDs once, without rerunning SQL or refunding caps."""
        if self._stack is None:
            raise JobBoundaryError('refresh requires the active guarded context')
        for job_id in list(self._jobs):
            if self._jobs[job_id]['configuration'].get('dryRun') is True:
                continue  # Dry runs need not create a persistent server job.
            bound_client = client or self._clients.get(job_id)
            if bound_client is None:
                self._failure(job_id, 'refresh', JobBoundaryError('no explicit client'))
                continue
            try:
                job = bound_client.get_job(job_id, project=self.binding.project,
                                           location=self.binding.location, retry=None)
                if job.statement_type == 'SCRIPT' or job.num_child_jobs:
                    self._scan_children(bound_client, job)
            except Exception:
                # get_job retains the exact failed lookup; unknown stays unknown.
                continue
        return self.summary()

    def invoke_dbt(self, args):
        """Use real dbt in-process so its adapter cannot escape the SDK guard."""
        if self._stack is None:
            raise JobBoundaryError('dbt requires the active guarded context')
        from dbt.cli.main import dbtRunner
        return dbtRunner().invoke(list(args))

    def summary(self):
        with self._mutex:
            observations = [deepcopy(row['observation']) for row in self._jobs.values()]
            # Parent SCRIPT stats roll up their children. Never sum both here.
            roots = [x for x in observations if x and not x['parent_job_id'] and not x['dry_run']]
            known = [x['total_bytes_billed'] for x in roots if x['total_bytes_billed'] is not None]
            executed = [row for row in self._jobs.values() if row['configuration'].get('dryRun') is not True]
            rejected_ids = {row['job_id'] for row in self._rejected}
            unknown = sum(row['observation'] is None or row['observation']['state'] != 'DONE'
                          or row['observation']['total_bytes_billed'] is None
                          or row['job_id'] in rejected_ids for row in executed)
            scripts = [x['job_id'] for x in roots if x['statement_type'] == 'SCRIPT' or x['child_job_count']]
            return dict(schema='tokenledger-dbt-job-summary/v1', run_id=self.run_id,
                        project=self.binding.project, location=self.binding.location,
                        job_count=len(self._jobs), reserved_bytes=self.reserved_bytes,
                        dry_run_count=len(self._jobs) - len(executed),
                        actual_job_count=len(executed),
                        remaining_reservation_bytes=self.binding.maximum_run_bytes_billed - self.reserved_bytes,
                        observed_billed_bytes=sum(known) if known else None,
                        complete_billed_total=unknown == 0,
                        unknown_or_incomplete_jobs=unknown,
                        rejected_job_count=len(rejected_ids),
                        rejected_observations=deepcopy(self._rejected),
                        child_job_count=len(self._children),
                        child_metadata_complete=all(self._child_scans.get(x, {}).get('complete') is True
                                                    for x in scripts),
                        child_observations=deepcopy(list(self._children.values())),
                        child_scans=deepcopy(list(self._child_scans.values())),
                        script_child_statistics='not separately added to parent totals',
                        observations=observations, ledger=str(self.path))
