"""Additive, explicitly scoped IAM bootstrap for the native boundary fixture.

This is administration evidence, never a denied-mutation or production pass.
The operator remains a privileged owner. No service-account keys are created.
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import time

from tl.bigquery.config import Binding
from tl.stream import ValidationError


ACCOUNT_NAMES = {
    'producer': 'tl-boundary-producer',
    'writer': 'tl-boundary-writer',
    'query': 'tl-boundary-query',
    'rebuild': 'tl-boundary-rebuild',
    'runtime': 'tl-boundary-runtime',
}
APPEND_PERMISSIONS = ['bigquery.tables.updateData']


def plan(binding, operator):
    if binding.query_principal or not binding.dataset.startswith('tokenledger_iam_'):
        raise ValidationError('IAM bootstrap requires an operator binding to a dedicated tokenledger_iam_ fixture')
    if not operator.startswith('user:') or '@' not in operator or any(c.isspace() for c in operator):
        raise ValidationError('explicit user:EMAIL administrator required')
    accounts = {role: name+'@'+binding.project+'.iam.gserviceaccount.com'
                for role, name in ACCOUNT_NAMES.items()}
    return dict(schema_version='tokenledger-bigquery-iam-plan/v1',
        project=binding.project, dataset=binding.dataset, table=binding.table,
        location=binding.location, operator=operator, accounts=accounts,
        custom_role='projects/'+binding.project+'/roles/tokenledgerAppend',
        append_permissions=APPEND_PERMISSIONS,
        source_roles={'writer': 'custom append permission only',
                      'query': 'roles/bigquery.dataViewer',
                      'rebuild': 'roles/bigquery.dataViewer'},
        job_users=['query', 'rebuild'],
        runtime_impersonates=['query', 'writer'],
        producer_authority='Cloud Run invoker only after separately pinned deployment',
        service_account_keys='none',
        required_admin_apis=['cloudresourcemanager.googleapis.com', 'iam.googleapis.com'],
        derived_dataset=binding.dataset+'_derived',
        external_grants='unproven; separate credential/admin threat boundary',
        acceptance='not_run')


class Admin:
    def __init__(self):
        import google.auth
        from google.auth.transport.requests import AuthorizedSession
        credential, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
        self.session = AuthorizedSession(credential)
        self.calls = []

    def call(self, method, url, body=None, *, missing=False):
        response = self.session.request(method, url, json=body, timeout=60)
        # Retain administrative responses, not bearer tokens or request headers.
        try:
            value = response.json()
        except ValueError:
            value = {'non_json_response': True}
        self.calls.append(dict(method=method, url=url, status_code=response.status_code,
                               response=value))
        if missing and response.status_code == 404:
            return None
        response.raise_for_status()
        return value

    def add_binding(self, resource, role, members):
        if resource.startswith('https://iam.googleapis.com/'):
            policy = self.call('POST', resource+':getIamPolicy?options.requestedPolicyVersion=3')
        else:
            policy = self.call('POST', resource+':getIamPolicy', {'options': {'requestedPolicyVersion': 3}})
        before = json.loads(json.dumps(policy))
        bindings = policy.setdefault('bindings', [])
        target = next((b for b in bindings if b['role'] == role and not b.get('condition')), None)
        if target is None:
            target = dict(role=role, members=[])
            bindings.append(target)
        target['members'] = sorted(set(target['members']) | set(members))
        if policy != before:
            # Preserve existing entries and etag. An intervening IAM edit fails.
            self.call('POST', resource+':setIamPolicy', {'policy': policy})
        return policy


def bootstrap(binding, operator, directory, *, admin=None):
    from google.cloud import bigquery
    from tl.bigquery.client import Client
    from tl.bigquery.contract import validate_source_table
    proposed = plan(binding, operator)
    root = Path(directory)
    if root.exists():
        raise ValidationError('IAM evidence directory already exists; use a new observation directory')
    root.mkdir(parents=True)
    def save(name, value):
        (root/name).write_text(json.dumps(value, sort_keys=True, indent=2)+'\n', encoding='utf-8')
    save('plan.json', proposed)
    project_url = 'https://cloudresourcemanager.googleapis.com/v1/projects/'+binding.project
    iam_root = 'https://iam.googleapis.com/v1/projects/'+binding.project
    result = dict(status='started', native_write_boundary='not_run', project=binding.project,
                  recorded_at=datetime.now(timezone.utc).isoformat())
    try:
        admin = admin or Admin()
        # No resource changes before validating the chosen project. If the
        # metadata API is disabled, explicit operator bootstrap must enable it.
        project = admin.call('GET', project_url)
        save('project.json', project)
        if project.get('projectId') != binding.project or project.get('parent'):
            raise ValidationError('this bootstrap requires the exact standalone personal project')
        enabled = admin.call('POST', 'https://serviceusage.googleapis.com/v1/projects/'+binding.project+
                            '/services:batchEnable', {'serviceIds': proposed['required_admin_apis']})
        for _ in range(30):
            if enabled.get('done'):
                break
            time.sleep(2)
            enabled = admin.call('GET', 'https://serviceusage.googleapis.com/v1/'+enabled['name'])
        if not enabled.get('done') or enabled.get('error'):
            raise ValidationError('administration API enablement is incomplete; inspect retained operation')
        save('project-iam-before.json', admin.call('POST', project_url+':getIamPolicy', {}))
        # Create the new fixture before granting identities. Historical acceptance
        # and workload datasets are never altered by this route.
        client = Client(binding)
        # An existing empty table may have live write streams. Never create a
        # second pin on it: offsets are unique only inside the original stream.
        # Interrupted bootstrap needs explicit recovery, not a silent retry.
        save('provision.json', client.provision(fresh=True))
        table = client.sdk.get_table(binding.table)
        validate_source_table(table)
        if table.num_rows:
            raise ValidationError('bootstrap requires an empty source fixture')
        save('source-before.json', table.to_api_repr())
        accounts = proposed['accounts']
        for role, email in accounts.items():
            resource = iam_root+'/serviceAccounts/'+email
            account = admin.call('GET', resource, missing=True)
            if account is None:
                account = admin.call('POST', iam_root+'/serviceAccounts', {
                    'accountId': ACCOUNT_NAMES[role],
                    'serviceAccount': {'displayName': 'Tokenledger boundary '+role}})
            if account.get('disabled') or account.get('email') != email:
                raise ValidationError('unexpected existing service account state')
            # Key absence is observed, not assumed from account naming.
            keys = admin.call('GET', resource+'/keys?keyTypes=USER_MANAGED')
            if keys.get('keys'):
                raise ValidationError('boundary identity has user-managed keys')
            admin.add_binding(resource, 'roles/iam.serviceAccountTokenCreator', [operator])
        role_url = 'https://iam.googleapis.com/v1/'+proposed['custom_role']
        custom = admin.call('GET', role_url, missing=True)
        if custom is None:
            custom = admin.call('POST', iam_root+'/roles', {
                'roleId': 'tokenledgerAppend', 'role': {
                    'title': 'Tokenledger source append transport', 'stage': 'GA',
                    'description': 'Trusted transport only; gateway enforces validated admission.',
                    'includedPermissions': APPEND_PERMISSIONS}})
        if sorted(custom.get('includedPermissions', [])) != APPEND_PERMISSIONS or custom.get('deleted'):
            raise ValidationError('existing custom append role differs; no broad role alteration attempted')
        source_url = ('https://bigquery.googleapis.com/bigquery/v2/projects/'+binding.project+
                      '/datasets/'+binding.dataset+'/tables/activity')
        admin.add_binding(source_url, proposed['custom_role'], ['serviceAccount:'+accounts['writer']])
        admin.add_binding(source_url, 'roles/bigquery.dataViewer',
                          ['serviceAccount:'+accounts[r] for r in ('query', 'rebuild')])
        admin.add_binding(project_url, 'roles/bigquery.jobUser',
                          ['serviceAccount:'+accounts[r] for r in ('query', 'rebuild')])
        for role in ('query', 'writer'):
            admin.add_binding(iam_root+'/serviceAccounts/'+accounts[role],
                              'roles/iam.serviceAccountTokenCreator', ['serviceAccount:'+accounts['runtime']])
        derived = bigquery.Dataset(binding.project+'.'+proposed['derived_dataset'])
        derived.location = binding.location
        derived = client.sdk.create_dataset(derived, exists_ok=True)
        if derived.location.lower() != binding.location.lower():
            raise ValidationError('derived fixture location differs')
        entries = list(derived.access_entries)
        entry = bigquery.AccessEntry('WRITER', 'userByEmail', accounts['rebuild'])
        if entry not in entries:
            derived.access_entries = entries+[entry]
            derived = client.sdk.update_dataset(derived, ['access_entries'])
        save('derived-after.json', derived.to_api_repr())
        # Pin one admin-created stream, kept out of caller-selected transport state.
        from google.cloud import bigquery_storage_v1
        from google.cloud.bigquery_storage_v1 import types
        storage = bigquery_storage_v1.BigQueryWriteClient()
        parent = f'projects/{binding.project}/datasets/{binding.dataset}/tables/activity'
        stream = storage.create_write_stream(parent=parent,
            write_stream=types.WriteStream(type_=types.WriteStream.Type.COMMITTED))
        separated = Binding(binding.project, binding.dataset, binding.location,
            binding.maximum_bytes_billed, binding.maximum_run_bytes_billed,
            query_principal=accounts['query'], writer_principal=accounts['writer'])
        separated.save(root/'binding.json')
        save('pinned-stream.json', dict(write_stream=stream.name, base_position=0,
             table_creation_time=table.to_api_repr()['creationTime'],
             table=binding.table, rollover='not implemented; missing/expired/finalized stream fails closed'))
        save('source-iam-after.json', admin.call('POST', source_url+':getIamPolicy', {}))
        save('project-iam-after.json', admin.call('POST', project_url+':getIamPolicy', {}))
        result.update(status='configured_unverified', accounts=accounts,
                      binding_identity=separated.identity, directory=str(root),
                      producer_deployment='not_run', mutation_tests='not_run', concurrency='not_run')
        return result
    except BaseException as exc:
        result.update(status='incomplete', error=type(exc).__name__+': '+str(exc),
                      recovery='Inspect retained state; no automatic IAM rollback or source deletion.')
        raise
    finally:
        save('admin-calls.json', admin.calls if admin is not None else [])
        save('result.json', result)
