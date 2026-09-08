"""Authenticated permission inspection, explicitly short of mutation testing."""
from tl.bigquery.auth import credentials
from tl.stream import ValidationError


TABLE_PERMISSIONS=('bigquery.tables.getData','bigquery.tables.updateData',
    'bigquery.tables.update','bigquery.tables.delete','bigquery.tables.setIamPolicy')
PROJECT_PERMISSIONS=('bigquery.jobs.create','bigquery.datasets.create','iam.serviceAccounts.actAs')


def inspect(binding):
    if not binding.query_principal:
        return dict(status='not_run',reason='Distinct query and writer service accounts are not explicitly bound.',
                    native_write_boundary='not_run')
    from google.cloud import bigquery
    from google.api_core.exceptions import Forbidden
    from google.auth.transport.requests import AuthorizedSession
    results={}
    for role in ('query','writer'):
        credential=credentials(binding,role)
        sdk=bigquery.Client(project=binding.project,location=binding.location,credentials=credential)
        try:
            table=sdk.test_iam_permissions(binding.table,list(TABLE_PERMISSIONS))
        except Forbidden as exc:
            if 'insufficient authentication scopes' not in str(exc).lower():
                raise
            results[role]=dict(status='token_scope_prevents_inspection',granted=None,
                forbidden_granted=None,missing=None,
                explanation='The deployed append token cannot inspect IAM. This is not proof of effective role permissions.')
            continue
        session=AuthorizedSession(credential)
        try:
            response=session.post(f'https://cloudresourcemanager.googleapis.com/v1/projects/{binding.project}:testIamPermissions',
                                  json={'permissions':list(PROJECT_PERMISSIONS)},timeout=30)
            response.raise_for_status()
        finally:
            session.close()
        granted=set(table.get('permissions',[]))|set(response.json().get('permissions',[]))
        forbidden=({'bigquery.tables.updateData','bigquery.tables.update','bigquery.tables.delete','bigquery.tables.setIamPolicy'}
                   if role=='query' else {'bigquery.jobs.create','bigquery.tables.update','bigquery.tables.delete','bigquery.tables.setIamPolicy',
                                         'bigquery.datasets.create','iam.serviceAccounts.actAs'})
        required={'bigquery.jobs.create','bigquery.tables.getData'} if role=='query' else {'bigquery.tables.updateData'}
        results[role]=dict(status='inspected',granted=sorted(granted),forbidden_granted=sorted(forbidden & granted),missing=sorted(required-granted))
    return dict(status='permission_snapshot',binding_identity=binding.identity,roles=results,
        permission_shape=('incomplete' if any(r['status']!='inspected' for r in results.values()) else
            'expected' if all(not r['forbidden_granted'] and not r['missing'] for r in results.values()) else 'different'),
        native_write_boundary='not_run',remaining=['actual denied mutation attempts','inherited and cross-project query authority review',
                                                'CDC and overwrite refusal','concurrent-writer and retry trial','audit log evidence'])
