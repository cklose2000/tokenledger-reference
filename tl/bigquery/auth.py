"""Separate impersonation targets are configuration, not proof of IAM controls."""
def credentials(binding,role):
    principal={'query':binding.query_principal,'writer':binding.writer_principal}[role]
    if principal is None: return None
    import google.auth
    from google.auth import impersonated_credentials
    source,_=google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
    return impersonated_credentials.Credentials(source_credentials=source,target_principal=principal,
        target_scopes=['https://www.googleapis.com/auth/bigquery.insertdata' if role=='writer'
                       else 'https://www.googleapis.com/auth/cloud-platform'],lifetime=900,
        quota_project_id=binding.project)
