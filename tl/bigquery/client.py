"""Cloud calls require a binding. Each query is estimated and bounded before use."""
from dataclasses import asdict
from pathlib import Path
import json
import uuid

from tl.stream import ValidationError
from tl.stream.events import canonical


class Client:
    def __init__(self, binding, *, sdk=None):
        self.binding=binding
        if sdk is None:
            try:
                from google.cloud import bigquery
            except ImportError as exc:
                raise ValidationError('install tokenledger[bigquery] for cloud execution') from exc
            # Credential lookup never selects the billing project or location.
            from tl.bigquery.auth import credentials
            sdk=bigquery.Client(project=binding.project, location=binding.location,credentials=credentials(binding,'query'))
        self.sdk=sdk
        self.reserved=0; self.jobs=[]

    def _config(self, *, dry):
        from google.cloud import bigquery
        return bigquery.QueryJobConfig(dry_run=dry, use_query_cache=False,
            use_legacy_sql=False, maximum_bytes_billed=self.binding.maximum_bytes_billed,
            labels={'application':'tokenledger', 'purpose':'synthetic-parity'})

    def estimate(self, sql):
        job=self.sdk.query(sql, job_config=self._config(dry=True), location=self.binding.location)
        count=int(job.total_bytes_processed or 0)
        if count>self.binding.maximum_bytes_billed:
            raise ValidationError('dry-run estimate exceeds the per-query byte ceiling')
        return dict(status='dry_run', total_bytes_processed=count, statement_type=job.statement_type,
                    cache_hit=False, native_execution='not_run')

    def query(self, sql, *, label):
        estimate=self.estimate(sql)
        # Reserve the *maximum*, not the estimate: every job can consume its cap.
        reserve=self.binding.maximum_bytes_billed
        if self.reserved+reserve>self.binding.maximum_run_bytes_billed:
            raise ValidationError('aggregate run byte ceiling exhausted; no query submitted')
        self.reserved+=reserve
        key='tl_'+uuid.uuid4().hex
        try:
            job=self.sdk.query(sql, job_config=self._config(dry=False), location=self.binding.location, job_id=key)
        except BaseException as exc:
            self.jobs.append(dict(label=label,job_id=key,project=self.binding.project,location=self.binding.location,
                status='submission_failed_or_unknown',errors=str(exc),total_bytes_billed=None,
                total_bytes_processed=None,slot_millis=None,cache_hit=None,estimate=estimate))
            raise
        try:
            rows=job.result()
            # Do not create a Storage Read API session or silently add its costs.
            table=rows.to_arrow(create_bqstorage_client=False)
        finally:
            details=dict(label=label, job_id=key, project=self.binding.project, location=self.binding.location,
                total_bytes_processed=job.total_bytes_processed, total_bytes_billed=job.total_bytes_billed,
                slot_millis=job.slot_millis, cache_hit=job.cache_hit,
                started=None if job.started is None else job.started.isoformat(),
                ended=None if job.ended is None else job.ended.isoformat(),
                errors=job.errors, estimate=estimate)
            self.jobs.append(details)
        return table, details

    def provision(self):
        from google.cloud import bigquery
        from tl.bigquery.schema import FIELDS
        if self.binding.query_principal:
            raise ValidationError('provision requires an explicit operator scratch binding; a reporting identity cannot administer datasets')
        dataset=bigquery.Dataset(self.binding.project+'.'+self.binding.dataset)
        dataset.location=self.binding.location
        # Create, never update an existing dataset's policy/location/expiration.
        dataset=self.sdk.create_dataset(dataset, exists_ok=True)
        if dataset.location.lower()!=self.binding.location.lower():
            raise ValidationError('existing dataset location differs from binding')
        fields=[]
        for name,kind,nullable in FIELDS:
            fields.append(bigquery.SchemaField(name,'NUMERIC' if kind.startswith('NUMERIC') else kind,
                         mode='NULLABLE' if nullable else 'REQUIRED',
                         **(dict(precision=18,scale=2) if kind.startswith('NUMERIC') else {})))
        table=bigquery.Table(self.binding.table, schema=fields)
        table.time_partitioning=bigquery.TimePartitioning(field='ts', type_='DAY')
        table.clustering_fields=['customer','activity']
        table.description='Synthetic ActivitySchema v2; validated append boundary; no CDC'
        actual=self.sdk.create_table(table, exists_ok=True)
        if ([f.to_api_repr() for f in actual.schema]!=[f.to_api_repr() for f in fields]
                or actual.time_partitioning.field!='ts'
                or actual.time_partitioning.type_!='DAY'
                or actual.clustering_fields!=['customer','activity']
                or actual.table_constraints is not None):
            raise ValidationError('existing stream physical contract differs; no table alteration attempted')
        return dict(status='provisioned',binding_identity=self.binding.identity,
            table=self.binding.table,physical_columns=14,reporting_intermediates=0,
            authority_mode=self.binding.authority_mode,iam_negative_tests='not_run',native_parity='not_run')
