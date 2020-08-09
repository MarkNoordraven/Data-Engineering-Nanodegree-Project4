              
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                table="",
                dq_checks=[]
                *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for dq_check in self.dq_checks:
            records = redshift_hook.get_records(f"{dq_check['check_sql']}")
            if max(len(records),records[0][0]) == dq_check['expected_results']:
                raise ValueError(f"Data quality check passed. Records {records} matched {dq_check['expected_results']}")
            else:
                raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
            self.log.info(f"Data quality on table check {dq_check['check_sql']} failed because results = {len(records)}")