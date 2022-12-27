from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 test_cases=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_cases = test_cases

    def execute(self, context):
        """
            DataQualityOperator: Check data in fact and dimensional tables.
        """
        self.log.info("Connect to Redshift")
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for i, test_case in enumerate(self.test_cases):
            sql = test_case['test_sql']
            exp_result = test_case['expected_result']

            records = redshift_hook.get_records(sql)

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError("Data quanlity check #{} has failed. Query returned no results: {}".format(i, sql))

            if exp_result != records[0][0]:
                raise ValueError("Data quality check #{} failed. Not meet the expected result: {}".format(i, sql))
            self.log.info("Data quality check #{} passed".format(i))