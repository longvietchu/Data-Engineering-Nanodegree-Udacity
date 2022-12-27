from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from operators.copy_redshift import CopyToRedshiftOperator
from operators.sas_value_redshift import SASValueToRedshiftOperator
from operators.data_quality import DataQualityOperator

from helpers.table_configs import sas_source_code_tables_data, copy_s3_keys, dq_checks

REDSHIFT_ARN = ""
S3_BUCKET = ""

default_args = {
    'owner': 'longcv',
    'end_date': datetime(2022, 12, 30),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
}

with DAG(
    'capstone_dag',
    default_args=default_args,
    description='Capstone Project for Data Engineering',
    start_date=datetime.now(),
    catchup=False,
    schedule_interval='@daily'
) as dag:

    start_operator = DummyOperator(task_id='Begin_execution')

    load_data_operators = []
    for table in copy_s3_keys:
        load_data_operators.append(CopyToRedshiftOperator(
            task_id=f'Copy_{table["name"]}_from_s3',
            aws_credentials_id='aws_credentials',
            redshift_conn_id='redshift',
            role_arn=REDSHIFT_ARN,
            table=table['name'],
            s3_bucket=S3_BUCKET,
            s3_key=table['key'],
            file_format=table['file_format'],
            delimiter=table['sep']
        ))
        
    for table in sas_source_code_tables_data:
        load_data_operators.append(SASValueToRedshiftOperator(
            task_id=f'Load_{table["name"]}_from_sas_source_code',
            aws_credentials_id='aws_credentials',
            redshift_conn_id='redshift',
            table=table['name'],
            s3_bucket=S3_BUCKET,
            s3_key='data/I94_SAS_Labels_Descriptions.SAS',
            sas_value=table['value'],
            columns=table['columns']
        ))

    dq_check_operators = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        dq_checks=dq_checks
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    start_operator >> load_data_operators >> dq_check_operators >> end_operator