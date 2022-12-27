import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from sparkify_dag import default_args

dag = DAG('create_table_dag', 
         start_date=datetime.datetime.now(),
         description='Create tables in Redshift',
         default_args=default_args)

start_operator = DummyOperator(task_id='Start_execution',  dag=dag)

create_table = PostgresOperator(
    task_id="Create_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql='sql/create_tables.sql'
)

end_operator = DummyOperator(task_id='End_execution',  dag=dag)

start_operator >> create_table >> end_operator