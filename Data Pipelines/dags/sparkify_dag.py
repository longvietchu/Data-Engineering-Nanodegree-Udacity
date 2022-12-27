from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'longcv3',
    'start_date': datetime(2022, 11, 1),
    'end_date': datetime(2022, 11, 30),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@daily',
          catchup=False,
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data/2018/11/",
    file_format="json",
    json_path="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/",    
    file_format="json",
    json_path='auto'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql_query=SqlQueries.user_table_insert,
    mode="delete-load"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql_query=SqlQueries.song_table_insert,
    mode="delete-load"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    sql_query=SqlQueries.artist_table_insert,
    mode="delete-load"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql_query=SqlQueries.time_table_insert,
    mode="delete-load"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    test_cases=[
        { 'test_sql': 'SELECT COUNT(*) FROM public.songplays WHERE userid IS NULL', 'expected_result': 0 }, 
        { 'test_sql': 'SELECT COUNT(DISTINCT "level") FROM public.songplays', 'expected_result': 2 },
        { 'test_sql': 'SELECT COUNT(*) FROM public.artists WHERE name IS NULL', 'expected_result': 0 },
        { 'test_sql': 'SELECT COUNT(*) FROM public.songs WHERE title IS NULL', 'expected_result': 0 },
        { 'test_sql': 'SELECT COUNT(*) FROM public.users WHERE first_name IS NULL', 'expected_result': 0 },
        { 'test_sql': 'SELECT COUNT(*) FROM public."time" WHERE weekday IS NULL', 'expected_result': 0 }
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Implementing tasks dependencies

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >>\
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >>\
run_quality_checks >> end_operator