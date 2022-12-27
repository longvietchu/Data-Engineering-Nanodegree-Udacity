# Data Pipelines with Airflow

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Data Source

- Log data: `s3://udacity-dend/log_data`
- Song data: `s3://udacity-dend/song_data`

## Project Structure

- dags/sql/create_tables.sql: Includes SQL statements for initializing staging, fact and dimension tables
- dags/create_table_dag.py: DAG for creating tables in Redshift
- dags/sparkify_dag.py: DAG for processing the data from S3 to Redshift
- plugins/helpers/sql_queries.py: Includes SQL statements for inserting data into tables
- plugins/operators/stage_redshift.py: Operator for copying data from S3 buckets into Redshift staging tables
- plugins/operators/load_fact.py: Operator for loading data from Redshift staging tables into fact table
- plugins/operators/load_dimension.py: Operator for loading data from Redshift staging tables into dimensional tables
- plugins/operators/data_quality.py: Operator for validating data quality in Redshift tables
