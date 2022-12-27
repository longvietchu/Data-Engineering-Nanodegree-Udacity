# Data Warehouse with AWS

## Overview
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. In this project, we build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

## How to run
1. Fill parameters in `dwh.cfg`
2. Run the notebook `run.ipynb`

## Project structure
```sql_queries.py```: includes all DDL and DML scripts for creating, dropping, inserting data into tables and copy command to stage data from S3 to Redshift.

```create_tables.py```: includes Python code for initializing staging, fact and dimension tables.

```etl.py```: includes Python code for loading data to staging, fact, dimension tables.

```run.ipynb```: a jupyter notebook to run the project and test the result.

## Database schema
### Staging tables
1. **staging_events** - stage event data from datasets
2. **staging_songs** - stage song data from datasets

### Fact table
1. **songplays** - records in log data associated with song plays i.e. records with page `NextSong`
```
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
```

### Dimension tables
2. **users** - users in the app
```
user_id, first_name, last_name, gender, level
```
3. **songs** - songs in music database
```
song_id, title, artist_id, year, duration
```
4. **artists** - artists in music database
```
artist_id, name, location, latitude, longitude
```
5. **time** - timestamps of records in songplays broken down into specific units
```
start_time, hour, day, week, month, year, weekday
```

## ETL pipeline
1. ```load_staging_tables```: runs the copy commands from ```copy_table_queries``` to load data from S3 into staging tables on Redshift
2. ```insert_tables```: processes data from staging tables into analytics tables on Redshift