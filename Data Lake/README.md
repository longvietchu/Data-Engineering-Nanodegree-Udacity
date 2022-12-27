# Spark and Data Lake

## Overview

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. This project builds an ETL pipeline for a data lake that load data from S3, process the data into analytics tables using Spark, and load them back into S3.

## Project structure

`etl.py`: includes Python code for processing source data

`test.ipynb`: a jupyter notebook to verify the result.

## How to run

1. Fill AWS credentials and S3 target bucket endpoint in `dl.cfg`
2. In the terminal, install pyspark and run the file `etl.py`
3. Verify the results by running the jupyter notebook `test.ipynb`

## ELT pipeline

### etl.py

1. `process_song_data`

   - Load raw data from S3 bucket, process that data with Spark, and write them back to S3 with _songs_ and _artists_ dimension table

2. `process_log_data`
   - Load raw data from S3 buckets, process event(log) dataset with Spark, and write them back to S3 with _time_ and _users_ dimension table and _songplays_ fact table

## Database schema

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
