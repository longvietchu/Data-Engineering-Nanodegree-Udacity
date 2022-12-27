# Date Modeling with Postgres

## Overview
In this project, we implement Data Modeling with Postgres, build an ETL pipeline using Python and relevant technologies. In the context of the startup, they want to analyze the data from their system which is songs and users' activities, in order to understand what songs users are listening to.

## How to run
Run these files respectively in terminal
```
python create_tables.py
python etl.py
```

## Project structure
```sql_queries.py```: includes all DDL and DML scripts for creating, dropping tables and inserting data into these tables.

```create_tables.py```: includes Python code for initializing sparkifydb database, fact and dimension tables.

```etl.py```: includes Python code for processing data in dataset.

```etl.ipynb```: a jupyter notebook to analyze dataset.

```test.ipynb```: a jupyter notebook to validate and test the project.

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

## ETL pipeline
1. ```process_data```: iterating dataset to apply ```process_song_file``` and ```process_log_file``` functions.
2. ```process_song_file```: processing ```song_data``` to insert data into songs and artists dimension tables.
3. ```process_log_file```: processing ```log_data``` to insert data into time and users dimension tables and songplays fact table.