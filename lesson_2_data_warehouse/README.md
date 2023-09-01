# Data Warehouse Project

## Background
The data provided is meant to mimic a music streaming startup who wants to move their processes onto a cloud. The data resides in S3 in a directory of JSON logs along with a directory of songs in the app.

The goal of this project is to build an ETL pipeline that ingests the data from S3, stages the data onto Redshift, and transfroms the data into a set of dimensional tables in order for the analytics team to determine insights. Here, we want to specifically understand user behavior regarding how people listen to music.

## Database schema design
This database follows a star schema, where the fact table is the log of song plays. The dimensions of the table include user, song, artist, and time.

The following is the database schema:

![schema image](https://github.com/maybeyue/udacity-data-engineering/blob/main/lesson_2_data_warehouse/schema.jpg)

This schema provides a simple, performative query execution that's easy to maintain.

## To run
Make sure appropriate credentials are included in `dwh.cfg`. 
The config files should be in the following format:
```
[CLUSTER]
HOST=
DB_NAME=
DB_USER=
DB_PASSWORD=
DB_PORT=

[IAM_ROLE]
ARN=

[S3]
LOG_DATA='s3://udacity-dend/log-data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song-data'
```

Once that is set up, run `create_tables.py` in order to get empty tables.

Once the tables are initialized, run `etl.py`. This will transform and add the final data Redshift.

Finally, run `basic_analysis.py` in order to gain some insights from the event data.