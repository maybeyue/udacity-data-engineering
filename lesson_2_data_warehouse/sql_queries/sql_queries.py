""" SQL queries for creating and dropping tables, and inserting data into
tables."""

import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay;"
user_table_drop = "DROP TABLE IF EXISTS dim_user;"
song_table_drop = "DROP TABLE IF EXISTS dim_song;"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist;"
time_table_drop = "DROP TABLE IF EXISTS dim_time;"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE staging_events (
        artist character varying(150),
        auth character varying(10),
        firstName character varying(20),
        gender character(1),
        itemInSession integer,
        lastName character varying(20),
        length numeric(10,5),
        level character varying(4),
        location character varying(100),
        method character varying(3),
        page character varying(25),
        registration numeric(20,6),
        sessionId integer,
        song character varying(200),
        status integer,
        ts numeric(20,6),
        userAgent character varying(150),
        userId integer
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs integer,
        artist_id character varying(20),
        artist_latitude numeric(10,5),
        artist_longitude numeric(10,5),
        artist_location character varying(200),
        artist_name character varying(200),
        song_id character varying(20),
        title character varying(200),
        duration numeric(10,5),
        year integer
    );
""")

songplay_table_create = ("""
    CREATE TABLE fact_songplay (
        songplay_id integer IDENTITY(0,1) PRIMARY KEY,
        start_time timestamp NOT NULL,
        user_id integer NOT NULL,
        level character varying(4),
        song_id character varying(20),
        artist_id character varying(20),
        session_id integer,
        location character varying(100),
        user_agent character varying(150)
    );
""")

user_table_create = ("""
    CREATE TABLE dim_user (
        user_id integer PRIMARY KEY,
        first_name character varying(20),
        last_name character varying(20),
        gender character(1),
        level character varying(4)
    );
""")

song_table_create = ("""
    CREATE TABLE dim_song (
        song_id character varying(20) PRIMARY KEY,
        title character varying(200),
        artist_id character varying(20),
        year integer,
        duration numeric(10,5)
    );
""")

artist_table_create = ("""
    CREATE TABLE dim_artist (
        artist_id character varying(20) PRIMARY KEY,
        name character varying(200),
        location character varying(200),
        latitude numeric(10,5),
        longitude numeric(10,5)
    );
""")

time_table_create = ("""
    CREATE TABLE dim_time (
        start_time timestamp PRIMARY KEY,
        hour integer,
        day integer,
        week integer,
        month integer,
        year integer,
        weekday integer
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM 's3://udacity-dend/log_data'
    credentials 'aws_iam_role={}'
    FORMAT AS JSON 's3://udacity-dend/log_json_path.json'
""").format(config['IAM_ROLE']['ARN'])

staging_songs_copy = ("""
COPY staging_songs
    FROM 's3://udacity-dend/song_data'
    credentials 'aws_iam_role={}'
    FORMAT AS JSON 'auto'
""").format(config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO fact_songplay (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    SELECT
        TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second' AS start_time,
        userId AS user_id,
        level,
        song_id,
        artist_id,
        sessionId AS session_id,
        location,
        userAgent AS user_agent
    FROM staging_events
    JOIN staging_songs
        ON staging_events.song = staging_songs.title
            AND staging_events.artist = staging_songs.artist_name
    WHERE page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO dim_user (
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
    SELECT DISTINCT
        userId AS user_id,
        firstName AS first_name,
        lastName AS last_name,
        gender,
        level
    FROM staging_events
    WHERE page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO dim_song (
        song_id,
        title,
        artist_id,
        year,
        duration
    )
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO dim_artist (
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    SELECT DISTINCT
        artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO dim_time (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    WITH timestamp_table AS (
        SELECT DISTINCT
            TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second' AS ts
        FROM staging_events
        WHERE page = 'NextSong'
    )
    SELECT
        ts AS start_time,
        EXTRACT(hour FROM ts) AS hour,
        EXTRACT(day FROM ts) AS day,
        EXTRACT(week FROM ts) AS week,
        EXTRACT(month FROM ts) AS month,
        EXTRACT(year FROM ts) AS year,
        EXTRACT(dayofweek FROM ts) AS weekday
    FROM timestamp_table
""")

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]
copy_table_queries = [
    staging_events_copy,
    staging_songs_copy
]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert
]

# tables generated
generated_tables = [
    "staging_events",
    "staging_songs",
    "fact_songplay",
    "dim_user",
    "dim_song",
    "dim_artist",
    "dim_time"
]