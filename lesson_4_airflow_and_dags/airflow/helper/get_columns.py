""" Gets necessary columns to define table
        and assist in insertion of columns."""

stage_events_fields = {
    'artist': 'character varying(150)',
    'auth': 'character varying(10)',
    'firstName': 'character varying(20)',
    'gender': 'character(1)',
    'itemInSession': 'integer',
    'lastName': 'character varying(20)',
    'length': 'numeric(10,5)',
    'level': 'character varying(4)',
    'location': 'character varying(100)',
    'method': 'character varying(3)',
    'page': 'character varying(25)',
    'registration': 'numeric(20,6)',
    'sessionId': 'integer',
    'song': 'character varying(200)',
    'status': 'integer',
    'ts': 'numeric(20,6)',
    'userAgent': 'character varying(150)',
    'userId': 'integer'
}

stage_songs_fields = {
        'num_songs': 'integer',
        'artist_id': 'character varying(20)',
        'artist_latitude': 'numeric(10,5)',
        'artist_longitude': 'numeric(10,5)',
        'artist_location': 'character varying(300)',
        'artist_name': 'character varying(400)',
        'song_id': 'character varying(20)',
        'title': 'character varying(200)',
        'duration': 'numeric(10,5)',
        'year': 'integer'
}

fact_songplay_fields = {
        'songplay_id': 'character varying(32) PRIMARY KEY',
        'start_time': 'timestamp NOT NULL',
        'user_id': 'integer NOT NULL',
        'level': 'character varying(4)',
        'song_id': 'character varying(20)',
        'artist_id': 'character varying(20)',
        'session_id': 'integer',
        'location': 'character varying(100)',
        'user_agent': 'character varying(150)'
}

dim_user_fields = {
        'user_id': 'integer PRIMARY KEY',
        'first_name': 'character varying(20)',
        'last_name': 'character varying(20)',
        'gender': 'character(1)',
        'level': 'character varying(4)'
}

dim_song_fields = {
        'song_id': 'character varying(20) PRIMARY KEY',
        'title': 'character varying(200)',
        'artist_id': 'character varying(20)',
        'year': 'integer',
        'duration': 'numeric(10,5)'
}

dim_artist_fields = {
        'artist_id': 'character varying(20) PRIMARY KEY',
        'name': 'character varying(400)',
        'location': 'character varying(300)',
        'latitude': 'numeric(10,5)',
        'longitude': 'numeric(10,5)'
}

dim_time_fields = {
        'start_time': 'timestamp PRIMARY KEY',
        'hour': 'integer',
        'day': 'integer',
        'week': 'integer',
        'month': 'integer',
        'year': 'integer',
        'weekday': 'integer'
}


def get_insertion_columns(columns):
    return ',\n'.join(columns.keys())


def get_create_table_def(columns):
    define_column_list = [
        f"{column} {col_type}"
        for column, col_type in columns.items()
    ]
    return ',\n'.join(define_column_list)
