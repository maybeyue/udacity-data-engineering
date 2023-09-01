"""Provide all of the queries used for the basic analysis of the data
warehouse."""

most_popular_artists = """
    SELECT
        dim_artist.artist_id,
        dim_artist.name,
        COUNT(songplay_id) total_plays
    FROM fact_songplay
    JOIN dim_artist ON dim_artist.artist_id = fact_songplay.artist_id
    GROUP BY dim_artist.artist_id, dim_artist.name
    ORDER BY 3 DESC
    LIMIT 10"""

most_popular_songs = """
    SELECT
        dim_song.song_id,
        dim_song.title,
        COUNT(songplay_id) total_plays
    FROM fact_songplay
    JOIN dim_song ON dim_song.song_id = fact_songplay.song_id
    GROUP BY dim_song.song_id, dim_song.title
    ORDER BY total_plays DESC
    LIMIT 10
"""
most_popular_songs_from_top_artists = f"""
    WITH top_artists AS (
        {most_popular_artists}
    ), song_listens AS (
        SELECT
            fact_songplay.artist_id,
            fact_songplay.song_id,
            COUNT(songplay_id) AS plays,
            RANK() OVER (
                PARTITION BY fact_songplay.artist_id
                ORDER BY COUNT(songplay_id) DESC
            ) AS rank
        FROM fact_songplay
        JOIN top_artists ON top_artists.artist_id = fact_songplay.artist_id
        GROUP BY fact_songplay.artist_id, fact_songplay.song_id
    ), top_song_per_artist AS (
        SELECT
            artist_id,
            song_id,
            plays
        FROM song_listens
        WHERE rank = 1
    )
    SELECT
        dim_artist.name,
        dim_song.title,
        plays
    FROM top_song_per_artist AS top_song
    JOIN dim_song ON dim_song.song_id = top_song.song_id
    JOIN dim_artist ON dim_artist.artist_id = top_song.artist_id
    ORDER BY dim_artist.name, dim_song.title
"""

most_popular_day_of_week = """
    SELECT weekday, COUNT(songplay_id)
    FROM fact_songplay
    JOIN dim_time ON dim_time.start_time = fact_songplay.start_time
    GROUP BY weekday
"""

most_popular_hour = """
    SELECT hour, COUNT(songplay_id)
    FROM fact_songplay
    JOIN dim_time ON dim_time.start_time = fact_songplay.start_time
    GROUP BY hour
"""

user_location = """
    SELECT location, count(DISTINCT session_id) as total_users
    FROM dim_user
    JOIN fact_songplay ON fact_songplay.user_id = dim_user.user_id
    GROUP BY location
    ORDER BY total_users DESC
    LIMIT 5
"""

artist_location = """
    SELECT location, count(artist_id) as total_artists
    FROM dim_artist
    GROUP BY location
    ORDER BY total_artists DESC
    WHERE location != ''
    LIMIT 5
"""

# define power users as the top 10% of listeners on the platform
# based on the total length of listens. Assume people listen to the full song.
# count the total users from each location that are power users
power_user_locations = """
    WITH user_listen_time AS (
        SELECT
            user_id,
            location,
            SUM(duration) AS listen_time
        FROM fact_songplay
        JOIN dim_song ON dim_song.song_id = fact_songplay.song_id
        GROUP BY user_id, location
    ), power_listener AS (
        SELECT
            user_id,
            location,
            listen_time,
            PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY listen_time) OVER ()
                AS listen_percentile
        FROM user_listen_time
    )
    SELECT
        location,
        COUNT(dim_user.user_id) AS total_power_users
    FROM power_listener
    JOIN dim_user ON dim_user.user_id = power_listener.user_id
    WHERE listen_time >= listen_percentile
    GROUP BY location
    ORDER BY total_power_users DESC
    LIMIT 5
"""

analysis_questions = {
    'Who are the most popular artists?': most_popular_artists,
    'What songs are the most popular?': most_popular_songs,
    'What is the most popular song of the top 10 most popular artists?':
        most_popular_songs_from_top_artists,
    'When is the most popular day of week for listening?':
        most_popular_day_of_week,
    'When is the most popular time of day for listening?': most_popular_hour,
    'Where are people listening to music from per session?': user_location,
    'Where are most of our artists located?': artist_location,
    'Where are the most power users from?': power_user_locations
}
