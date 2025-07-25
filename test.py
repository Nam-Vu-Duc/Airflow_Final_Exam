import psycopg2
import json
import glob

# postgres
conn = psycopg2.connect(
    dbname="airflow",
    user="airflow",
    password="airflow",
    host="localhost",
    port="5432"
)

class SqlQueries:
    songplay_table_insert = ("""
        SELECT
            md5(events.sessionid || events.start_time) songplay_id,
            events.start_time, 
            events.userid, 
            events.level, 
            songs.song_id, 
            songs.artist_id, 
            events.sessionid, 
            events.location, 
            events.useragent
            FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
        FROM public.staging_events
        WHERE page='NextSong') events
        LEFT JOIN staging_songs songs
        ON events.song = songs.title
            AND events.artist = songs.artist_name
            AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)

cur = conn.cursor()

def check_duplicates(table_name, check_column):
    cur.execute(f"""
        SELECT COUNT(*)
        FROM (
            SELECT {check_column}, COUNT({check_column})
            FROM public.{table_name}
            GROUP BY {check_column}
            HAVING COUNT({check_column}) > 1
        ) AS null_tables
    """)
    count = cur.fetchone()[0]

    if count > 0:
        print(f"Failed: {table_name} has {count} duplicate values.")
    else:
        print(f"Passed: {table_name} has 0 duplicate values.")

check_duplicates('artists', 'artistid')
check_duplicates('songplays', 'playid')
check_duplicates('songs', 'songid')
check_duplicates('time', 'start_time')
check_duplicates('users', 'userid')
