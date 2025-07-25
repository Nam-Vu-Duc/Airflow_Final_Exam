from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import psycopg2
import json
import glob

# postgres
conn = psycopg2.connect(
    dbname="airflow",
    user="airflow",
    password="airflow",
    host="host.docker.internal",
    port="5432"
)

def stage_events():
    file_path = glob.glob("data/log_data/*.json")
    total_records = []
    current_total = 1

    for file in file_path:
        with open(file) as f:
            total_records += [json.loads(line) for line in f]

    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS public.staging_events (
        artist varchar(256),
        auth varchar(256),
        firstname varchar(256),
        gender varchar(256),
        iteminsession int4,
        lastname varchar(256),
        length numeric(18,0),
        "level" varchar(256),
        location varchar(256),
        "method" varchar(256),
        page varchar(256),
        registration numeric(18,0),
        sessionid int4,
        song varchar(256),
        status int4,
        ts int8,
        useragent varchar(256),
        userid int4
    );
    """)
    conn.commit()

    for record in total_records:
        try:
            print(f'Load total: {current_total} records')
            current_total += 1
            if record['userId'] == '': record['userId'] = 0
            cur.execute("""
                INSERT INTO public.staging_events(
                artist,
                auth,
                firstname,
                gender,
                iteminsession,
                lastname,
                length,
                level,
                location,
                method,
                page,
                registration,
                sessionid,
                song,
                status,
                ts,
                useragent,
                userid
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                record['artist'],
                record['auth'],
                record['firstName'],
                record['gender'],
                record['itemInSession'],
                record['lastName'],
                record['length'],
                record['level'],
                record['location'],
                record['method'],
                record['page'],
                record['registration'],
                record['sessionId'],
                record['song'],
                record['status'],
                record['ts'],
                record['userAgent'],
                record['userId']
            ))
            conn.commit()
        except Exception as e:
            print(e)
    return

def stage_songs():
    nested_path = ['A', 'B', 'C']
    total_file_path = []
    total_records = []
    current_total = 0

    for nested in nested_path:
        total_file_path += glob.glob(f"data/song_data/A/A/{nested}/*.json")

    for nested in nested_path:
        total_file_path += glob.glob(f"data/song_data/A/B/{nested}/*.json")

    for file in total_file_path:
        with open(file) as f:
            total_records += [json.loads(line) for line in f]

    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS public.staging_songs (
        num_songs int4,
        artist_id varchar(256),
        artist_name varchar(256),
        artist_latitude numeric(18,0),
        artist_longitude numeric(18,0),
        artist_location varchar(256),
        song_id varchar(256),
        title varchar(256),
        duration numeric(18,0),
        "year" int4
    );
    """)
    conn.commit()

    for record in total_records:
        try:
            print(f'Load total: {current_total} records')
            current_total += 1
            cur.execute("""
                INSERT INTO public.staging_songs(
                num_songs,
                artist_id,
                artist_name,
                artist_latitude,
                artist_longitude,
                artist_location,
                song_id,
                title,
                duration,
                year
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                record['num_songs'],
                record['artist_id'],
                record['artist_name'],
                record['artist_latitude'],
                record['artist_longitude'],
                record['artist_location'],
                record['song_id'],
                record['title'],
                record['duration'],
                record['year'],
            ))
            conn.commit()
        except Exception as e:
            print(e)
    return

def load_songplays_fact_table():
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS public.songplays (
        playid varchar(32) NOT NULL,
        start_time timestamp NOT NULL,
        userid int4 NOT NULL,
        level varchar(256),
        songid varchar(256),
        artistid varchar(256),
        sessionid int4,
        location varchar(256),
        user_agent varchar(256),
        CONSTRAINT songplays_pkey PRIMARY KEY (playid)
    );
    """)
    conn.commit()

    cur.execute("""
        INSERT INTO public.songplays (
            playid,
            start_time,
            userid,
            level,
            songid,
            artistid,
            sessionid,
            location,
            user_agent
        )
        SELECT
            md5(events.sessionid || events.start_time::text) AS playid,
            events.start_time, 
            events.userid, 
            events.level, 
            songs.song_id, 
            songs.artist_id, 
            events.sessionid, 
            events.location, 
            events.useragent
        FROM (
            SELECT 
                TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time,
                *
            FROM public.staging_events
            WHERE page = 'NextSong'
        ) AS events
        LEFT JOIN public.staging_songs AS songs
        ON events.song = songs.title
        AND events.artist = songs.artist_name
        AND events.length = songs.duration;
    """)
    conn.commit()

    print('Load songplays_fact_table successfully')
    return

def load_song_dim_table():
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS public.songs (
        songid varchar(256) NOT NULL,
        title varchar(256),
        artistid varchar(256),
        year int4,
        duration numeric(18,0),
        CONSTRAINT songs_pkey PRIMARY KEY (songid)
    );
    """)
    conn.commit()

    cur.execute("""
        INSERT INTO public.songs (
            songid,
            title,
            artistid,
            year,
            duration
        )
        SELECT distinct song_id, title, artist_id, year, duration
        FROM public.staging_songs
    """)
    conn.commit()

    print('Load song_dim_table successfully')
    return

def load_user_dim_table():
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS public.users (
        userid int4 NOT NULL,
        first_name varchar(256),
        last_name varchar(256),
        gender varchar(256),
        level varchar(256),
        CONSTRAINT users_pkey PRIMARY KEY (userid)
    );
    """)
    conn.commit()

    cur.execute("""
        INSERT INTO public.users (
        userid,
        first_name,
        last_name,
        gender,
        level
        )
        SELECT distinct userid, firstname, lastname, gender, level
        FROM public.staging_events
        WHERE page='NextSong'
        ON CONFLICT (userid) DO NOTHING
    """)
    conn.commit()

    print('Load user_dim_table successfully')
    return

def load_artist_dim_table():
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS public.artists (
        artistid varchar(256) NOT NULL,
        name varchar(256),
        location varchar(256),
        lattitude numeric(18,0),
        longitude numeric(18,0)
    );
    """)
    conn.commit()

    cur.execute("""
       INSERT INTO public.artists (
            artistid,
            name,
            location,
            lattitude,
            longitude
        )
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM public.staging_songs
    """)
    conn.commit()

    print('Load artist_dim_table successfully')
    return

def load_time_dim_table():
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS public.time (
        start_time timestamp NOT NULL,
        hour int4,
        day int4,
        week int4,
        month varchar(256),
        year int4,
        weekday varchar(256),
        CONSTRAINT time_pkey PRIMARY KEY (start_time)
    );
    """)
    conn.commit()

    cur.execute("""
        INSERT INTO public.time (
            start_time,
            hour,
            day,
            week,
            month,
            year,
            weekday
        )
        SELECT 
            start_time, 
            extract(hour from start_time), 
            extract(day from start_time), 
            extract(week from start_time), 
            extract(month from start_time), 
            extract(year from start_time), 
            extract(dow from start_time)
        FROM public.songplays
        ON CONFLICT (start_time) DO NOTHING
    """)
    conn.commit()

    print('Load time_dim_table successfully')
    return

def data_quality_checks():
    def check_empty(table_name):
        cur.execute(f"SELECT COUNT(*) FROM public.{table_name}")
        count = cur.fetchone()[0]

        if count < 1:
            print(f"Failed: {table_name} is empty.")
        else:
            print(f"Passed: {table_name} has {count} rows.")

    def check_null(table_name, check_column):
        cur.execute(f"SELECT COUNT(*) FROM public.{table_name} WHERE {check_column} IS NULL")
        count = cur.fetchone()[0]

        if count > 0:
            print(f"Failed: {table_name} has {count} NULL values.")
        else:
            print(f"Passed: {table_name} has 0 NULL values.")

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

    cur = conn.cursor()

    table_names = ['artists', 'songplays', 'songs', 'time', 'users']
    table_columns = ['artistid', 'playid', 'songid', 'start_time', 'userid']

    for i in range(len(table_names)):
        check_empty(table_names[i])
        check_null(table_names[i], table_columns[i])
        check_duplicates(table_names[i], table_columns[i])

    return

default_args = {
    "Owner":'NamVD13',
    "Depends_on_past":False,
    "Start_date":datetime(2025, 7, 21, 0, 0),
    "Retries":3,
    "Retry_delay":timedelta(minutes=5),
    "catchup":False,
    "email_on_retry":True,
}

with DAG(
    dag_id="TuneStream_ETL",
    default_args=default_args,
    schedule='@hourly',
    tags={"namvd13"},
) as dag:
    # DummyOperator is deprecated and beginning with the version 2.4.0 is not supported any more
    begin_execution = EmptyOperator(
        task_id="begin_execution",
    )

    Stage_songs = PythonOperator(
        task_id="Stage_songs",
        python_callable=stage_songs
    )

    Stage_events = PythonOperator(
        task_id="Stage_events",
        python_callable=stage_events,
    )

    Load_songplays_fact_table = PythonOperator(
        task_id="Load_songplays_fact_table",
        python_callable=load_songplays_fact_table,
    )

    Load_song_dim_table = PythonOperator(
        task_id="Load_song_dim_table",
        python_callable=load_song_dim_table,
    )

    Load_user_dim_table = PythonOperator(
        task_id="Load_user_dim_table",
        python_callable=load_user_dim_table,
    )

    Load_artist_dim_table = PythonOperator(
        task_id="Load_artist_dim_table",
        python_callable=load_artist_dim_table,
    )

    Load_time_dim_table = PythonOperator(
        task_id="Load_time_dim_table",
        python_callable=load_time_dim_table,
    )

    data_quality_checks = PythonOperator(
        task_id="data_quality_checks",
        python_callable=data_quality_checks,
    )

    End_execution = EmptyOperator(
        task_id="End_execution",
    )

    begin_execution >> [Stage_songs, Stage_events] >> Load_songplays_fact_table >> [Load_song_dim_table, Load_user_dim_table, Load_artist_dim_table, Load_time_dim_table] >> data_quality_checks >> End_execution