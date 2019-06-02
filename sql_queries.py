import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplay"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES


staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        artist          VARCHAR(2000),
        auth            VARCHAR,
        firstname       VARCHAR,
        gender          VARCHAR,
        iteminsession   INTEGER,
        lastname        VARCHAR,
        length          FLOAT,
        level           VARCHAR,
        location        VARCHAR(2000),
        method          VARCHAR,
        page            VARCHAR,
        registration    FLOAT,
        sessionid       INTEGER,
        song            VARCHAR(2000),
        status          INTEGER,
        ts              TIMESTAMP,
        useragent       VARCHAR,
        userid          integer
    )      
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_song            INTEGER,
        artist_id           VARCHAR(2000),
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     VARCHAR(2000),
        artist_name         VARCHAR(2000),
        song_id             VARCHAR, 
        title               VARCHAR,
        duration            FLOAT,
        year                INTEGER
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay (
        songplay_id     INTEGER IDENTITY(0,1) PRIMARY KEY SORTKEY,
        start_time      TIMESTAMP NOT NULL,
        user_id         INTEGER NOT NULL,
        level           VARCHAR,
        song_id         VARCHAR NOT NULL,
        artist_id       VARCHAR(2000) NOT NULL,
        session_id      INTEGER,
        location        VARCHAR(2000),
        user_agent      VARCHAR
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id     INTEGER PRIMARY KEY DISTKEY,
        first_name  VARCHAR,
        last_name   VARCHAR,
        gender      CHAR,
        level       VARCHAR
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id     VARCHAR PRIMARY KEY,
        title       VARCHAR(2000),
        artist_id   VARCHAR DISTKEY NOT NULL,
        year        INTEGER,
        duration    FLOAT
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id   VARCHAR PRIMARY KEY DISTKEY,
        name        VARCHAR(2000),
        location    VARCHAR(2000),
        latitude    FLOAT,
        longitude   FLOAT
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time  TIMESTAMP PRIMARY KEY SORTKEY DISTKEY,
        hour        INTEGER,
        day         INTEGER,
        week        INTEGER,
        month       INTEGER,
        year        INTEGER,
        weekday     INTEGER
    )
""")
# STAGING TABLES
LOG_DATA = config.get('S3', 'LOG_DATA')
SONG_DATA = config.get('S3', 'SONG_DATA')
ARN = config.get('IAM_ROLE', 'ARN')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
#REGION = config.get('DWH', 'DWH_REGION')
REGION = 'us-west-2'

staging_events_copy = ("""
    COPY staging_events FROM {} 
    iam_role '{}' 
    region '{}' 
    FORMAT AS JSON {} timeformat 'epochmillisecs';
""").format(LOG_DATA, ARN, REGION, LOG_JSONPATH)


staging_songs_copy = ("""
    COPY staging_songs FROM {} 
    iam_role '{}' 
    region '{}' 
    FORMAT AS JSON 'auto';
""").format(SONG_DATA, ARN, REGION)

#staging_events_copy = ("""
#    COPY {} FROM {} 
#    credentials 'aws_iam_role={}'
#    region 'us-west-2' delimiter ',' CSV IGNOREHEADER 1;
#""").format("staging_events", config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'))


#staging_songs_copy = ("""
#    COPY {} FROM {} 
#    credentials 'aws_iam_role={}'
#    region 'us-west-2' delimiter ',' CSV IGNOREHEADER 1;
#""").format("staging_songs", config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES
#SELECT DISTINCT to_timestamp(to_char(ev.ts, '9999-99-99 99:99:99'), 'YYYY-MM-DD HH24:MI:SS'),
songplay_table_insert = ("""
    INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT ev.ts,
                    ev.userId,
                    ev.level,
                    s.song_id,
                    s.artist_id,
                    ev.sessionid,
                    ev.location,
                    ev.userAgent
    FROM staging_events as ev 
    JOIN staging_songs s ON ev.song = s.title 
                        and ev.artist = s.artist_name
                        and ev.page = 'NextSong';
""")


user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId,
                    firstName,
                    lastName,
                    gender,
                    level
    FROM staging_events
    WHERE userId IS NOT NULL
    and   page = 'NextSong';
""")


song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id,
                    title,
                    artist_id,
                    year,
                    duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")


artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id,
                    artist_name,
                    artist_location,
                    artist_latitude,
                    artist_longitude
    FROM staging_songs
    where artist_id IS NOT NULL;
""")


time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT start_time,
                    EXTRACT(hour FROM start_time),
                    EXTRACT(day FROM start_time),
                    EXTRACT(week FROM start_time),
                    EXTRACT(month FROM start_time),
                    EXTRACT(year FROM start_time),
                    EXTRACT(weekday FROM start_time)
    FROM songplay
    WHERE start_time IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
