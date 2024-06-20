import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# Global Variables 

LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE = config.get("IAM_ROLE", "ARN")


# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXITS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INTEGER,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAr,
    registration FLOAT,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts numeric,
    userAgent VARCHAR,
    userId INTEGER
);""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year INTEGER
);""")

songplay_table_create = ("""
CREAT TABLE IF NOT EXISTS songplay
(
 songplay_id             INTEGER IDENTITY(0,1),
 start_time              TIMESTAMP,
 user_id                 INTERGER,
 level                   VARCHAR,
 song_id                 VARCHAR,
 artist_id               VARCHAR,
 session_id              INTERGER,
 location                VARCHAR,
 user_agent              VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
user_id       INTEGER PRIMARY KEY,
first_name    VARCHAR,
last_name     VARCHAR,
gender        VARCHAR,
level         VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
song_id        VARCHAR PRIMARY KEY,
title          VARCHAR,
artist_id      VARCHAR,
year           INTEGER,
duration       FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
artist_id        VARCHAR PRIMARY KEY,
name             VARCHAR,
location         VARCHAR,
latitude         FLOAT,
longitude        FLOAT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
start_time      TIMESTAMP PRIMARY KEY,
hour            INTEGER,
day             INTEGER,
week            INTEGER,
month           INTEGER,
year            INTEGER,
weekday         VARCHAR
);
""")

# STAGING TABLES
staging_events_copy = (f"""
    COPY staging_events FROM {LOG_DATA}
    CREDENTIALS 'aws_iam_role={IAM_ROLE}'
    REGION 'us-east-1'
    FORMAT AS JSON {LOG_JSONPATH}
    TIMEFORMAT AS 'epochmillisecs'
""")

staging_songs_copy = (f"""
    COPY staging_songs FROM {SONG_DATA}
    CREDENTIALS 'aws_iam_role={IAM_ROLE}'
    REGION 'us-east-1'
    FORMAT AS JSON 'auto'
    TRUNCATECOLUMNS
    BLANKSASNULL
    EMPTYASNULL
""")

# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT
    TIMESTAMP 'epoch' + se.ts / 1000 * INTERVAL '1 second' AS start_time,
    se.userid,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionid,
    se.location,
    se.useragent
FROM staging_events se
JOIN staging_songs ss ON ss.artist_name = se.artist AND se.page = 'NextSong' AND se.song = ss.title
""")


user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT
    se3.userid,
    se3.firstname,
    se3.lastname,
    se3.gender,
    se3.level
FROM
    staging_events se3
JOIN (
    SELECT
        userid,
        MAX(ts) AS max_time_stamp
    FROM
        staging_events
    WHERE
        page = 'NextSong'
    GROUP BY
        userid
) se2 ON se3.userid = se2.userid AND se3.ts = se2.max_time_stamp
WHERE
    se3.page = 'NextSong';
""")
 
song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM
    staging_songs;
""")
 
artist_table_insert = ("""
INSERT INTO artists (artist_id , name, location, latitude, longitude)
SELECT DISTINCT 
    artist_id
    ,artist_name
    ,artist_location
    ,artist_latitude
    ,artist_longitude
FROM staging_songs;
""")
 
time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
    TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
    EXTRACT (hour FROM start_time),
    EXTRACT (day FROM start_time),
    EXTRACT (week FROM start_time),
    EXTRACT (month FROM start_time),
    EXTRACT (year FROM start_time),
    EXTRACT (weekday FROM start_time)
FROM staging_events
WHERE ts IS NOT NULL;
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop] 
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]