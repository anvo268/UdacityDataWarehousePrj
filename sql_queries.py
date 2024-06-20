import configparser


# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

# DROP TABLES
staging_events_table_drop = """--sql 
    DROP TABLE IF EXISTS stg_song_events;
"""
staging_songs_table_drop = """--sql 
    DROP TABLE IF EXISTS stg_songs;
"""
songplay_table_drop = """--sql 
    DROP TABLE IF EXISTS fct_song_plays;
"""
user_table_drop = """--sql 
    DROP TABLE IF EXISTS dim_users;
"""
song_table_drop = """--sql 
    DROP TABLE IF EXISTS dim_songs;
"""
artist_table_drop = """--sql 
    DROP TABLE IF EXISTS dim_artists;
"""
time_table_drop = """--sql 
    DROP TABLE IF EXISTS dim_time_dimensions;
"""

# CREATE TABLES

staging_events_table_create = """--sql
CREATE TABLE stg_song_events (
    artist TEXT,
    auth TEXT,
    firstName TEXT,
    gender CHAR(1),
    itemInSession INT,
    lastName TEXT,
    length FLOAT,
    level TEXT,
    location TEXT,
    method TEXT,
    page TEXT,
    registration BIGINT,
    sessionId INT,
    song TEXT,
    status INT,
    ts BIGINT,
    userAgent TEXT,
    userId INT
);        
"""

staging_songs_table_create = """--sql
CREATE TABLE stg_songs (
    num_songs INTEGER,
    artist_id VARCHAR(50),
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location TEXT,
    artist_name TEXT,
    song_id VARCHAR(50),
    title TEXT,
    duration FLOAT,
    year INTEGER
);
"""

songplay_table_create = """--sql
CREATE TABLE fct_song_plays (
    auth TEXT,
    item_in_session INT sortkey,
    LEVEL TEXT,
    location TEXT,
    session_id INT,
    song_id TEXT distkey,
    user_id INT, 
    time_key INT,
    ts TIMESTAMP

)
"""

user_table_create = """--sql
CREATE TABLE dim_users (
    firstName TEXT,
    lastName TEXT,
    gender CHAR(1),
    level TEXT,
    registration BIGINT sortkey,
    user_id INT 
)
DISTSTYLE ALL
"""

song_table_create = """--sql
CREATE TABLE dim_songs (
    song_id TEXT distkey,
    title TEXT sortkey,
    duration FLOAT,
    year INT
);
"""

artist_table_create = """--sql
CREATE TABLE dim_artists (
    artist_id TEXT distkey,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location TEXT,
    artist_name TEXT sortkey
)
"""

time_table_create = """--sql
CREATE TABLE dim_time_dimensions (
    time_key int distkey,
    hour int,
    date DATE NOT NULL sortkey,
    day SMALLINT NOT NULL,
    week SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    quarter SMALLINT NOT NULL,
    year SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    fiscal_year SMALLINT NOT NULL,
    fiscal_quarter SMALLINT NOT NULL,
    season TEXT,
    special_event TEXT
)
"""

# STAGING TABLES

staging_events_copy = """--sql
COPY stg_song_events
FROM 's3://udacity-dend/log_data' CREDENTIALS 'aws_iam_role={}' JSON 's3://udacity-dend/log_json_path.json' REGION 'us-west-2';
""".format(
    config.get("IAM_ROLE", "ARN")
)

staging_songs_copy = """--sql
copy stg_songs
FROM 's3://udacity-dend/song_data' credentials 'aws_iam_role={}' json 'auto' region 'us-west-2';
    """.format(
    config.get("IAM_ROLE", "ARN")
)


# FINAL TABLES

songplay_table_insert = """--sql
INSERT INTO fct_song_plays
SELECT auth,
    iteminsession AS item_in_session,
    LEVEL,
    location,
    sessionid AS session_id,
    songs.song_id,
    userid AS user_id,
    CAST(
        TO_CHAR(
            date_trunc(
                'hour',
                TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second'
            ),
            'YYYYMMDDHH24'
        ) AS bigint
    ) AS time_key,
    TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second' AS ts
FROM stg_song_events EVENTS
    -- Join on songs to get song_id. Left join because many song titles don't appear in the songs collection
    -- This is something as I DE I'd like to verify the cause of w/ the source data owners
    LEFT JOIN stg_songs songs ON (
        EVENTS.artist = songs.artist_name
        AND EVENTS.song = songs.title
    )
WHERE EVENTS.page = 'NextSong' -- I'm inferring that "NextSong" leads to a play event
"""

user_table_insert = """--sql
INSERT INTO dim_users
SELECT DISTINCT firstname,
    lastname,
    gender,
    LEVEL,
    registration,
    userid
FROM stg_song_events 
"""

song_table_insert = """--sql
INSERT INTO dim_songs
SELECT DISTINCT song_id,
    title,
    duration,
    year
FROM stg_songs
"""

artist_table_insert = """--sql
INSERT INTO dim_artists
SELECT DISTINCT artist_id,
    artist_latitude,
    artist_longitude,
    artist_location,
    artist_name
FROM stg_songs
"""

# I don't 100% get why I'm bothering with this if I can always just derive the time dimensions from the timestamp
time_table_insert = """--sql
INSERT INTO dim_time_dimensions
WITH unique_times AS (
    SELECT DISTINCT 
        date_trunc('hour', TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS trunc_time
    FROM (select * from stg_song_events limit 100)
)
SELECT 
    CAST(TO_CHAR(trunc_time, 'YYYYMMDDHH24') AS bigint) AS time_key,
    CAST(SUBSTRING(CAST(trunc_time AS text) FROM 9 FOR 2) AS integer) AS hour,
    DATE(trunc_time) AS date,
    EXTRACT(
        DAY
        FROM trunc_time
    ) AS DAY,
    EXTRACT(
        WEEK
        FROM trunc_time
    ) AS week,
    EXTRACT(
        MONTH
        FROM trunc_time
    ) AS MONTH,
    EXTRACT(
        QUARTER
        FROM trunc_time
    ) AS quarter,
    EXTRACT(
        YEAR
        FROM trunc_time
    ) AS year,
    EXTRACT(
        DOW
        FROM trunc_time
    ) + 1 AS day_of_week,
    -- Redshift DOW: 0=Sunday, ..., 6=Saturday. Adding 1 to make it 1=Sunday, ..., 7=Saturday.
    CASE
        WHEN EXTRACT(
            DOW
            FROM trunc_time
        ) IN (0, 6) THEN TRUE
        ELSE FALSE
    END AS is_weekend,
    CASE
        WHEN EXTRACT(
            MONTH
            FROM trunc_time
        ) BETWEEN 1 AND 3 THEN EXTRACT(
            YEAR
            FROM trunc_time
        ) - 1
        ELSE EXTRACT(
            YEAR
            FROM trunc_time
        )
    END AS fiscal_year,
    CASE
        WHEN EXTRACT(
            MONTH
            FROM trunc_time
        ) BETWEEN 1 AND 3 THEN 4
        WHEN EXTRACT(
            MONTH
            FROM trunc_time
        ) BETWEEN 4 AND 6 THEN 1
        WHEN EXTRACT(
            MONTH
            FROM trunc_time
        ) BETWEEN 7 AND 9 THEN 2
        ELSE 3
    END AS fiscal_quarter,
    CASE
        WHEN EXTRACT(
            MONTH
            FROM trunc_time
        ) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(
            MONTH
            FROM trunc_time
        ) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(
            MONTH
            FROM trunc_time
        ) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Fall'
    END AS season,
    NULL AS special_event -- Placeholder for any special events logic
FROM unique_times
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
