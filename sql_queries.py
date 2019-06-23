import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS staging_events (artist varchar, \
                                                            auth varchar NOT NULL, \
                                                            firstName varchar, \
                                                            gender varchar, \
                                                            itemInSession int, \
                                                            lastName varchar, \
                                                            length numeric, \
                                                            level varchar NOT NULL, \
                                                            location varchar, \
                                                            method varchar NOT NULL, \
                                                            page varchar NOT NULL, \
                                                            registration bigint, \
                                                            sessionId int, \
                                                            song varchar, \
                                                            status int NOT NULL, \
                                                            ts bigint NOT NULL distkey sortkey, \
                                                            userAgent text, \
                                                            userId int)""")


staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (num_songs int, \
                                                                   artist_id varchar NOT NULL, \
                                                                   artist_latitude numeric, artist_longitude numeric, artist_location varchar, \
                                                                   artist_name varchar NOT NULL, \
                                                                   song_id varchar PRIMARY KEY distkey sortkey, \
                                                                   title varchar NOT NULL, \
                                                                   duration numeric NOT NULL, \
                                                                   year int NOT NULL)""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id int identity (0,1) PRIMARY KEY distkey, \
                                                                  start_time bigint NOT NULL sortkey, \
                                                                  user_id int NOT NULL, \
                                                                  level varchar, \
                                                                  song_id varchar, \
                                                                  artist_id varchar, \
                                                                  session_id int NOT NULL, \
                                                                  location varchar, \
                                                                  user_agent varchar)""")


user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id int PRIMARY KEY, \
                                                          first_name varchar, \
                                                          last_name varchar, \
                                                          gender varchar, \
                                                          level varchar NOT NULL)
                                                          diststyle all""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY distkey, \
                                                          title varchar NOT NULL, \
                                                          artist_id varchar NOT NULL, \
                                                          year int NOT NULL, \
                                                          duration int NOT NULL)""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY, \
                                                              name varchar NOT NULL, \
                                                              location varchar, \
                                                              latitude numeric, \
                                                              longitude numeric)
                                                              diststyle all""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time bigint PRIMARY KEY sortkey, \
                                                         hour int NOT NULL, \
                                                         day int NOT NULL, \
                                                         week int NOT NULL, \
                                                         month int NOT NULL, \
                                                         year int NOT NULL, \
                                                         weekday int NOT NULL)
                                                         diststyle all""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from {}
    credentials 'aws_iam_role={}'
    format as json {} region 'us-west-2';
    """).format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""copy staging_songs from {}
    credentials 'aws_iam_role={}'
    format as json 'auto' region 'us-west-2';
    """).format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
                            SELECT staging_events.ts,
                                   staging_events.userid,
                                   staging_events.level,
                                   songs.song_id,
                                   artists.artist_id,
                                   staging_events.sessionid,
                                   staging_events.location,
                                   staging_events.useragent
                            FROM staging_events
                            LEFT JOIN songs ON songs.title = staging_events.song
                            LEFT JOIN artists ON artists.name = staging_events.artist
                            WHERE staging_events.page = 'NextSong'""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT userId, firstName, lastName, gender, level FROM staging_events t1
                        WHERE t1.ts = (SELECT MAX(t2.ts) 
                                       FROM staging_events t2 WHERE t2.userid = t1.userid)""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) 
                        SELECT DISTINCT song_id, title, artist_id, year, duration FROM staging_songs""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
                          SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM staging_songs""")

# Adopted from "https://knowledge.udacity.com/questions/47005"
time_table_insert = ("""INSERT INTO time(start_time,hour,day,week,month,year,weekday)
                        SELECT DISTINCT ts,
                                EXTRACT(HOUR FROM start_time) AS hour,
                                EXTRACT(DAY FROM start_time) AS day,
                                EXTRACT(WEEK FROM start_time) AS week,
                                EXTRACT(MONTH FROM start_time) AS month,
                                EXTRACT(YEAR FROM start_time) AS year,
                                EXTRACT(DOW FROM start_time) AS weekday
                        FROM (SELECT distinct ts, '1970-01-01'::date + ts/1000 * interval '1 second' as start_time
                              FROM staging_events)""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]