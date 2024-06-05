-- Create a warehouse
CREATE WAREHOUSE SPOTIFYWH;

-- Create and use a database
CREATE OR REPLACE DATABASE SPOTIFY_DB;
USE SPOTIFY_DB;

-- Create a schema
CREATE OR REPLACE SCHEMA SPOTIFY_SCHEMA;

-- Create a CSV file format
CREATE OR REPLACE FILE FORMAT SPOTIFY_SCHEMA.CSV_FILE_FORMAT
    TYPE = CSV 
    FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'NULL')
    EMPTY_FIELD_AS_NULL = TRUE;

-- Create a stage
CREATE OR REPLACE STAGE SPOTIFY_SCHEMA.SPOTIFY_STAGE
    URL='--BUCKET URL--'
    CREDENTIALS=(AWS_KEY_ID='XXX' AWS_SECRET_KEY='XXX')
    FILE_FORMAT=SPOTIFY_SCHEMA.CSV_FILE_FORMAT;

-- Create tables
CREATE OR REPLACE TABLE SPOTIFY_SCHEMA.ALBUM
(
    ALBUM_ID STRING,
    ALBUM_NAME STRING,
    ALBUM_RELEASE_DATE DATE,
    ALBUM_TOTAL_TRACKS INT,
    ALBUM_URL STRING
);

CREATE OR REPLACE TABLE SPOTIFY_SCHEMA.ARTIST 
(
    ARTIST_ID STRING,
    ARTIST_NAME STRING,
    ARTIST_LINK STRING
);

CREATE OR REPLACE TABLE SPOTIFY_SCHEMA.SONGS 
(
    SONG_ID STRING,
    SONG_NAME STRING,
    SONG_DURATION INT,
    SONG_POPULARITY INT,
    TRACK_NUM BIGINT
);

-- Create pipes with PATTERN attribute to match specific file types
CREATE OR REPLACE PIPE SPOTIFY_SCHEMA.ALBUM_PIPE 
AUTO_INGEST = TRUE 
AS 
COPY INTO SPOTIFY_SCHEMA.ALBUM
    FROM @SPOTIFY_SCHEMA.SPOTIFY_STAGE
    FILE_FORMAT = SPOTIFY_SCHEMA.CSV_FILE_FORMAT
    PATTERN = '.*album.*\\.csv';

CREATE OR REPLACE PIPE SPOTIFY_SCHEMA.ARTIST_PIPE 
AUTO_INGEST = TRUE 
AS 
COPY INTO SPOTIFY_SCHEMA.ARTIST
    FROM @SPOTIFY_SCHEMA.SPOTIFY_STAGE
    FILE_FORMAT = SPOTIFY_SCHEMA.CSV_FILE_FORMAT
    PATTERN = '.*artist.*\\.csv';

CREATE OR REPLACE PIPE SPOTIFY_SCHEMA.SONGS_PIPE 
AUTO_INGEST = TRUE 
AS 
COPY INTO SPOTIFY_SCHEMA.SONGS
    FROM @SPOTIFY_SCHEMA.SPOTIFY_STAGE
    FILE_FORMAT = SPOTIFY_SCHEMA.CSV_FILE_FORMAT
    PATTERN = '.*songs.*\\.csv';

-- Select data from tables to verify ingestion
SELECT * FROM SPOTIFY_SCHEMA.ALBUM;
SELECT * FROM SPOTIFY_SCHEMA.ARTIST;
SELECT * FROM SPOTIFY_SCHEMA.SONGS;