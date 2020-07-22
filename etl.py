import configparser
import os

from pyspark.sql import SparkSession


def create_spark_session():
    """
    Establishes a SparkSession and returns it as an object
    """
    spark_session = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.Apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark_session


def process_song_data(spark, bucket_name):
    """
    Reads song-data from S3; creates song_table and artist_table; and stores them in parquet format.
    """
    df = spark.read.json('/'.join(('s3:/', bucket_name, 'input', 'song-data', '*', '*', '*', '*.json')))
    df.createOrReplaceTempView("song_data")
    song_table = spark.sql(
        '''
        SELECT
            DISTINCT(song_id)   AS song_id,
            title,
            artist_id,
            year,
            duration
        FROM song_data
            WHERE song_id IS NOT NULL
        '''
    )
    song_table.write.partitionBy("year", "artist_id").parquet(
        path='/'.join(('s3:/', bucket_name, 'output', 'song_table.parquet')), mode="overwrite")

    artist_table = spark.sql(
        """
        SELECT  
            DISTINCT(artist_id) AS artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
        FROM song_data
            WHERE artist_id IS NOT NULL
        """
    )
    artist_table.write.parquet(path='/'.join(('s3:/', bucket_name, 'output', 'artist_table.parquet')), mode="overwrite")


def process_log_data(spark, bucket_name):
    """
    Reads log-data from S3; creates users_table, time_table and songplays_table; and stores them in parquet format.
    """
    df = spark.read.json('/'.join(('s3:/', bucket_name, 'input', 'log-data', '*.json')))
    df.createOrReplaceTempView("log_data")

    # filter by actions for song plays
    df = spark.sql(
        """
        SELECT
            *,
            cast(ts/1000 as Timestamp) as timestamp
        FROM log_data
            WHERE page = 'NextSong'
        """
    )
    df.createOrReplaceTempView("log_data")

    # extract columns for users table
    users_table = spark.sql(
        """
        SELECT
            ld.userId              AS user_id,
            ld.firstName,
            ld.lastName,
            ld.gender,
            ld.level
        FROM log_data ld
            INNER JOIN
                (SELECT userId, MAX(ts) as latest_activity_ts
                FROM log_data
                GROUP BY userId) groupped_ld
            ON ld.userId = groupped_ld.userId
            AND ld.ts = groupped_ld.latest_activity_ts
        WHERE ld.userId IS NOT null
        """
    )

    # write users table to parquet files
    users_table.write.parquet(path='/'.join(('s3:/', bucket_name, 'output', 'user_table.parquet')), mode="overwrite")

    # extract columns to create time table
    time_table = spark.sql(
        """
        SELECT
            distinct timestamp          AS start_time,
            hour(timestamp)             AS hour,
            day(timestamp)              AS day,
            weekofyear(timestamp)       AS week,
            month(timestamp)            AS month,
            year(timestamp)             AS year,
            weekday(timestamp)          AS weekday
        FROM log_data
        """
    )

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(
        path='/'.join(('s3:/', bucket_name, 'output', 'time_table.parquet')), mode="overwrite")

    # read in song data to use for songplays table
    song_table = spark.read.parquet('/'.join(('s3:/', bucket_name, 'output', 'song_table.parquet')))
    song_table.createOrReplaceTempView("song_table")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql(
        """
        SELECT
            ld.timestamp    AS start_time,
            ld.userId       AS user_id,
            ld.level,
            st.song_id,
            st.artist_id,
            ld.sessionId    AS session_id,
            ld.location,
            ld.userAgent    AS user_agent,
            year(ld.timestamp) AS year,
            month(ld.timestamp) AS month
        FROM log_data ld
            INNER JOIN song_table st
                ON ld.song = st.title
        """
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(
        path='/'.join(('s3:/', bucket_name, 'output', 'songplays_table.parquet')), mode="overwrite")


def main():
    config = configparser.ConfigParser()
    config.read('dl.cfg')
    os.environ['AWS_ACCESS_KEY_ID'] = config['IAM']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['IAM']['AWS_SECRET_ACCESS_KEY']
    bucket_name = config['S3']['BUCKET_NAME']
    spark = create_spark_session()
    process_song_data(spark, bucket_name)
    process_log_data(spark, bucket_name)
    spark.stop()


if __name__ == "__main__":
    main()
