import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This function creates a session with Spark.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function reads the songs JSON files from S3 and processes them with Spark. 
    We separate the files into specific dataframes that represent the tables in our star schema model.
    These tables are saved back to the output folder indicated by output_data parameter.
    """
    
    #song_data = input_data + "song_data/*/*/*"
    song_data = input_data + "song_data/A/B/C/*"
        
    # read song data file
    df = spark.read.json(song_data)   

    # extract columns to create songs table
    songs_table = df.select('song_id', \
                            'title', \
                            'artist_id', \
                            'year', \
                            'duration').distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(path = output_data + 'songs/songs.parquet')

    # extract columns to create artists table
    artists_table =  df.select('artist_id', \
                              'artist_name', \
                              'artist_location', \
                              'artist_latitude', \
                              'artist_longitude').distinct()
    
    # write artists table to parquet files
    artists_table.write.parquet(path = output_data + 'artists/artists.parquet')


def process_log_data(spark, input_data, output_data):
    """
     This function will ingest the data from the log-data path. 
     Additionally, will create the parquets files for the users table and the time table. In the case of the songplays table, it will be joined with the data from the song_path.
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/2018/11/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")
    df.createOrReplaceTempView("logs_data")
    
    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName',
                            'gender', 'level').dropDuplicates()
    users_table.createOrReplaceTempView('users')
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users.parquet", mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    #df = df.withColumn('timestamp', get_timestamp(df.ts))
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    #df = df.withColumn("datetime", get_datetime(col("ts")))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select(
         'timestamp',
         hour('datetime').alias('hour'),
         dayofmonth('datetime').alias('day'),
         weekofyear('datetime').alias('week'),
         month('datetime').alias('month'),
         year('datetime').alias('year'),
         date_format('datetime', 'F').alias('weekday')
     )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(path= output_data + 'time/time.parquet')

    # read in song data to use for songplays table   
    #song_data =  input_data + "song_data/*/*/*.json" 
    song_data = input_data + "song_data/A/B/C/*"
    song_df = spark.read.json(song_data)
    song_df.createOrReplaceTempView("songs_data")
    # extract columns from joined song and log datasets to create songplays table    
    
    songplays_table = spark.sql("""
    SELECT monotonically_increasing_id() AS songplay_id,
        to_timestamp(logs_data.ts/1000) AS start_time,        
        logs_data.userId AS user_id,
        logs_data.level AS level,
        songs_data.song_id AS song_id,
        songs_data.artist_id AS artist_id,
        logs_data.sessionId AS session_id,
        logs_data.location AS location,
        logs_data.userAgent AS user_agent
    FROM logs_data JOIN songs_data ON logs_data.artist = songs_data.artist_name""")
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, 'songplays.parquet'), 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
