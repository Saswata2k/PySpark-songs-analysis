import configparser
import os
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']
os.environ["PYSPARK_SUBMIT_ARGS"] = "--master pyspark-shell --driver-memory 2g"


class SparkifyETL:

    def __init__(self):
        self.data_song = None
        self.data_log = None

    def create_spark_session(self):
        spark = SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.2") \
            .getOrCreate()
        # .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        # .config("spark.hadoop.fs.s3a..access.key", "KEY") \
        # .config("spark.hadoop.fs.s3a..secret.key", "KEY") \
        return spark

    def process_song_data(self, spark, input_data, output_data):
        # get filepath to song data file
        file_path = os.path.join("data", "song_data", "*", "*", "*")

        # read song data file
        self.data_song = spark.read.json(input_data + file_path)

        # extract columns to create songs table
        df_song = self.data_song.select("song_id", "title", "artist_id", "year", "duration")
        df_song = df_song.withColumn("year", F.col("year").cast(T.IntegerType()))

        # write songs table to parquet files partitioned by year and artist
        df_song.write.mode("overwrite").parquet(output_data + "song.parquet")

        # extract columns to create artists table
        df_artist = self.data_song.select("artist_id", "artist_name", "artist_location", "artist_latitude",
                                          "artist_longitude")
        df_artist = df_artist.withColumn("artist_latitude", F.col("artist_latitude").cast(T.DecimalType()))
        df_artist = df_artist.withColumn("artist_longitude", F.col("artist_longitude").cast(T.DecimalType()))

        # write artists table to parquet files
        df_artist.write.mode("overwrite").parquet(output_data + "artist.parquet")

    def process_log_data(self, spark, input_data, output_data):
        # get filepath to log data file
        file_path = os.path.join("data", "log-data")

        # read log data file
        self.data_log = spark.read.json(file_path)

        # filter by actions for song plays
        self.data_log = self.data_log.where(F.col("page") == "NextSong")

        # extract columns for users table
        df_user = self.data_log.select("userId", "firstName", "lastName", "gender", "level")
        df_user = df_user.withColumn("userId", F.expr("cast(userId as long) userId"))

        # write users table to parquet files
        df_user.write.mode("overwrite").parquet(output_data + "user.parquet")

        # create timestamp column from original timestamp column
        df_time = self.data_log.select("ts")
        time_format = "yyyy-MM-dd' 'HH:mm:ss.SSS"

        df_time = self.data_log.withColumn("start_time",
                                           F.to_utc_timestamp(
                                               F.from_unixtime(self.data_log.ts / 1000, format=time_format),
                                               tz="UTC"))
        df_time = df_time.withColumn("hour", F.hour(F.col("start_time")))
        df_time = df_time.withColumn("day", F.dayofmonth(F.col("start_time")))
        df_time = df_time.withColumn("week", F.weekofyear(F.col("start_time")))
        df_time = df_time.withColumn("month", F.month(F.col("start_time")))
        df_time = df_time.withColumn("year", F.year(F.col("start_time")))
        df_time = df_time.withColumn("weekday", F.dayofweek(F.col("start_time")))

        # write time table to parquet files partitioned by year and month
        df_time.write.mode("overwrite").parquet(output_data + "time.parquet")

        # extract columns from joined song and log datasets to create songplays table
        self.data_log.createOrReplaceTempView("t_log")
        self.data_song.createOrReplaceTempView("t_song")
        df_time.createOrReplaceTempView("t_time")

        df_song_play = spark.sql("select t_time.start_time, t_log.userId, t_log.level, \
                                  t_song.song_id, t_song.artist_id, t_log.sessionId, \
                                  t_log.location, t_log.userAgent \
                                  from t_log  \
                                  inner join t_song \
                                  on t_log.song=t_song.title \
                                  inner join t_time \
                                  on t_time.ts = t_log.ts \
                                  where t_log.artist = t_song.artist_name \
                                  and song_id is not null \
                                  ")

        # write songplays table to parquet files partitioned by year and month
        df_song_play.write.mode("overwrite").parquet(output_data + "songplay.parquet")


def main():
    sparkify = SparkifyETL()
    spark = sparkify.create_spark_session()
    input_data = ""
    # input_data = "s3a://udacity-dend/"
    # output_data = ""
    output_data = "s3a://sparkify-saswata/data/"
    sparkify.process_song_data(spark, input_data, output_data)
    sparkify.process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
