## Sparkify Data Lake Project with PySpark

### Summary of the project
Data sources:
1. Song json files containing song and artist information
2. Log file containing user,time and songplay session info

In this project, we're going to build an ETL pipeline that will be able to extract song and log data from an S3 bucket, 
process the data using Spark and load the data back into s3 as a set of dimensional tables in spark parquet files. 

Steps:
--
 1. We're populating Songs and artist table from songs data.  
 2. Use these two tables to populate values to songplay table by joining both. 
 3. Extract user information from these log files. We derive a separate time table for future 
    analysis based on the timestamp data from logs.e.g. the source data for timestamp is in unix format and 
    that will need to be converted to timestamp from which we can extract useful info.
 4. There might be duplicate data , Hence based on certain subsets we have to remove the duplicate rows

Schema
--
We follow start schema for our ELT process using Spark which will contain clean data that is suitable for OLAP(Online Analytical 
Processing) operations
```
DIMENSIONAL TABLE :
T_SONG => song_id String, title String, artist_id String, year int, duration double
T_ARTIST => artist_id String, artist_name String, location String, latitude decimal, longitude decimal
T_TIME_TABLE => start_time timestamp, day int, week int, month int, year int, weekday int
T_USER => user_id long, first_name String, last_name String, gender String, level String

FACT TABLE :
T_SONG_PLAY => songplay_id int,start_time timestamp, user_id long, level String,song_id String, artist_id String, session_id int, location String, user_agent String
```

Datasets used
--
The datasets used are retrieved from the udacity s3 bucket and hold files in a JSON format. 
There are two datasets: log_data and song_data. The song_data dataset is a subset of the the 
Million Song Dataset while the log_data contains generated log files based on the songs in song_data.

Project Files
--
### etl.py
This script once executed retrieves the song and log data from the s3 bucket,
transforms the data into fact and dimensional tables then loads the table data back into s3 as parquet files. We use
partitions while saving to s3 for easier read operations.

### dl.cfg
It contains our AWS keys. (Replaced with dummy key value for security)

Replace your AWS access and secret key in the config file in the following format (don't use quotes)
```
[AWS] 
AWS_ACCESS_KEY_ID = your aws key 
AWS_SECRET_ACCESS_KEY = your aws secret key
```

To run the script:

cmd : 
```
spark-submit --packages org.apache.hadoop:hadoop-aws:2.7.0 etl.py
```