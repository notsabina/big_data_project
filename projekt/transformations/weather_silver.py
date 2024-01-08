import sys

import re
from datetime import datetime
from functools import reduce


from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, BooleanType
import findspark

import logging

import os

import requests

def delete_files_with_prefix(hdfs_path, prefix):
    webhdfs_url = "http://localhost:50070/webhdfs/v1"

    list_status_url = f"{webhdfs_url}{hdfs_path}?op=LISTSTATUS"
    response = requests.get(list_status_url)
    
    if response.status_code == 200:
        files = response.json()["FileStatuses"]["FileStatus"]

        for file in files:
            file_path = file["pathSuffix"]
            if file_path.startswith(prefix):
                delete_url = f"{webhdfs_url}{hdfs_path}/{file_path}?op=DELETE"
                response = requests.delete(delete_url)

                if response.status_code == 200:
                    print(f"Deleted '{file_path}'")
                else:
                    print(f"Error deleting '{file_path}'. Response: {response.text}")
    else:
        print(f"Error. Response: {response.text}")



def status_to_boolean(column):
    return F.when(F.col(column).isNull(), F.lit(True).cast(BooleanType())).otherwise(F.lit(True).cast(BooleanType()))

def status_to_text(column):
    """
    ... because there are three possible values
    it is what it is
    """
    return (
        F.when(F.col(column).isNull(), F.lit('Y'))
        .when(F.col(column) == F.lit(8), F.lit('N'))
        .otherwise(F.lit('X'))   
    )

    


def main():
    findspark.init()
    # # Logging configuration
    # formatter = logging.Formatter('[%(asctime)s] %(levelname)s @ line %(lineno)d: %(message)s')
    # handler = logging.StreamHandler(sys.stdout)
    # handler.setLevel(logging.INFO)
    # handler.setFormatter(formatter)
    # logger = logging.getLogger()
    # logger.setLevel(logging.INFO)
    # logger.addHandler(handler)

   
    spark = SparkSession.builder.appName("weather_silver").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    # logger.info("Starting spark application")

    # logger.info("Loading k_d_t files")
    # logger.info(f"k_d_t table: {k_d_t_weather.show(10)}")
    
    
    # logger.info("Loading k_d files")
    # logger.info(f"k_d table: {k_d_weather.show(10)}")


    hdfs_path = "hdfs://localhost:8020/user/projekt/bronze/stage/pogoda"
    k_d_pattern = "k_d_[0-9]*.csv"
    k_d_t_pattern = "k_d_t_[0-9]*.csv"

    # Read k_d_t_ files into a DataFrame
    WEATHER_SCHEMA_k_d_t = StructType([
        StructField("station_code", IntegerType(), True),
        StructField("station_name", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True),
        StructField("temperature_avg", DoubleType(), True),
        StructField("temperature_avg_status", DoubleType(), True),
        StructField("humidity_avg", DoubleType(), True),
        StructField("humidity_avg_status", DoubleType(), True),
        StructField("wind_speed_avg", DoubleType(), True),
        StructField("wind_speed_avg_status", StringType(), True),
        StructField("cloud_coverage_avg", StringType(), True),
        StructField("cloud_coverage_avg_status", DoubleType(), True),
    ])
    k_d_t_weather = spark.read.option("encoding", "cp1250").csv(f"{hdfs_path}/{k_d_t_pattern}", schema = WEATHER_SCHEMA_k_d_t)
    
    # Read k_d_ files into a DataFrame
    WEATHER_SCHEMA_k_d =   StructType([
        StructField("station_code", IntegerType(), True),
        StructField("station_name", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True),
        StructField("temperature_max", DoubleType(), True),
        StructField("temperature_max_status", StringType(), True),
        StructField("temperature_min", DoubleType(), True),
        StructField("temperature_min_status", StringType(), True),
        StructField("temperature_avg", DoubleType(), True),
        StructField("temperature_avg_status", IntegerType(), True),
        StructField("temperature_ground_min", DoubleType(), True),
        StructField("temperature_ground_min_status", StringType(), True),
        StructField("rain", DoubleType(), True),
        StructField("rain_status", StringType(), True),
        StructField("rain_type", StringType(), True),
        StructField("snow_height", DoubleType(), True),
        StructField("snow_height_status", StringType(), True),
    ])
    k_d_weather = (
        spark.read.option("encoding", "cp1250").csv(f"{hdfs_path}/{k_d_pattern}", schema = WEATHER_SCHEMA_k_d)
        .drop('temperature_avg','temperature_avg_status','station_name')
    )
    
   

    # logger.info("Joining two tables...")
    df = k_d_t_weather.join(k_d_weather, on=['station_code','year','month','day'], how="inner")

    # logger.info("Performing data manipulation")
    df = (
        df.withColumn('year', F.col('year') + F.lit(1)) # a bit of time warping
        .withColumn('temperature_max_status',status_to_boolean('temperature_max_status'))
        .withColumn('temperature_min_status',status_to_boolean('temperature_min_status'))
        .withColumn('temperature_avg_status',status_to_boolean('temperature_avg_status'))
        .withColumn('temperature_ground_min_status',status_to_boolean('temperature_ground_min_status'))
        .withColumn('rain_status',status_to_text('rain_status'))
        .withColumn('snow_height_status',status_to_text('snow_height_status'))
        .withColumn('humidity_avg_status',status_to_boolean('humidity_avg_status'))
        .withColumn('wind_speed_avg_status',status_to_boolean('wind_speed_avg_status'))
        .withColumn('cloud_coverage_avg_status',status_to_boolean('cloud_coverage_avg_status'))
    )
    # logger.info(f"Result: {df.show(10)}")

 
    
    
    output_path = f'hdfs://localhost:8020/user/projekt/silver/weather'
    # logger.info(f"Saving to: {output_path}")
    df.write.mode('append').parquet(output_path)

    delete_files_with_prefix("/user/projekt/bronze/stage/pogoda", "k_d_")


    # logger.info("Ending spark application")
    spark.stop()



if __name__ == '__main__':
    main()
    sys.exit()