import sys

import re
from datetime import datetime
from functools import reduce

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, BooleanType
import findspark

import logging

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




def main():
    findspark.init()
    
    # Logging configuration
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s @ line %(lineno)d: %(message)s')
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    spark = SparkSession.builder.appName("airports_silver").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    AIRPORTS_SCHEMA = StructType([
        StructField("airport_id", StringType(), True),
        StructField("airport_name", StringType(), True),
        StructField("city_iata_code", StringType(), True),
        StructField("country_iso2", StringType(), True),
        StructField("country_name", StringType(), True),
        StructField("geoname_id", StringType(), True),
        StructField("gmt", StringType(), True),
        StructField("iata_code", StringType(), True),
        StructField("icao_code", StringType(), True),
        StructField("id", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("timezone", StringType(), True), 
    
    ])

    
    table = 'airports'
    hdfs_path = 'hdfs://localhost:8020/user/projekt/bronze/stage/samoloty'
    file_paths = (
        spark.read.text(f'{hdfs_path}/{table}*')
        .withColumn('source_file', F.input_file_name())
        .select('source_file')
        .distinct()
        .collect()
    )

    file_paths = [row.source_file for row in file_paths] 

    # logger.info(f"Reading aircraft_types files: {file_paths}")
    
    dfs = []   
    for file_path in file_paths:
        print(file_path)
        raw_json = spark.read.json(file_path)
        df = spark.createDataFrame(raw_json.select('data').collect()[0][0], schema=AIRPORTS_SCHEMA)

        dfs.append(df)

    df = reduce(DataFrame.union, dfs)
    # logger.info(f"After parsing and union {df.show(10)}")
    
    output_path = f'hdfs://localhost:8020/user/projekt/silver/{table}'
    # logger.info(f"Saving to silver.... Path: {output_path}")
    df.write.mode('append').parquet(output_path)    

    delete_files_with_prefix("/user/projekt/bronze/stage/samoloty", "airport")


    # logger.info("Ending spark application")
    spark.stop()

if __name__ == '__main__':
    main()
    sys.exit()