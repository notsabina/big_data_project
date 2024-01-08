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

    spark = SparkSession.builder.appName("airplanes_silver").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    AIRPLANES_SCHEMA = StructType([
        StructField("airline_iata_code", StringType(), True),
        StructField("airline_icao_code", StringType(), True),
        StructField("airplane_id", StringType(), True),
        StructField("construction_number", StringType(), True),
        StructField("delivery_date", StringType(), True),
        StructField("engines_count", StringType(), True),
        StructField("engines_type", StringType(), True),
        StructField("first_flight_date", StringType(), True),
        StructField("iata_code_long", StringType(), True),
        StructField("iata_code_short", StringType(), True),
        StructField("iata_type", StringType(), True),
        StructField("icao_code_hex", StringType(), True),
        StructField("id", StringType(), True),
        StructField("line_number", StringType(), True),
        StructField("model_code", StringType(), True),
        StructField("model_name", StringType(), True),
        StructField("plane_age", StringType(), True),
        StructField("plane_class", StringType(), True),
        StructField("plane_owner", StringType(), True),
        StructField("plane_series", StringType(), True),
        StructField("plane_status", StringType(), True),
        StructField("production_line", StringType(), True),
        StructField("registration_date", StringType(), True),
        StructField("registration_number", StringType(), True),
        StructField("rollout_date", StringType(), True),
        StructField("test_registration_number", StringType(), True)
    ])

    
    table = 'airplanes'
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
        df = spark.createDataFrame(raw_json.select('data').collect()[0][0], schema=AIRPLANES_SCHEMA)

        dfs.append(df)

    df = reduce(DataFrame.union, dfs)
    # logger.info(f"After parsing and union {df.show(10)}")
    
    output_path = f'hdfs://localhost:8020/user/projekt/silver/{table}'
    # logger.info(f"Saving to silver.... Path: {output_path}")
    df.write.mode('append').parquet(output_path)    

    delete_files_with_prefix("/user/projekt/bronze/stage/samoloty", "airplane")


    # logger.info("Ending spark application")
    spark.stop()

if __name__ == '__main__':
    main()
    sys.exit()