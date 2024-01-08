import sys

import re
from datetime import datetime
from functools import reduce

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, BooleanType
import findspark

from pyspark.sql.functions import input_file_name

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



FLIGHTS_SCHEMA = StructType([
    StructField("aircraft_iata", StringType(), True),
    StructField("aircraft_icao", StringType(), True),
    StructField("aircraft_icao24", StringType(), True),
    StructField("aircraft_registration", StringType(), True),
    
    StructField("airline_iata", StringType(), True),
    StructField("airline_icao", StringType(), True),
    StructField("airline_name", StringType(), True),
    StructField("arrival_actual", StringType(), True),
    
    StructField("arrival_actual_runway", StringType(), True),
    StructField("arrival_airport", StringType(), True),
    StructField("arrival_baggage", StringType(), True),
    StructField("arrival_delay", IntegerType(), True),
    StructField("arrival_estimated", StringType(), True), # to timestamp
    StructField("arrival_gate", StringType(), True),
    StructField("arrival_iata", StringType(), True),
    StructField("arrival_icao", StringType(), True),
    StructField("arrival_scheduled", StringType(), True), # to timestamp
    StructField("arrival_terminal", StringType(), True),
    StructField("arrival_timezone", StringType(), True),

    StructField("departure_actual_runway", StringType(), True),
    StructField("departure_airport", StringType(), True),
    StructField("departure_delay", IntegerType(), True),
    StructField("departure_estimated", StringType(), True), # to timestamp
    StructField("departure_gate", StringType(), True),
    StructField("departure_iata", StringType(), True),
    StructField("departure_icao", StringType(), True),
    StructField("departure_scheduled", StringType(), True), # to timestamp
    StructField("departure_terminal", StringType(), True),
    StructField("departure_timezone", StringType(), True),

    StructField("flight_codeshared_airline_iata", StringType(), True),
    StructField("flight_codeshared_airline_icao", StringType(), True),
    StructField("flight_codeshared_airline_name", StringType(), True),
    StructField("flight_codeshared_flight_iata", StringType(), True),
    StructField("flight_codeshared_flight_icao", StringType(), True),
    StructField("flight_codeshared_flight_number", StringType(), True),
    
    StructField("flight_iata", StringType(), True),
    StructField("flight_icao", StringType(), True),
    StructField("flight_number", StringType(), True),
    
    StructField("flight_date", StringType(), True),
    StructField("flight_status", StringType(), True),
    
    StructField("live_altitude", StringType(), True),
    StructField("live_direction", IntegerType(), True),
    StructField("live_is_ground", BooleanType(), True),
    StructField("live_latitude", StringType(), True),
    StructField("live_longitude", StringType(), True),
    StructField("live_speed_horizontal", StringType(), True),
    StructField("live_speed_vertical", StringType(), True),
    StructField("live_speed_updated", StringType(), True),     # to timestamp

    StructField("source_file", StringType(), True),     # to timestamp
    StructField("timestamp", StringType(), True),     # to timestamp
])


def flatten_row(row):
    """
    For parsing nested JSON rows.
    """
    flattened_row = {
        "aircraft_iata" : getattr(row.aircraft, 'iata', None),
        "aircraft_icao" : getattr(row.aircraft, 'icao', None),
        "aircraft_icao24" : getattr(row.aircraft, 'icao24', None),
        "aircraft_registration" : getattr(row.aircraft, 'registration', None),
        
        
        "airline_iata": getattr(row.airline, 'iata', None),
        "airline_icao": getattr(row.airline, 'icao', None),
        "airline_name":getattr(row.airline, 'name', None),

        "arrival_actual": getattr(row.arrival, 'actual', None),
        "arrival_actual_runway" : getattr(row.departure, 'actual_runway', None),
        "arrival_airport" : getattr(row.departure, 'airport', None),
        "arrival_baggage" : getattr(row.departure, 'baggage', None),
        "arrival_delay" : getattr(row.departure, 'delay', None),
        "arrival_estimated" : getattr(row.arrival, 'estimated', None),
        "arrival_gate" : getattr(row.arrival, 'gate', None),
        "arrival_iata" : getattr(row.arrival, 'iata', None),
        "arrival_icao" : getattr(row.arrival, 'icao', None),
        "arrival_scheduled" : getattr(row.arrival, 'scheduled', None),
        "arrival_terminal" : getattr(row.arrival, 'terminal', None),
        "arrival_timezone" : getattr(row.arrival, 'timezone', None),

        "departure_actual": getattr(row.departure, 'actual', None),
        "departure_actual_runway" : getattr(row.departure, 'actual_runway', None),
        "departure_airport" : getattr(row.departure, 'airport', None),
        "departure_estimated" : getattr(row.departure, 'estimated', None),
        "departure_gate" : getattr(row.departure, 'gate', None),
        "departure_iata" : getattr(row.departure, 'iata', None),
        "departure_icao" : getattr(row.departure, 'icao', None),
        "departure_scheduled" : getattr(row.departure, 'scheduled', None),
        "departure_terminal" : getattr(row.departure, 'terminal', None),
        "departure_timezone" : getattr(row.departure, 'timezone', None),
        
        "flight_codeshared_airline_iata": getattr(row.flight.codeshared, 'airline_iata', None),
        "flight_codeshared_airline_icao": getattr(row.flight.codeshared, 'airline_icao', None),
        "flight_codeshared_airline_name": getattr(row.flight.codeshared, 'airline_name', None),
        "flight_codeshared_flight_iata": getattr(row.flight.codeshared, 'flight_iata', None),
        "flight_codeshared_flight_icao": getattr(row.flight.codeshared, 'flight_icao', None),
        "flight_codeshared_flight_number": getattr(row.flight.codeshared, 'flight_number', None),

        "flight_iata" : getattr(row.flight, 'iata', None),
        "flight_icao" : getattr(row.flight, 'icao', None),
        "flight_number" : getattr(row.flight, 'number', None),
        
        "flight_date": getattr(row, 'flight_date', None),
        "flight_status": getattr(row, 'flight_status', None),

        "live_altitude": getattr(row.live, 'altitude', None),
        "live_direction": getattr(row.live, 'direction', None),
        "live_is_ground": getattr(row.live, 'is_ground', None),
        "live_latitude": getattr(row.live, 'latitude', None),
        "live_longitude": getattr(row.live, 'speed_horizontal', None),
        "live_speed_horizontal": getattr(row.live, 'speed_horizontal', None),
        "live_speed_vertical": getattr(row.live, 'speed_vertical', None),
        "live_speed_updated": getattr(row.live, 'updated', None),
        
    }
    return flattened_row



def main():
    findspark.init()
    
    # Logging configuration
    # formatter = logging.Formatter('[%(asctime)s] %(levelname)s @ line %(lineno)d: %(message)s')
    # handler = logging.StreamHandler(sys.stdout)
    # handler.setLevel(logging.INFO)
    # handler.setFormatter(formatter)
    # logger = logging.getLogger()
    # logger.setLevel(logging.INFO)
    # logger.addHandler(handler)

    spark = SparkSession.builder.appName("fligths_silver").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    # logger.info("Starting spark application")

    output_path = f'hdfs://localhost:8020/user/projekt/silver/flights'
    
    # file_paths = get_file_path('samoloty', file_starts_with='flights')
    # file_paths_chunks = [file_paths[x:x+500] for x in range(0, len(file_paths), 500)]
    
    table = 'flights'
    hdfs_path = 'hdfs://localhost:8020/user/projekt/bronze/stage/samoloty'
    file_paths = (
        spark.read.text(f'{hdfs_path}/{table}*')
        .withColumn('source_file', F.input_file_name())
        .select('source_file')
        .distinct()
        .collect()
    )
    file_paths = [row.source_file for row in file_paths] 
    file_paths_chunks = [file_paths[x:x+1000] for x in range(0, len(file_paths), 1000)]


    timestamp_pattern = re.compile(r'\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}')

    
    for i,chunk in enumerate(file_paths_chunks):
        try:
            bronze_flights = spark.read.json(chunk)
            # raw_rows = bronze_flights.select('data').where(F.col('data').isNotNull()).collect()[0][0]
            raw_dfs = bronze_flights.select('data').where(F.col('data').isNotNull()).withColumn('source_file',F.input_file_name()).collect()
                
            
            flattened_rows = list()
            for raw_df in raw_dfs:
                source_file = raw_df[1].split('/')[-1]
                timestamp = timestamp_pattern.search(source_file).group()
                raw_df = raw_df[0]
                if not raw_df:
                    continue
                for row in raw_df:
                    result = flatten_row(row)
                    result['source_file'] = source_file
                    result['timestamp'] = timestamp
                    flattened_rows.append(result)
            df = (
                spark.createDataFrame(flattened_rows, schema=FLIGHTS_SCHEMA)
                .withColumn('live_altitude', F.col('live_altitude').cast(DoubleType()))
                .withColumn('live_longitude', F.col('live_longitude').cast(DoubleType()))
                .withColumn('live_latitude', F.col('live_latitude').cast(DoubleType()))
                .withColumn('live_latitude', F.col('live_latitude').cast(DoubleType()))
                .withColumn('live_speed_horizontal', F.col('live_speed_horizontal').cast(DoubleType()))
                .withColumn('live_speed_vertical', F.col('live_speed_vertical').cast(DoubleType()))
                
                .withColumn('arrival_estimated', F.col('arrival_estimated').cast(TimestampType()))
                .withColumn('arrival_scheduled', F.col('arrival_scheduled').cast(TimestampType()))
                .withColumn('departure_estimated', F.col('departure_estimated').cast(TimestampType()))
                .withColumn('departure_scheduled', F.col('departure_scheduled').cast(TimestampType()))
                .withColumn('live_speed_updated', F.col('live_speed_updated').cast(TimestampType()))
                .withColumn('timestamp', F.from_unixtime(F.unix_timestamp(F.col('timestamp'), 'yyyy-MM-dd-HH-mm-ss')).cast(TimestampType()))
                .withColumn('year', F.year(F.col('timestamp')))
                .withColumn('month', F.month(F.col('timestamp')))
                .withColumn('day', F.dayofmonth(F.col('timestamp')))
                
            )
            df.write.partitionBy('year','month','day').mode('append').parquet(output_path)
            # df.write.mode('append').parquet(output_path)
        
        except Exception as e:
            print(e)
    
    delete_files_with_prefix("/user/projekt/bronze/stage/samoloty", "flight")


    
    # logger.info("Ending spark application")
    spark.stop()

if __name__ == '__main__':
    main()
    sys.exit()