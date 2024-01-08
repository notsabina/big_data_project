import sys

import subprocess
import re
from datetime import datetime
from functools import reduce
import findspark
findspark.init()
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, BooleanType

from pyspark.sql.functions import col



import happybase


import pandas as pd

def encode_if_not_none(value):
    return str(value).encode('utf-8') if value is not None else None

def return_cf_for_col(coll, structure):
    for key, value in structure.items():
        if coll in value:
            return key
    return None

def connect(table_name):
    VM_adress = 'localhost'
    connection = happybase.Connection(VM_adress, timeout=None)
    table = connection.table(table_name)
    return table

def print_table(limit = None, table_name = 'default'):
    table = connect(table_name)
    
    for key, data in table.scan(None):
        print(f'KEY: {key}')
        for column, value in data.items():
            print(f'\tCOLUMNS: {column} VALUE: {value}')


def main():
    

    spark = SparkSession.builder.appName("gold_to_hbase").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    VM_adress = 'localhost'
    table_name = 'projekt'

    all = spark.read.parquet('/user/projekt/gold/flights_weather/')
  
    structure = {
    'flight': [
        'arrival_year',
        'arrival_month',
        'arrival_day',
        'arrival_iata',
        'aircraft_iata',
        'aircraft_icao',
        'aircraft_icao24',
        'aircraft_registration',
        'airline_iata',
        'airline_icao',
        'airline_name',
        'arrival_actual',
        'arrival_actual_runway',
        'arrival_airport',
        'arrival_baggage',
        'arrival_delay',
        'arrival_estimated',
        'arrival_gate',
        'arrival_icao',
        'arrival_scheduled',
        'arrival_terminal',
        'arrival_timezone',
        'departure_actual_runway',
        'departure_airport',
        'departure_delay',
        'departure_estimated',
        'departure_gate',
        'departure_iata',
        'departure_icao',
        'departure_scheduled',
        'departure_terminal',
        'departure_timezone',
        'flight_codeshared_airline_iata',
        'flight_codeshared_airline_icao',
        'flight_codeshared_airline_name',
        'flight_codeshared_flight_iata',
        'flight_codeshared_flight_icao',
        'flight_codeshared_flight_number',
        'flight_iata',
        'flight_icao',
        'flight_number',
        'flight_date',
        'flight_status'
    ],

    'weather': [
        'arrival_to_poland',
        'arrival_station_name',
        'arrival_station_code',
        'arrival_temperature_avg',
        'arrival_temperature_avg_status',
        'arrival_humidity_avg',
        'arrival_humidity_avg_status',
        'arrival_wind_speed_avg',
        'arrival_wind_speed_avg_status',
        'arrival_cloud_coverage_avg',
        'arrival_cloud_coverage_avg_status',
        'arrival_temperature_max',
        'arrival_temperature_max_status',
        'arrival_temperature_min',
        'arrival_temperature_min_status',
        'arrival_temperature_ground_min',
        'arrival_temperature_ground_min_status',
        'arrival_rain',
        'arrival_rain_status',
        'arrival_rain_type',
        'arrival_snow_height',
        'arrival_snow_height_status',
        'departure_station_name',
        'departure_station_code',
        'departure_temperature_avg',
        'departure_temperature_avg_status',
        'departure_humidity_avg',
        'departure_humidity_avg_status',
        'departure_wind_speed_avg',
        'departure_wind_speed_avg_status',
        'departure_cloud_coverage_avg',
        'departure_cloud_coverage_avg_status',
        'departure_temperature_max',
        'departure_temperature_max_status',
        'departure_temperature_min',
        'departure_temperature_min_status',
        'departure_temperature_ground_min',
        'departure_temperature_ground_min_status',
        'departure_rain',
        'departure_rain_status',
        'departure_rain_type',
        'departure_snow_height',
        'departure_snow_height_status'
    ]
}
    small_df = all

    columns_selection = []
    for key, value in structure.items():
        columns_selection += value

    small_df = small_df[columns_selection]

    column_families = []
    for key, value in structure.items():
        column_families += [key]
    column_families

    table_name = 'projekt'

    for column in small_df.columns:
        small_df = small_df.withColumn(column, col(column).cast("string"))

    small_df = small_df.toPandas()

    table = connect(table_name)


    for i in range (len(small_df)):
        flight_icao = small_df.loc[i, 'flight_icao']
        departure_scheduled = small_df.loc[i, 'departure_scheduled']
        row_key = f'row{flight_icao}_{departure_scheduled}'
        data = {}
        columns_not_null = list(small_df.columns[small_df.iloc[i].notna()])
    
        for coll in columns_selection:
            if coll in columns_not_null:
                family = return_cf_for_col(coll, structure)
                data[f'{family}:{coll}'.encode()] = encode_if_not_none(small_df.loc[i, coll])
    
        table.put(row_key, data)


    
    
    spark.stop()

if __name__ == '__main__':
    main()
    sys.exit()