import sys

import subprocess
import re
from datetime import datetime
from functools import reduce

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, BooleanType
import findspark

import logging



def main():
    findspark.init()

    spark = SparkSession.builder.appName("silver_to_gold").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    #####################
    # LOAD FLIGHTS DATA #
    #####################
    filter_live = """
        live_altitude is not null or 
        live_direction is not null or 
        live_is_ground is not null or 
        live_latitude is not null or  
        live_longitude is not null or 
        live_speed_horizontal is not null or 
        live_speed_vertical is not null or 
        live_speed_updated is not null or  
    """
    
    polish_airports_iata = ["WAW", "KRK", "GDN", "WRO", "KTW", "POZ", "RZE", "SZZ", "BZG", "LUZ"]
    
    
    flights = (
        spark.read.parquet('hdfs://localhost:8020/user/projekt/silver/flights')
        .filter(filter_live)
        .dropDuplicates(['arrival_scheduled','departure_scheduled','flight_icao'])
    
        #arrival
        .withColumn('arrival_year', F.year(F.col('arrival_scheduled')))
        .withColumn('arrival_month', F.month(F.col('arrival_scheduled')))
        .withColumn('arrival_day', F.dayofmonth(F.col('arrival_scheduled')))
    
        #departure
        .withColumn('departure_year', F.year(F.col('departure_scheduled')))
        .withColumn('departure_month', F.month(F.col('departure_scheduled')))
        .withColumn('departure_day', F.dayofmonth(F.col('departure_scheduled')))
    
        # if arrival airport in Poland then true other wise the departure is from Poland
        .withColumn('arrival_to_poland',
                    F.when(F.col('departure_iata').isin(*polish_airports_iata),F.lit(True).cast(BooleanType()))
                    .otherwise(F.lit(False).cast(BooleanType()))
       )
    )

    arrivals = flights.where(F.col('arrival_to_poland') == F.lit(True).cast(BooleanType()))
    departures = flights.where(F.col('arrival_to_poland') == F.lit(False).cast(BooleanType()))




    #####################
    # LOAD WEATHER DATA #
    #####################
    mapping = [("WAW", "WARSZAWA-BIELANY"),
        ("KRK", "KRAKÓW-OBSERWATORIUM"),
        ("GDN", "GDAŃSK-RĘBIECHOWO"),
        ("WRO", "PUCZNIEW"),
        ("KTW", "DRONIOWICE"),
        ("POZ", "CHRZĄSTOWO"),
        ("RZE", "DYNÓW"),
        ("SZZ", "GOLENIÓW"),
        ("BZG", "KOŁUDA WIELKA"),
        ("LUZ", "PUŁAWY")
    ]
    schema = ["airport_code", "station_name"]
    link_weather_flight = spark.createDataFrame(mapping, schema=schema)

    weather = (
        spark.read.parquet('hdfs://localhost:8020/user/projekt/silver/weather')   
        .join(link_weather_flight, on='station_name', how='left')
    )
    
    cols_not_to_rename = ['airport_code']
    cols_to_rename = [c for c in weather.columns if (c not in cols_not_to_rename)]

    weather_arrivals = weather.select([F.col('airport_code').alias('arrival_iata')] + [F.col(c).alias(f'arrival_{c}') for c in cols_to_rename])
    weather_departures = weather.select([F.col('airport_code').alias('departure_iata')] + [F.col(c).alias(f'departure_{c}') for c in cols_to_rename])

    arrival_flights = arrivals.join(weather_arrivals, on=['arrival_year','arrival_month','arrival_day','arrival_iata'], how='left')
    departure_flights = arrivals.join(weather_departures, on=['departure_year','departure_month','departure_day','departure_iata'], how='left')
    
    
    fligths_weather = arrival_flights.unionByName(departure_flights, allowMissingColumns=True)
    output_path = f'hdfs://localhost:8020/user/projekt/gold/flights_weather'
    # logger.info(f"Saving to: {output_path}")
    fligths_weather.write.mode('append').parquet(output_path)

    # sheesh
    # logger.info("Ending spark application")
    spark.stop()

if __name__ == '__main__':
    main()
    sys.exit()