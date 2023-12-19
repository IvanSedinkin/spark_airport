import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType
from pyspark.sql.types import StructField

"""
The task is to build a summary table about all airlines containing the input data.
"""


def process(spark, flights_path_input, airlines_path_input, result_path_input):
    """
    Main process of task

    :param spark: SparkSession
    :param flights_path_input: path to data with flights
    :param airlines_path_input: path to data with c airlines
    :param result_path_input: path to result of data manipulation
    """

    # schema flights input data

    df_schema_flight = StructType(
        [
            StructField("YEAR", IntegerType()),
            StructField("MONTH", IntegerType()),
            StructField("DAY", IntegerType()),
            StructField("DAY_OF_WEEK", IntegerType()),
            StructField("AIRLINE", StringType()),
            StructField("FLIGHT_NUMBER", IntegerType()),
            StructField("TAIL_NUMBER", StringType()),
            StructField("ORIGIN_AIRPORT", StringType()),
            StructField("DESTINATION_AIRPORT", StringType()),
            StructField("SCHEDULED_DEPARTURE", IntegerType()),
            StructField("DEPARTURE_TIME", IntegerType()),
            StructField("DEPARTURE_DELAY", DoubleType()),
            StructField("TAXI_OUT", IntegerType()),
            StructField("WHEELS_OFF", IntegerType()),
            StructField("SCHEDULED_TIME", IntegerType()),
            StructField("ELAPSED_TIME", IntegerType()),
            StructField("AIR_TIME", DoubleType()),
            StructField("DISTANCE", IntegerType()),
            StructField("WHEELS_ON", IntegerType()),
            StructField("TAXI_IN", IntegerType()),
            StructField("SCHEDULED_ARRIVAL", IntegerType()),
            StructField("ARRIVAL_TIME", IntegerType()),
            StructField("ARRIVAL_DELAY", IntegerType()),
            StructField("DIVERTED", IntegerType()),
            StructField("CANCELLED", IntegerType()),
            StructField("CANCELLATION_REASON", StringType()),
            StructField("AIR_SYSTEM_DELAY", IntegerType()),
            StructField("SECURITY_DELAY", IntegerType()),
            StructField("AIRLINE_DELAY", IntegerType()),
            StructField("LATE_AIRCRAFT_DELAY", IntegerType()),
            StructField("WEATHER_DELAY", IntegerType())
        ]
    )

    # schema airlines input data

    df_schema_airlines = StructType([
        StructField("IATA_CODE", StringType()),
        StructField("AIRLINE", StringType())
    ])

    df_flights = spark.read.schema(df_schema_flight).parquet(flights_path_input)
    df_airlines = spark.read.schema(df_schema_airlines).parquet(airlines_path_input)

    df_flights.registerTempTable("flights")
    df_airlines.registerTempTable("airlines")

    ds_result = spark.sql(
        """
            select
            air.AIRLINE as AIRLINE_NAME,
            count(*) as correct_count,
            sum (
                case when DIVERTED = 1 then 1 else 0 end
            ) as diverted_count,
            sum (
                case when CANCELLED = 1 then 1 else 0 end
            ) as cancelled_count,
            avg(DISTANCE) as avg_distance,
            avg(AIR_TIME) as avg_air_time,
            sum (
                case when CANCELLATION_REASON = 'A' then 1 else 0 end
            ) as airline_issue_count,
            sum (
                case when CANCELLATION_REASON = 'B' then 1 else 0 end
            ) as weather_issue_count,
            sum (
                case when CANCELLATION_REASON = 'C' then 1 else 0 end
            ) as nas_issue_count,
            sum (
                case when CANCELLATION_REASON = 'D' then 1 else 0 end
            ) as security_issue_count
            from flights as fl
            left join airlines as air
            on fl.AIRLINE = air.IATA_CODE
            group by air.AIRLINE
        """
    )

    ds_result.show(truncate=False, n=100000)
    ds_result.write.mode('overwrite').parquet(result_path_input)


def main(flights_path, airlines_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, airlines_path, result_path)


def _spark_session():
    """
    Create SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJobAirlines').getOrCreate()


if __name__ == "__main__":
    flights_path_variable = os.environ['flights_path']
    airlines_path_variable = os.environ['airlines_path']
    result_path_variable = os.environ['result_path']

    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default=flights_path_variable,
                        help='Please set flights datasets path.')
    parser.add_argument('--airlines_path', type=str, default=airlines_path_variable,
                        help='Please set airlines datasets path.')
    parser.add_argument('--result_path', type=str, default=result_path_variable,
                        help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    airlines_path = args.airlines_path
    result_path = args.result_path
    main(flights_path, airlines_path, result_path)
