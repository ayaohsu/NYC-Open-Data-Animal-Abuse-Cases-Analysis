import logging
import sys
import time

from pyspark.sql import SparkSession

from pyspark.sql.functions import col, row_number, lit, to_date, date_format,\
     year, month, dayofyear, substring, when
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

S3_FILE_NAME = "311_response.json"
S3_BUCKET_NAME = "311-dataset"

MAX_SIZE_STRING_COLUMN_REDSHIFT = 256

def transform_requests_df(requests):
    if "closed_date" not in requests.columns:
        requests_with_closed_date = requests.withColumn("closed_date_time", lit(None))
    else:
        requests_with_closed_date = requests
    
    requests_with_desired_fields = requests_with_closed_date\
        .select(col("unique_key"),\
            to_date(col("created_date")).alias("created_date"),\
            to_date(col("closed_date")).alias("closed_date"),\
            col("complaint_type"),\
            col("incident_zip"),\
            col("incident_address"),\
            col("location_type"),\
            col("descriptor"),\
            col("location.latitude").alias("latitude").cast("double"),\
            col("location.longitude").alias("longitude").cast("double"),\
            col("x_coordinate_state_plane"),\
            col("y_coordinate_state_plane"),\
            col("resolution_description"),\
            col("cross_street_1"),\
            col("cross_street_2"),\
            col("intersection_street_1"),\
            col("intersection_street_2")\
        )
    
    requests_with_separate_resolution_description_columns = requests_with_desired_fields\
        .withColumn("resolution_description_1", substring("resolution_description", 1, MAX_SIZE_STRING_COLUMN_REDSHIFT))\
        .withColumn("resolution_description_2", substring("resolution_description", MAX_SIZE_STRING_COLUMN_REDSHIFT+1, MAX_SIZE_STRING_COLUMN_REDSHIFT))\
        .drop("resolution_description")
    
    requests_with_normalized_animal_abuse_complaint_type = requests_with_separate_resolution_description_columns\
        .withColumn("complaint_type_new", when(col("complaint_type") == "Animal-Abuse", "Animal Abuse").otherwise(col("complaint_type")))\
        .drop("complaint_type")\
        .withColumnRenamed("complaint_type_new", "complaint_type")

    return requests_with_normalized_animal_abuse_complaint_type


def create_dim_complaint_type_table(sparkSession, requests):
    complaint_types = requests.select(col("complaint_type").alias("complaint_type_name"))\
        .distinct().orderBy("complaint_type")

    complaint_types_with_partition = complaint_types.withColumn("partition", lit("ALL"))
    one_partition = Window.partitionBy("partition").orderBy("partition")

    complaint_types_with_row_numbers = complaint_types_with_partition\
        .withColumn("complaint_type_key", row_number().over(one_partition))\
        .drop("partition")
    
    complaint_types_with_row_numbers.createOrReplaceTempView("dim_complaint_type")

def create_dim_date_table(sparkSession, requests):
    created_dates = requests.select(to_date("created_date").alias("date")).filter("date is not null").distinct()
    closed_dates = requests.select(to_date("closed_date").alias("date")).filter("date is not null").distinct()
    all_dates = created_dates.union(closed_dates).distinct()
    
    dates_with_key = all_dates.withColumn("date_key", date_format(col("date"), "yyyyMMdd").cast(IntegerType()))
    dates_with_year_month_day = dates_with_key\
        .withColumn("year", year(col("date")))\
        .withColumn("month", month(col("date")))\
        .withColumn("dayofyear", dayofyear(col("date")))

    dates_with_year_month_day.createOrReplaceTempView("dim_date")

def create_location_type_table(sparkSession, requests):
    location_types = requests.select(col("location_type").alias("location_type_name"))\
        .filter("location_type_name is not null").distinct().orderBy("location_type")

    location_types_with_partition = location_types.withColumn("partition", lit("ALL"))
    one_partition = Window.partitionBy("partition").orderBy("partition")

    location_types_with_row_numbers = location_types_with_partition\
        .withColumn("location_type_key", row_number().over(one_partition))\
        .drop("partition")
    
    location_types_with_row_numbers.createOrReplaceTempView("dim_location_type")

def create_fact_service_request_table(sparkSession, requests):
    
    complaint_types = sparkSession.sql("SELECT complaint_type_key, complaint_type_name FROM dim_complaint_type")
    
    requests_with_complaint_type_keys = requests\
        .join(complaint_types, requests["complaint_type"] == complaint_types["complaint_type_name"])\
        .select(requests["*"], complaint_types["complaint_type_key"])\
        .drop("complaint_type")

    location_types = sparkSession.sql("SELECT location_type_key, location_type_name FROM dim_location_type")
    
    requests_with_location_type_keys = requests_with_complaint_type_keys\
        .join(location_types, requests_with_complaint_type_keys["location_type"] == location_types["location_type_name"], "left")\
        .select(requests_with_complaint_type_keys["*"], location_types["location_type_key"])\
        .drop("location_type")

    dates = sparkSession.sql("SELECT date_key, date FROM dim_date")
    requests_with_created_date_key = requests_with_location_type_keys\
        .join(dates, requests_with_location_type_keys["created_date"] == dates["date"])\
        .select(requests_with_location_type_keys["*"], dates["date_key"].alias("created_date_key"))\
        .drop("created_date")
    
    requests_with_closed_date_key = requests_with_created_date_key\
        .join(dates, requests_with_created_date_key["closed_date"] == dates["date"], "left")\
        .select(requests_with_created_date_key["*"], dates["date_key"].alias("closed_date_key"))\
        .drop("closed_date")

    requests_with_closed_date_key.createOrReplaceTempView("fact_service_request")

def transform_311_requests(sparkSession):
    start_time = time.time()

    s3_uri = f"s3://{S3_BUCKET_NAME}/{S3_FILE_NAME}"
    requests_311 = sparkSession.read.json(s3_uri)

    request = transform_requests_df(requests_311)

    create_dim_complaint_type_table(sparkSession, request)
    create_dim_date_table(sparkSession, request)
    create_location_type_table(sparkSession, request)
    create_fact_service_request_table(sparkSession, request)

    end_time = time.time()
    elapsed_time = end_time - start_time
    logging.info(f"Finished transforming data. [elapsed_time={elapsed_time}]")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("kaporos_311_requests_analysis_transform").getOrCreate()

    transform_logger = logging.getLogger()
    transform_logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    transform_logger.addHandler(handler)

    transform_311_requests(spark)
    spark.stop()