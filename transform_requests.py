import logging
import sys

from pyspark.sql import SparkSession

from pyspark.sql.functions import col, row_number, lit, to_date, date_format
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

S3_FILE_NAME = "311_response.json"
S3_BUCKET_NAME = "311-dataset"

def create_dim_complaint_type_table(sparkSession, requests):
    complaint_types = requests.select(col("complaint_type").alias("complaint_type_name"))\
        .distinct().orderBy("complaint_type")

    complaint_types_with_partition = complaint_types.withColumn("partition", lit("ALL"))
    one_partition = Window.partitionBy("partition").orderBy("partition")

    complaint_types_with_row_numbers = complaint_types_with_partition\
        .withColumn("complaint_type_key", row_number().over(one_partition))\
        .drop("partition")
    
    complaint_types_with_row_numbers.createOrReplaceTempView("dim_complaint_type")
    complaint_types_with_row_numbers.show()

def create_dim_date_table(sparkSession, requests):
    created_dates = requests.select(to_date("created_date").alias("date")).filter("date is not null").distinct()
    created_dates.show()
    closed_dates = requests.select(to_date("closed_date").alias("date")).filter("date is not null").distinct()
    closed_dates.show()
    all_dates = created_dates.union(closed_dates).distinct()
    all_dates.show()

    dates_with_key = all_dates.withColumn("date_key", date_format(col("date"), "yyyyMMdd").cast(IntegerType()))
    dates_with_key.createOrReplaceTempView("dim_date_type")
    dates_with_key.show()    

def create_fact_service_requests_table(sparkSession, requests):
    requests_with_desired_fields = requests\
        .select(col("unique_key"),\
            col("created_date"),\
            col("closed_date"),\
            col("complaint_type"),\
            col("incident_zip"),\
            col("address_type"),\
            col("descriptor"),\
            col("location.human_address").alias("human_address"),\
            col("location.latitude").alias("latitude"),\
            col("location.longitude").alias("longitude"),\
            col("resolution_description"),\
            col("cross_street_1"),\
            col("cross_street_2"),\
            col("intersection_street_1"),\
            col("intersection_street_2")\
        )
    
    complaint_types = sparkSession.sql("SELECT * FROM dim_complaint_type")
    
    requests_with_complaint_type_keys = requests_with_desired_fields\
        .join(complaint_types, requests_with_desired_fields["complaint_type"] == complaint_types["complaint_type_name"])\
        .select(requests_with_desired_fields["*"], complaint_types["complaint_type_key"])\
        .drop("complaint_type")
    
    requests_with_complaint_type_keys.createOrReplaceTempView("fact_service_requests")

    requests_with_complaint_type_keys.show()

def transform_311_requests(sparkSession):
    s3_uri = f"s3://{S3_BUCKET_NAME}/{S3_FILE_NAME}"
    requests_311 = sparkSession.read.json(s3_uri)

    create_dim_complaint_type_table(sparkSession, requests_311)
    create_dim_date_table(sparkSession, requests_311)
    create_fact_service_requests_table(sparkSession, requests_311)

    logging.info(f"Finished transforming data.")

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