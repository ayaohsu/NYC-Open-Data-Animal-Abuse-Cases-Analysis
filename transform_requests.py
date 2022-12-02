import logging

from pyspark.sql.functions import col, row_number, lit
from pyspark.sql.window import Window

S3_FILE_NAME = "311_response.json"
S3_BUCKET_NAME = "311-dataset"

def transform_311_requests(sparkSession):
    s3_uri = f"s3://{S3_BUCKET_NAME}/{S3_FILE_NAME}"
    requests_311 = sparkSession.read.json(s3_uri)

    requests_with_desired_fields = requests_311\
        .select(col("unique_key"),
        col("created_date"),
        col("closed_date"),
        col("complaint_type"),
        col("incident_zip"),
        col("address_type"),
        col("descriptor"),
        col("location.human_address").alias("human_address"),
        col("location.latitude").alias("latitude"),
        col("location.longitude").alias("longitude"),
        col("resolution_description"),
        col("cross_street_1"),
        col("cross_street_2"),
        col("intersection_street_1"),
        col("intersection_street_2"))
    
    requests_with_desired_fields.createOrReplaceTempView("fact_service_requests")
    requests_with_desired_fields.show()

    complaint_types = requests_311.select(col("complaint_type").alias("complaint_type_name"))\
        .distinct().orderBy("complaint_type")

    complaint_types_with_partition = complaint_types.withColumn("partition", lit("ALL"))
    one_partition = Window.partitionBy("partition").orderBy("partition")

    complaint_types_with_row_numbers = complaint_types_with_partition\
        .withColumn("complaint_type_key", row_number().over(one_partition))\
        .drop("partition")
    
    complaint_types_with_row_numbers.createOrReplaceTempView("dim_complaint_type")
    complaint_types_with_row_numbers.show()

    logging.info(f"Finished transforming data.")