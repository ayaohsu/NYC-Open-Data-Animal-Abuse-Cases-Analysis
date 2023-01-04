import logging
import sys
import os

from pyspark.sql import SparkSession

from extract_311_requests import extract_311_requests_to_s3
from transform_requests import transform_311_requests
from load_into_redshift import load_tables_into_redshift

from data_quality_checks import pass_data_quality_checks

APP_NAME = "kaporos_311_requests_analysis"

if __name__ == "__main__":
    
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)

    extract_311_requests_to_s3()

    spark = SparkSession.builder \
        .appName(APP_NAME) \
        .getOrCreate()

    transform_311_requests(spark)
    
    if not pass_data_quality_checks(spark):
        spark.stop()
        raise Exception('Failed data quality checks. Exiting without loading into Redshift.')

    load_tables_into_redshift(spark)

    spark.stop()