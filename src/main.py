import logging
import sys

from pyspark.sql import SparkSession

from extract_311_requests import extract_311_requests_to_s3
from transform_requests import transform_311_requests
from load_into_redshift import load_tables_into_redshift

APP_NAME = "kaporos_311_requests_analysis"

def run_etl_pipeline(sparkSession):
    extract_311_requests_to_s3()
    transform_311_requests(sparkSession)
    load_tables_into_redshift(sparkSession)

if __name__ == "__main__":
    
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)
    
    spark = SparkSession.builder \
        .appName(APP_NAME) \
        .getOrCreate()

    run_etl_pipeline(spark)

    spark.stop()