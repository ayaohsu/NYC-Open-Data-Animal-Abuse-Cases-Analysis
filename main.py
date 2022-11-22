import logging
import requests
import sys
import json

import boto3
from botocore.exceptions import ClientError

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType

APP_NAME = "kaporos_311_requests_analysis"
APP_TOKEN = "TJzJnHzT1F5ke8o6AxflJwvMG"
DATASET_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9"
S3_FILE_NAME = "311_response.json"
S3_BUCKET_NAME = "311-dataset"

def extract_311_requests_to_s3():
    responses_311 = requests.get(DATASET_URL, {
        "$$app_token": APP_TOKEN,
        "complaint_type": "Animal Abuse",
        "borough": "BROOKLYN",
        "$limit": 50000
    })

    complaints_311_in_json = responses_311.json()

    logging.info(f"Finished extracting data from 311. [requests_count={len(complaints_311_in_json)}]")

    with open(S3_FILE_NAME, 'w') as writer:
        writer.write(json.dumps(complaints_311_in_json))

    s3_client = boto3.client("s3")
    try:
        response = s3_client.upload_file(S3_FILE_NAME, S3_BUCKET_NAME, S3_FILE_NAME)
    except ClientError as e:
        logging.error(f"Failed to upload file to s3. [error={e}]")

def transform_311_requests():
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    s3_uri = f"s3a://{S3_BUCKET_NAME}/{S3_FILE_NAME}"
    requests_311 = spark.read.json(s3_uri)
    requests_311.printSchema()
    requests_311.createOrReplaceTempView("complaints")
    spark.sql("SELECT year(created_date), COUNT(*) FROM complaints GROUP BY year(created_date)").show()
    
    spark.stop()

if __name__ == "__main__":
    
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)
    
    extract_311_requests_to_s3()
    transform_311_requests()