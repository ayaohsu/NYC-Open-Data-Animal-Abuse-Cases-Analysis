
import logging
import requests
import json
import sys

import boto3
from botocore.exceptions import ClientError

APP_NAME = "kaporos_311_requests_analysis"
APP_TOKEN = "TJzJnHzT1F5ke8o6AxflJwvMG"
DATASET_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9"
S3_FILE_NAME = "311_response.json"
S3_BUCKET_NAME = "311-dataset"

def extract_311_requests_to_s3():

    query_to_311_requests = """
        borough = 'BROOKLYN'
    """

    # query_to_311_requests = """
    #     date_extract_y(created_date) >= 2015
    #     and complaint_type in ('Animal Abuse', 'Animal-Abuse')
    #     and borough = 'BROOKLYN'
    # """

    responses_311 = requests.get(DATASET_URL, {
        "$$app_token": APP_TOKEN,
        "$limit":  10, #1000000,
        "$where": query_to_311_requests
    })

    if not responses_311.ok:
        error_message = "Failed to extract data from 311. [status_code={responses_311.status_code}][reason={responses_311.reason}]"
        logging.error(error_message)
        raise Exception(error_message)

    complaints_311_in_json = responses_311.json()
    logging.info(f"Finished extracting data from 311. [requests_count={len(complaints_311_in_json)}]")

    with open(S3_FILE_NAME, 'w') as writer:
        writer.write(json.dumps(complaints_311_in_json))

    s3_client = boto3.client("s3")
    try:
        s3_client.upload_file(S3_FILE_NAME, S3_BUCKET_NAME, S3_FILE_NAME)
    except ClientError as e:
        error_message = f"Failed to upload file to s3. [error={e}]"
        logging.error(error_message)
        raise Exception(error_message)

if __name__ == "__main__":

    extract_logger = logging.getLogger()
    extract_logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    extract_logger.addHandler(handler)

    extract_311_requests_to_s3()