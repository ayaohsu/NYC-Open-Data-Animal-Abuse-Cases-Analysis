import logging
import sys

from extract_311_requests import extract_311_requests_to_s3
from transform_requests import transform_311_requests

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