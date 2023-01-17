import pytest
from pyspark.sql import SparkSession

from extract_311_requests import extract_311_requests_to_s3
from transform_requests import transform_311_requests
from load_into_redshift import load_tables_into_redshift, REDSHIFT_URL, IAM_ROLE_ARN

class MockedResponse:
    def __init__(self):
        self.ok = True
    
    def json(self):
        sample_311_request = {
            "unique_key":"55577067",
            "created_date":"2022-10-01T23:24:42.000",
            "closed_date":"2022-10-01T23:28:08.000",
            "agency":"NYPD",
            "agency_name":"New York City Police Department",
            "complaint_type":"Animal-Abuse",
            "descriptor":"Tortured",
            "location_type":"Store/Commercial",
            "incident_zip":"11206",
            "incident_address":"205 LEE AVENUE",
            "street_name":"LEE AVENUE",
            "cross_street_1":"HEYWARD STREET",
            "cross_street_2":"LYNCH STREET",
            "intersection_street_1":"HEYWARD STREET",
            "intersection_street_2":"LYNCH STREET",
            "address_type":"ADDRESS",
            "city":"BROOKLYN",
            "landmark":"LEE AVENUE",
            "status":"Closed",
            "resolution_description":"The Police Department responded to the complaint and determined that police action was not necessary.",
            "resolution_action_updated_date":"2022-10-01T23:28:12.000",
            "community_board":"01 BROOKLYN",
            "bbl":"3022320009",
            "borough":"BROOKLYN",
            "x_coordinate_state_plane":"996583",
            "y_coordinate_state_plane":"194956",
            "open_data_channel_type":"ONLINE",
            "park_facility_name":"Unspecified",
            "park_borough":"BROOKLYN",
            "latitude":"40.701777357509755",
            "longitude":"-73.95552043960885",
            "location":{"latitude":"40.701777357509755","longitude":"-73.95552043960885","human_address":"{\"address\": \"\", \"city\": \"\", \"state\": \"\", \"zip\": \"\"}"}
        }
        return [sample_311_request]

def test_e2e(spark_session, mocker):
    mocker.patch('extract_311_requests.requests.get',
        return_value= MockedResponse())

    extract_311_requests_to_s3()
    transform_311_requests(spark_session)
    load_tables_into_redshift(spark_session)

    reqeusts_df = spark_session.read \
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", REDSHIFT_URL) \
        .option("aws_iam_role", IAM_ROLE_ARN) \
        .option("Tempdir", "s3://311-dataset/") \
        .option("dbtable", "fact_service_request") \
        .load()

    requests = reqeusts_df.collect()
    
    assert len(requests) == 1
    assert requests[0]['unique_key'] == "55577067"
    assert requests[0]['complaint_type'] == "Animal Abuse"

    dates_df = spark_session.read \
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", REDSHIFT_URL) \
        .option("aws_iam_role", IAM_ROLE_ARN) \
        .option("Tempdir", "s3://311-dataset/") \
        .option("dbtable", "dim_date") \
        .load()

    dates = dates_df.collect()
    
    assert len(requests) == 1
    assert dates[0]['date_key'] == 20221001

    location_df = spark_session.read \
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", REDSHIFT_URL) \
        .option("aws_iam_role", IAM_ROLE_ARN) \
        .option("Tempdir", "s3://311-dataset/") \
        .option("dbtable", "dim_location_type") \
        .load()

    locations = location_df.collect()
    
    assert len(locations) == 1
    assert locations[0]['incident_address'] == "205 LEE AVENUE"
    assert locations[0]['incident_zip'] == "11206"