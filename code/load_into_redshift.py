import logging

REDSHIFT_DATABASE_NAME = "requests"
REDSHIFT_USER_NAME = "testuser1"
REDSHIFT_URL = f"jdbc:redshift:iam://redshift-cluster-1.cqljvt3iaanm.us-east-1.redshift.amazonaws.com:5439/{REDSHIFT_DATABASE_NAME}?user={REDSHIFT_USER_NAME}"
IAM_ROLE_ARN = "arn:aws:iam::607143918644:role/redshift-access-to-s3"


def load_tables_into_redshift(sparkSession):
    complaint_type_df = sparkSession.sql("SELECT complaint_type_key, complaint_type_name FROM dim_complaint_type")
    
    complaint_type_df.write\
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", REDSHIFT_URL)\
        .option("aws_iam_role", IAM_ROLE_ARN) \
        .option("Tempdir", "s3://311-dataset/")\
        .option("dbtable", "dim_complaint_type")\
        .mode("error")\
        .save()

    date_df = sparkSession.sql("SELECT date_key, year, month, dayofyear FROM dim_date")
    
    date_df.write\
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", REDSHIFT_URL)\
        .option("aws_iam_role", IAM_ROLE_ARN) \
        .option("Tempdir", "s3://311-dataset/")\
        .option("dbtable", "dim_date")\
        .mode("error")\
        .save()
    
    location_type_df = sparkSession.sql("SELECT location_type_key, location_type_name FROM dim_location_type")
    
    location_type_df.write\
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", REDSHIFT_URL)\
        .option("aws_iam_role", IAM_ROLE_ARN) \
        .option("Tempdir", "s3://311-dataset/")\
        .option("dbtable", "dim_location_type")\
        .mode("error")\
        .save()
    
    fact_service_request = sparkSession.sql("""
    SELECT 
        unique_key,
        created_date_key,
        closed_date_key,
        complaint_type_key,
        incident_zip,
        location_type_key,
        descriptor,
        human_address,
        latitude,
        longitude,
        resolution_description,
        cross_street_1,
        cross_street_2,
        intersection_street_1,
        intersection_street_2
    FROM 
        fact_service_request
    """)
    
    fact_service_request.write\
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", REDSHIFT_URL)\
        .option("aws_iam_role", IAM_ROLE_ARN) \
        .option("Tempdir", "s3://311-dataset/")\
        .option("dbtable", "fact_service_request")\
        .mode("error")\
        .save()

    logging.info(f"Finished loading data.")