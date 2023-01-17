import logging
import time


REDSHIFT_URL = f"jdbc:redshift:iam://redshift-cluster-1.cqljvt3iaanm.us-east-1.redshift.amazonaws.com:5439/requests?user=testuser1"
IAM_ROLE_ARN = "arn:aws:iam::607143918644:role/redshift-access-to-s3"

def load_tables_into_redshift(sparkSession):
    start_time = time.time()

    date_df = sparkSession.sql("SELECT date_key, year, month, dayofyear FROM dim_date")
    
    date_df.write\
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", REDSHIFT_URL)\
        .option("aws_iam_role", IAM_ROLE_ARN) \
        .option("Tempdir", "s3://311-dataset/")\
        .option("dbtable", "dim_date")\
        .mode("overwrite")\
        .save()
    
    location_df = sparkSession.sql("""
        SELECT 
            location_key, 
            incident_address, 
            incident_zip, 
            location_type, 
            latitude, 
            longitude,
            cross_street_1,
            cross_street_2 
        FROM dim_location
    """)
    
    location_df.write\
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", REDSHIFT_URL)\
        .option("aws_iam_role", IAM_ROLE_ARN) \
        .option("Tempdir", "s3://311-dataset/")\
        .option("dbtable", "dim_location_type")\
        .mode("overwrite")\
        .save()
    
    fact_service_request = sparkSession.sql("""
    SELECT 
        unique_key,
        created_date_key,
        closed_date_key,
        complaint_type,
        descriptor,
        location_key,
        resolution_description_1,
        resolution_description_2
    FROM 
        fact_service_request
    """)
    
    fact_service_request.write\
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", REDSHIFT_URL)\
        .option("aws_iam_role", IAM_ROLE_ARN) \
        .option("Tempdir", "s3://311-dataset/")\
        .option("dbtable", "fact_service_request")\
        .mode("overwrite")\
        .save()

    end_time = time.time()
    elapsed_time = end_time - start_time

    logging.info(f"Finished loading data. [elapsed_time={elapsed_time}]")