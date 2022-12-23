import logging

def load_tables_into_redshift(sparkSession):
    complaint_type_df = sparkSession.sql("SELECT complaint_type_key, complaint_type_name FROM dim_complaint_type")
    
    redshift_database_name = "requests"
    redshift_user_name = "testuser1"
    url = f"jdbc:redshift:iam://redshift-cluster-1.cqljvt3iaanm.us-east-1.redshift.amazonaws.com:5439/{redshift_database_name}?user={redshift_user_name}"
    iam_role_arn = "arn:aws:iam::607143918644:role/redshift-access-to-s3"

    complaint_type_df.write\
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", url)\
        .option("aws_iam_role", iam_role_arn) \
        .option("Tempdir", "s3://311-dataset/")\
        .option("dbtable", "dim_complaint_type")\
        .mode("overwrite")\
        .save()

    date_df = sparkSession.sql("SELECT date_key, year, month, dayofyear FROM dim_date")
    
    date_df.write\
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", url)\
        .option("aws_iam_role", iam_role_arn) \
        .option("Tempdir", "s3://311-dataset/")\
        .option("dbtable", "dim_date")\
        .mode("overwrite")\
        .save()
    
    location_type_df = sparkSession.sql("SELECT location_type_key, location_type_name FROM dim_location_type")
    
    location_type_df.write\
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", url)\
        .option("aws_iam_role", iam_role_arn) \
        .option("Tempdir", "s3://311-dataset/")\
        .option("dbtable", "dim_location_type")\
        .mode("overwrite")\
        .save()
    
    fact_service_request = sparkSession.sql("""
    SELECT 
        unique_key,
        created_date_key,
        closed_date_key,
        complaint_type_key,
        incident_zip,
        incident_address,
        location_type_key,
        descriptor,
        latitude,
        longitude,
        resolution_description_1,
        resolution_description_2,
        cross_street_1,
        cross_street_2,
        intersection_street_1,
        intersection_street_2
    FROM 
        fact_service_request
    """)
    
    fact_service_request.write\
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", url)\
        .option("aws_iam_role", iam_role_arn) \
        .option("Tempdir", "s3://311-dataset/")\
        .option("dbtable", "fact_service_request")\
        .mode("overwrite")\
        .save()

    logging.info(f"Finished loading data.")