
def load_tables_into_redshift(sparkSession):
    complaint_type_df = sparkSession.sql("SELECT complaint_type_key, complaint_type_name FROM dim_complaint_type")

    url = "jdbc:redshift:iam://redshift-cluster-1.cqljvt3iaanm.us-east-1.redshift.amazonaws.com:5439/"
    iam_role_arn = "arn:aws:iam::607143918644:role/redshift-access-to-s3"
    
    complaint_type_df.write\
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", url)\
        .option("user", "testuser1")
        .option("Tempdir", "s3://311-dataset/")\
        .option("dbtable", "dim_complaint_type")\
        .option("aws_iam_role", iam_role_arn) \
        .mode("error")\
        .save()