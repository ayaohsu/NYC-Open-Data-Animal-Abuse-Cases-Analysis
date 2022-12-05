
def load_tables_into_redshift(sparkSession):
    complaint_type_df = sparkSession.sql("SELECT complaint_type_key, complaint_type_name FROM dim_complaint_type")

    complaint_type_df.write\
        .format("jdbc")\
        .option("url","jdbc:redshift://redshift-cluster-1.cqljvt3iaanm.us-east-1.redshift.amazonaws.com:5439/dev")\
        .option("user", "testuser1")\
        .option("Tempdir", "s3://311-dataset/")\
        .option("dbtable", "dim_complaint_type")\
        .save()