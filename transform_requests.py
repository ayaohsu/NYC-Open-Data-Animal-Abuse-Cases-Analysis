
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

APP_NAME = "kaporos_311_requests_analysis"

def transform_311_requests():
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    s3_uri = f"s3a://{S3_BUCKET_NAME}/{S3_FILE_NAME}"
    requests_311 = spark.read.json(s3_uri)
    requests_311.printSchema()
    requests_311.createOrReplaceTempView("complaints")
    spark.sql(
    """
        SELECT year(created_date), COUNT(*) 
        FROM complaints 
        GROUP BY year(created_date)
    """).show()
    
    spark.stop()