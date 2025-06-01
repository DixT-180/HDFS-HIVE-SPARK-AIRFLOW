import sys
import boto3
import pandas as pd
import io
from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("WriteToHDFS")
    .config("spark.driver.memory", "2g")         # 4 GB RAM for the driver
    .config("spark.executor.memory", "2g")       # 2 GB RAM per executor
    .config("spark.executor.cores", "4")         # 4 cores per executor
    .config("spark.cores.max", "4")              # Max total cores
    .getOrCreate()
)

# Args
bucket = sys.argv[1] if len(sys.argv) > 1 else "mybucket"
filename = sys.argv[2] if len(sys.argv) > 2 else "data.csv"
hdfs_target = "hdfs://namenode:9000/user/staging_area/" #+ filename

# Get data from MinIO with boto3
s3 = boto3.client(
    's3',
    endpoint_url='http://minio:9003',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    region_name='us-east-1',
)
obj = s3.get_object(Bucket=bucket, Key=filename)
df_pd = pd.read_csv(io.BytesIO(obj['Body'].read()))

# Create Spark session
spark = SparkSession.builder.appName("WriteToHDFS").getOrCreate()

# Convert pandas DataFrame to Spark DataFrame
df_spark = spark.createDataFrame(df_pd)

# Save as CSV to HDFS (can use .parquet, .json, .orc, etc.)
df_spark.write.mode("overwrite").csv(hdfs_target, header=True)
print(f"✅ Saved to HDFS: {hdfs_target}")

spark.stop()
