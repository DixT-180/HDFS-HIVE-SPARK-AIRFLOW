# # import sys
# # import boto3
# # import pandas as pd
# # import io
# # from pyspark.sql import SparkSession


# # spark = (
# #     SparkSession.builder
# #     .appName("WriteToHDFS")
# #     .config("spark.driver.memory", "2g")         # 4 GB RAM for the driver
# #     .config("spark.executor.memory", "2g")       # 2 GB RAM per executor
# #     .config("spark.executor.cores", "4")         # 4 cores per executor
# #     .config("spark.cores.max", "4")              # Max total cores
# #     .getOrCreate()
# # )

# # # Args
# # bucket = sys.argv[1] if len(sys.argv) > 1 else "mybucket"
# # filename = sys.argv[2] if len(sys.argv) > 2 else "data.csv"
# # hdfs_target = "hdfs://namenode:9000/user/staging_area/" #+ filename

# # # Get data from MinIO with boto3
# # s3 = boto3.client(
# #     's3',
# #     endpoint_url='http://minio:9003',
# #     aws_access_key_id='minioadmin',
# #     aws_secret_access_key='minioadmin',
# #     region_name='us-east-1',
# # )
# # obj = s3.get_object(Bucket=bucket, Key=filename)
# # df_pd = pd.read_csv(io.BytesIO(obj['Body'].read()))

# # # Create Spark session
# # spark = SparkSession.builder.appName("WriteToHDFS").getOrCreate()

# # # Convert pandas DataFrame to Spark DataFrame
# # df_spark = spark.createDataFrame(df_pd)

# # # Save as CSV to HDFS (can use .parquet, .json, .orc, etc.)
# # df_spark.write.mode("overwrite").csv(hdfs_target, header=True)
# # print(f"✅ Saved to HDFS: {hdfs_target}")

# # spark.stop()


# import sys
# import boto3
# import pandas as pd
# import io
# from pyspark.sql import SparkSession

# spark = (
#     SparkSession.builder
#     .appName("WriteToHDFS")
#     .config("spark.driver.memory", "1g")
#     .config("spark.executor.memory", "1g")
#     .config("spark.executor.cores", "4")
#     .config("spark.cores.max", "4")
#     .getOrCreate()
# )

# # Args
# bucket = sys.argv[1] if len(sys.argv) > 1 else "mybucket"
# filename = sys.argv[2] if len(sys.argv) > 2 else "data.csv"
# output_dir = "hdfs://namenode:9000/user/staging_area/"  # <-- change this to your local output path

# # Get data from MinIO with boto3
# s3 = boto3.client(
#     's3',
#     endpoint_url='http://minio:9003',
#     aws_access_key_id='minioadmin',
#     aws_secret_access_key='minioadmin',
#     region_name='us-east-1',
# )
# obj = s3.get_object(Bucket=bucket, Key=filename)
# df_pd = pd.read_csv(io.BytesIO(obj['Body'].read()))

# # Convert pandas DataFrame to Spark DataFrame
# df_spark = spark.createDataFrame(df_pd)

# row_count = df_spark.count()
# print(f"Number of rows: {row_count}")

# # Write using default Spark partitioning (could be multiple output part files)
# df_spark.write.mode("append").csv(output_dir, header=True)

# print(f"Appended CSV(s) to: {output_dir}")

# spark.stop()




###################33
import sys
import pyarrow.csv as pacsv
import pyarrow.fs as pafs
import pandas as pd
from pyspark.sql import SparkSession

# Args
bucket = sys.argv[1] if len(sys.argv) > 1 else "mybucket"
filename = sys.argv[2] if len(sys.argv) > 2 else "data.csv"
hdfs_parquet_path = f"hdfs://namenode:9000/user/staging_area/"

# Connect to MinIO via PyArrow S3FileSystem
s3 = pafs.S3FileSystem(
    endpoint_override="http://minio:9003",
    access_key="minioadmin",
    secret_key="minioadmin",
    region="us-east-1"
)
s3_path = f"{bucket}/{filename}"

# Read CSV from S3 (MinIO) as Arrow Table
with s3.open_input_file(s3_path) as f:
    arrow_table = pacsv.read_csv(f)

# Convert Arrow Table to Pandas DataFrame
df_pd = arrow_table.to_pandas()

# Create Spark session
spark = (
    SparkSession.builder
    .appName("WriteToHDFS")
    .config("spark.driver.memory", "1g")
    .config("spark.executor.memory", "1g")
    .config("spark.executor.cores", "4")
    .config("spark.cores.max", "4")
    .getOrCreate()
)

# Create Spark DataFrame from Pandas DataFrame
df_spark = spark.createDataFrame(df_pd)
df_spark = df_spark.repartition(3) 
for c in df_spark.columns:
    df_spark = df_spark.withColumnRenamed(c, c.lower())

df_spark.show()
# Show row count (optional)
print(f"Number of rows: {df_spark.count()}")
df_spark.printSchema()

# Write Spark DataFrame as Parquet to HDFS
df_spark.write.mode("append").parquet(hdfs_parquet_path)

print(f"✅ Wrote Parquet to HDFS: {hdfs_parquet_path}")

spark.stop()
