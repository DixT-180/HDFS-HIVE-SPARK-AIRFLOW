# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, lower
# from pyspark.sql.types import *

# import sys

# file_prefix = sys.argv[1] if len(sys.argv) > 1 else "tested"
# db_name = f"{file_prefix}_db"
# table_name = f"{file_prefix}_cleaned"

# schema = StructType([
#     StructField("PassengerId", LongType()),
#     StructField("Survived", LongType()),
#     StructField("Pclass", LongType()),
#     StructField("Name", StringType()),
#     StructField("Sex", StringType()),
#     StructField("Age", DoubleType()),
#     StructField("SibSp", LongType()),
#     StructField("Parch", LongType()),
#     StructField("Ticket", StringType()),
#     StructField("Fare", DoubleType()),
#     StructField("Cabin", StringType()),
#     StructField("Embarked", StringType()),
# ])

# spark = SparkSession.builder \
#     .appName("stream-csv-to-iceberg") \
#     .config("spark.executor.memory", "1g") \
#     .config("spark.cores.max", "8") \
#     .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
#     .config("spark.sql.catalog.local.type", "hadoop") \
#     .config("spark.sql.catalog.local.warehouse", "hdfs://namenode:9000/warehouse/hdfs-iceberg") \
#     .getOrCreate()

# # Streaming read
# df = spark.readStream \
#     .format("parquet") \
#     .option("header", True) \
#     .option("cleanSource", "archive") \
#     .option("sourceArchiveDir", "hdfs://namenode:9000/user/hdfs-archive/archive/") \
#     .option("maxFilesPerTrigger", 5) \
#     .schema(schema) \
#     .load("hdfs://namenode:9000/user/staging_area/")

# # Transformations
# df = df.withColumn("Name", lower(col("Name")))
# df = df.drop("Embarked", "Cabin", "Parch", "SibSp")

# # Create DB & Table (adjust as needed for your Iceberg catalog)
# spark.sql("CREATE DATABASE IF NOT EXISTS local.people_db_hdfs")
# spark.sql("""
#    CREATE TABLE IF NOT EXISTS local.people_db_hdfs.people_hdfs (
#       passengerid BIGINT,
#       survived BIGINT,
#       pclass BIGINT,
#       name STRING,
#       sex STRING,
#       age DOUBLE,
#       ticket STRING,
#       fare DOUBLE
#    )
#    USING ICEBERG
#    PARTITIONED BY (pclass)
# """)

# query = df.writeStream \
#     .format("iceberg") \
#     .outputMode("append") \
#     .option("checkpointLocation", "hdfs://namenode:9000/user/streaming_checkpoint/people_hdfs") \
#     .trigger(processingTime="5 seconds") \
#     .toTable("local.people_db_hdfs.people_hdfs")

# query.awaitTermination()


#   # .trigger(once=True) \








# For hive iceberg table


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower
from pyspark.sql.types import *
import sys

file_prefix = sys.argv[1] if len(sys.argv) > 1 else "tested"

schema = StructType([
     StructField("PassengerId", LongType()),
    StructField("Survived", LongType()),
    StructField("Pclass", LongType()),
    StructField("Name", StringType()),
    StructField("Sex", StringType()),
    StructField("Age", DoubleType()),
    StructField("SibSp", LongType()),
    StructField("Parch", LongType()),
    StructField("Ticket", StringType()),
    StructField("Fare", DoubleType()),
    StructField("Cabin", StringType()),
    StructField("Embarked", StringType()),
])

spark = SparkSession.builder \
    .appName("stream-csv-to-iceberg") \
    .config("spark.executor.memory", "1g") \
    .config("spark.cores.max", "8") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://metastore:9083") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.hadoop.iceberg.engine.hive.enabled", "true") \
    .getOrCreate()

df = spark.readStream \
    .format("parquet") \
    .option("header", True) \
    .option("cleanSource", "archive") \
    .option("sourceArchiveDir", "hdfs://namenode:9000/user/archive/") \
    .option("maxFilesPerTrigger", 5) \
    .schema(schema) \
    .load("hdfs://namenode:9000/user/staging_area/") \
    #.csv("hdfs://namenode:9000/user/staging_area/")

df = df.withColumn("Name", lower(col("Name")))
cols_to_drop = ["Embarked", "Cabin", "Parch", "SibSp"]
df = df.drop(*cols_to_drop)

# for c in df.columns:
#     df = df.withColumnRenamed(c, c.lower())

spark.sql("""
   CREATE DATABASE IF NOT EXISTS people_db
""")

# No USE statement needed if you specify people_db.people
spark.sql("""
   CREATE TABLE IF NOT EXISTS people_db.people (
  passengerid BIGINT,
  survived BIGINT,
  pclass BIGINT,
  name STRING,
  sex STRING,
  age DOUBLE,
  ticket STRING,
  fare DOUBLE
)
USING ICEBERG
PARTITIONED BY (pclass)
LOCATION 'hdfs://namenode:9000/user/hive/warehouse'
TBLPROPERTIES ('engine.hive.enabled'='true');
""")

query = df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", "hdfs://namenode:9000/user/streaming_checkpoint/people") \
    .trigger(processingTime="5 seconds") \
    .toTable("people_db.people")

query.awaitTermination()
