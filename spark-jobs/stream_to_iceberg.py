from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, when
from pyspark.sql.functions import col
from pyspark.sql.types import *
import sys


file_prefix = sys.argv[1] if len(sys.argv) > 1 else "tested"
db_name = f"{file_prefix}_db"
table_name = f"{file_prefix}_cleaned"



# schema = StructType([
#     StructField("id", IntegerType()),
#     StructField("name", StringType()),
#     StructField("age", IntegerType()),
#     StructField("gender", StringType())
# ])

schema = StructType([
    StructField("PassengerId", IntegerType()),
    StructField("Survived", IntegerType()),
    StructField("Pclass", IntegerType()),
    StructField("Name", StringType()),
    StructField("Sex", StringType()),
    StructField("Age", DoubleType()),
    StructField("SibSp", IntegerType()),
    StructField("Parch", IntegerType()),
    StructField("Ticket", StringType()),
    StructField("Fare", DoubleType()),
    StructField("Cabin", StringType()),
    StructField("Embarked", StringType()),
])

spark = SparkSession.builder \
    .appName("stream-csv-to-iceberg") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.memory", "2g") \
    .config("spark.cores.max", "6") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "hdfs://namenode:9000/warehouse/iceberg") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.jars", "/opt/spark/external-jars/iceberg-spark-runtime-3.5_2.12-1.9.0.jar") \
    .getOrCreate()





df = spark.readStream \
    .option("header", True) \
    .option("cleanSource", "archive") \
    .option("sourceArchiveDir", "hdfs://namenode:9000/user/archive/") \
    .option("maxFilesPerTrigger", 1) \
    .schema(schema) \
    .csv("hdfs://namenode:9000/user/staging_area/")


df = df.withColumn("Name", lower(col("Name")))

cols_to_drop = ["Embarked", "Cabin", "Parch", "SibSp"]
df = df.drop(*cols_to_drop)





# df = df \
#     .filter(col("age") >= 25) \
#     .withColumn("gender", lower(col("gender"))) \
#     .withColumn(
#         "age",
#         when(col("age") < 35, "young")
#         .when((col("age") >= 35) & (col("age") < 50), "middle-aged")
#         .otherwise("senior")
#     )

# Create the database if it doesn't exist
# spark.sql("DROP TABLE IF EXISTS local.people_db.people")

spark.sql("CREATE DATABASE IF NOT EXISTS local.people_db")

# Create Iceberg table if not exists (schema must match your data)
spark.sql("""
   CREATE TABLE IF NOT EXISTS local.people_db.people (
    passengerid INT,
    survived INT,
    pclass INT,
    name STRING,
    sex STRING,
    age DOUBLE,
    ticket STRING,
    fare DOUBLE
) USING ICEBERG
    PARTITIONED BY (pclass)

""")




#batch streaming which we are doing
query = df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", "hdfs://namenode:9000/user/streaming_checkpoint/people") \
    .trigger(processingTime="5 seconds") \
    .toTable("local.people_db.people")

query.awaitTermination()

  # .trigger(once=True) \