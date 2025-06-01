from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, lower, when
from pyspark.sql.functions import col

schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("gender", StringType())
])

spark = SparkSession.builder \
    .appName("stream-csv-to-iceberg") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.memory", "2g") \
    .config("spark.cores.max", "6") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "hdfs://namenode:9000/warehouse/iceberg") \
    .getOrCreate()



# .appName("stream-csv-to-iceberg") \
#     .config("spark.executor.cores", "2") \
#     .config("spark.executor.memory", "1g") \
#     .config("spark.cores.max", "6") \

df = spark.readStream \
    .option("header", True) \
    .schema(schema) \
    .csv("hdfs://namenode:9000/user/staging_area/")



df = df \
    .filter(col("age") >= 25) \
    .withColumn("gender", lower(col("gender"))) \
    .withColumn(
        "age",
        when(col("age") < 35, "young")
        .when((col("age") >= 35) & (col("age") < 50), "middle-aged")
        .otherwise("senior")
    )

# Create the database if it doesn't exist
# spark.sql("DROP TABLE IF EXISTS local.people_db.people")

spark.sql("CREATE DATABASE IF NOT EXISTS local.people_db")

# Create Iceberg table if not exists (schema must match your data)
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.people_db.people (
      id INT,
      name STRING,
      age STRING,
      gender STRING
    ) USING ICEBERG
""")


#continuos streaming

# query = df.writeStream \
#     .format("iceberg") \
#     .outputMode("append") \
#     .option("checkpointLocation", "hdfs://namenode:9000/user/streaming_checkpoint/people") \
#     .trigger(processingTime="10 seconds") \
#     .toTable("local.people_db.people")

# query.awaitTermination()




#batch streaming which we are doing
query = df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", "hdfs://namenode:9000/user/streaming_checkpoint/people") \
    .trigger(once=True) \
    .toTable("local.people_db.people")

query.awaitTermination()

