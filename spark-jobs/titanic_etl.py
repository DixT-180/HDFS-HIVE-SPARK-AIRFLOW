from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import *
import sys

file_prefix = sys.argv[1]

# file_prefix = "tested"
db_name = f"{file_prefix}_db"
table_name = f"{file_prefix}_cleaned"
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
    .appName("test-iceberg-write") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "hdfs://namenode:9000/warehouse/iceberg") \
    .getOrCreate()

df = spark.read.option("header", True).schema(schema).csv("hdfs://namenode:9000/user/staging_area/tested_part*.csv")

for col_name in df.columns:
    df = df.withColumnRenamed(col_name, col_name.lower())

print("Count:", df.count())
df.show(5)

median_age = 28.0
df = df.withColumn("age", when(col("age").isNull(), lit(median_age)).otherwise(col("age")))
df = df.drop("embarked", "cabin", "parch", "sibsp")
keep_cols = ["passengerid", "survived", "pclass", "name", "sex", "age", "ticket", "fare"]
df = df.select(*keep_cols)
print("Rows to write:", df.count())
df.show(5)

spark.sql(f"CREATE DATABASE IF NOT EXISTS local.{db_name}")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS local.{db_name}.{table_name} (
      passengerid INT,
      survived INT,
      pclass INT,
      name STRING,
      sex STRING,
      age DOUBLE,
      ticket STRING,
      fare DOUBLE
    ) USING ICEBERG
""")

df.write.format("iceberg").mode("append").save(f"local.{db_name}.{table_name}")
print("✅ Data written to Iceberg!")
