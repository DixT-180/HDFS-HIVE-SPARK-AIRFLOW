# # from pyspark.sql import SparkSession
# # from pyspark.sql.functions import col, when, lit
# # from pyspark.sql.types import *
# # import sys

# # file_prefix = sys.argv[1]

# # # file_prefix = "tested"
# # db_name = f"{file_prefix}_db"
# # table_name = f"{file_prefix}_cleaned"
# # schema = StructType([
# #     StructField("PassengerId", IntegerType()),
# #     StructField("Survived", IntegerType()),
# #     StructField("Pclass", IntegerType()),
# #     StructField("Name", StringType()),
# #     StructField("Sex", StringType()),
# #     StructField("Age", DoubleType()),
# #     StructField("SibSp", IntegerType()),
# #     StructField("Parch", IntegerType()),
# #     StructField("Ticket", StringType()),
# #     StructField("Fare", DoubleType()),
# #     StructField("Cabin", StringType()),
# #     StructField("Embarked", StringType()),
# # ])

# # spark = SparkSession.builder \
# #     .appName("test-iceberg-write") \
# #     .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
# #     .config("spark.sql.catalog.local.type", "hadoop") \
# #     .config("spark.sql.catalog.local.warehouse", "hdfs://namenode:9000/warehouse/iceberg") \
# #     .getOrCreate()

# # df = spark.read.option("header", True).schema(schema).csv("hdfs://namenode:9000/user/staging_area/tested_part*.csv")

# # for col_name in df.columns:
# #     df = df.withColumnRenamed(col_name, col_name.lower())

# # print("Count:", df.count())
# # df.show(5)

# # median_age = 28.0
# # df = df.withColumn("age", when(col("age").isNull(), lit(median_age)).otherwise(col("age")))
# # df = df.drop("embarked", "cabin", "parch", "sibsp")
# # keep_cols = ["passengerid", "survived", "pclass", "name", "sex", "age", "ticket", "fare"]
# # df = df.select(*keep_cols)
# # print("Rows to write:", df.count())
# # df.show(5)

# # spark.sql(f"CREATE DATABASE IF NOT EXISTS local.{db_name}")
# # spark.sql(f"""
# #     CREATE TABLE IF NOT EXISTS local.{db_name}.{table_name} (
# #       passengerid INT,
# #       survived INT,
# #       pclass INT,
# #       name STRING,
# #       sex STRING,
# #       age DOUBLE,
# #       ticket STRING,
# #       fare DOUBLE
# #     ) USING ICEBERG
# # """)

# # df.write.format("iceberg").mode("append").save(f"local.{db_name}.{table_name}")
# # print("✅ Data written to Iceberg!")


# # #########################################################################3



# from pyspark.sql import SparkSession
# from pyspark.sql.types import *
# import sys

# # ======== Parameters ========
# file_prefix = sys.argv[1] if len(sys.argv) > 1 else "tested"
# db_name = f"{file_prefix}_db"
# table_name = f"{file_prefix}_cleaned"

# schema = StructType([
#     StructField("PassengerId", IntegerType()),
#     StructField("Survived", IntegerType()),
#     StructField("Pclass", IntegerType()),
#     StructField("Name", StringType()),
#     StructField("Sex", StringType()),
#     StructField("Age", DoubleType()),
#     StructField("SibSp", IntegerType()),
#     StructField("Parch", IntegerType()),
#     StructField("Ticket", StringType()),
#     StructField("Fare", DoubleType()),
#     StructField("Cabin", StringType()),
#     StructField("Embarked", StringType()),
# ])

# # ======== Spark Session ========
# spark = SparkSession.builder \
#     .appName("test-iceberg-write") \
#     .config("spark.executor.cores", "2") \
#     .config("spark.executor.memory", "2g") \
#     .config("spark.cores.max", "6") \
#     .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
#     .config("spark.sql.catalog.local.type", "hadoop") \
#     .config("spark.sql.catalog.local.warehouse", "hdfs://namenode:9000/warehouse/iceberg") \
#     .getOrCreate()

# # ======== Prepare Table ========
# spark.sql(f"CREATE DATABASE IF NOT EXISTS local.{db_name}")
# spark.sql(f"""
#     CREATE TABLE IF NOT EXISTS local.{db_name}.{table_name} (
#       passengerid INT,
#       survived INT,
#       pclass INT,
#       name STRING,
#       sex STRING,
#       age DOUBLE,
#       ticket STRING,
#       fare DOUBLE
#     ) USING ICEBERG
# """)

# # ======== Read Streaming DataFrame ========
# df = spark.readStream \
#     .option("header", True) \
#     .option("cleanSource", "archive") \
#     .option("sourceArchiveDir", "hdfs://namenode:9000/user/archive/") \
#     .option("maxFilesPerTrigger", 1) \
#     .schema(schema) \
#     .csv("hdfs://namenode:9000/user/staging_area/")



# df = df.withColumn("Name", lower(col("Name")))

# cols_to_drop = ["Embarked", "Cabin", "Parch", "SibSp"]
# df = df.drop(*cols_to_drop)

# # (Optional) Data cleaning/transformation here, e.g.:
# # df = df.withColumnRenamed("PassengerId", "passengerid") ...etc

# # ======== Write Stream to Iceberg ========
# checkpoint_path = f"hdfs://namenode:9000/user/staging_area/{db_name}_checkpoint"

# query = (
#     df.writeStream
#       .format("iceberg")
#       .outputMode("append")
#       .option("checkpointLocation", checkpoint_path)
#       .toTable(f"local.{db_name}.{table_name}")
# )

# print(f"✅ Started streaming to Iceberg table local.{db_name}.{table_name}")
# query.awaitTermination()



from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower
from pyspark.sql.types import *
import sys

# ======== Parameters ========
file_prefix = sys.argv[1] if len(sys.argv) > 1 else "tested"
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

# ======== Spark Session ========
spark = SparkSession.builder \
    .appName("test-iceberg-write") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.memory", "2g") \
    .config("spark.cores.max", "6") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "hdfs://namenode:9000/warehouse/iceberg") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()

# ======== Prepare Table ========
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

# ======== Read Streaming DataFrame ========
df = spark.readStream \
    .option("header", True) \
    .option("cleanSource", "archive") \
    .option("sourceArchiveDir", "hdfs://namenode:9000/user/archive/") \
    .option("maxFilesPerTrigger", 1) \
    .schema(schema) \
    .csv("hdfs://namenode:9000/user/staging_area/")

# ======== Transformations ========
# Convert column names to lowercase
for col_name in df.columns:
    df = df.withColumnRenamed(col_name, col_name.lower())

# Replace null ages with median (assuming median_age = 28.0 as in your batch script)
# median_age = 28.0
# df = df.withColumn("age", when(col("age").isNull(), lit(median_age)).otherwise(col("age")))

# Drop unnecessary columns
cols_to_drop = ["embarked", "cabin", "parch", "sibsp"]
df = df.drop(*cols_to_drop)

# ======== Write Stream to Iceberg ========
checkpoint_path = f"hdfs://namenode:9000/user/staging_area/{db_name}_checkpoint/"

query = (
    df.writeStream
      .format("iceberg")
      .outputMode("append")
      .option("checkpointLocation", checkpoint_path)
      .toTable(f"local.{db_name}.{table_name}")
)

print(f"✅ Started streaming to Iceberg table local.{db_name}.{table_name}")
query.awaitTermination()