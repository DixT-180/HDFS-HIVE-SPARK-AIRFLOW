# from pyspark.sql import SparkSession

# spark = SparkSession.builder \
#     .appName("Read from Iceberg") \
#     .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
#     .config("spark.sql.catalog.local.type", "hadoop") \
#     .config("spark.sql.catalog.local.warehouse", "hdfs://namenode:9000/warehouse/iceberg") \
#     .getOrCreate()

# df = spark.read.format("iceberg").load("local.people_db.people")
# df.show()  # Or whatever processing you want

# # Optionally write results, aggregate, etc.

#########################################################3


import sys
from pyspark.sql import SparkSession

file_prefix = sys.argv[1] if len(sys.argv) > 1 else "titanic"
db_name = f"{file_prefix}_db"
table_name = f"{file_prefix}_cleaned"

spark = SparkSession.builder \
    .appName("Read from Iceberg") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "hdfs://namenode:9000/warehouse/iceberg") \
    .getOrCreate()

df = spark.read.format("iceberg").load(f"local.{db_name}.{table_name}")
df.show()
