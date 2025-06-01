from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Read from Iceberg") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "hdfs://namenode:9000/warehouse/iceberg") \
    .getOrCreate()

df = spark.read.format("iceberg").load("local.people_db.people")
df.show()  # Or whatever processing you want

# Optionally write results, aggregate, etc.
