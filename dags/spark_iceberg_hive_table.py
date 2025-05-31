from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="spark_iceberg_hive_table",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["spark", "iceberg", "hive"],
) as dag:

    spark_iceberg_job = SSHOperator(
    task_id="run_iceberg_hive_job",
    ssh_conn_id="spark_master_ssh",
    command=(
        "export JAVA_HOME=/usr/local/openjdk-11 && "
        "/opt/spark/bin/spark-submit "
        "--jars /opt/spark/external-jars/iceberg-spark-runtime-3.5_2.12-1.9.0.jar "
        "--master spark://spark-master:7077 --deploy-mode client "
        "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
        "--conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog "
        "--conf spark.sql.catalog.spark_catalog.type=hive "
        "--conf spark.sql.catalogImplementation=hive "   # <---- THIS LINE IS NEEDED!
        "--conf spark.hadoop.hive.metastore.uris=thrift://metastore:9083 "
        "--conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 "
        "--conf spark.sql.warehouse.dir=hdfs://namenode:9000/user/hive/warehouse "
        "--conf spark.hadoop.iceberg.engine.hive.enabled=true "
        "--conf spark.eventLog.enabled=true "
        "--conf spark.eventLog.dir=hdfs://namenode:9000/spark-logs "
        "/opt/spark-jobs/write_iceberg_hive.py "
    
    ),

    get_pty=True,
)
