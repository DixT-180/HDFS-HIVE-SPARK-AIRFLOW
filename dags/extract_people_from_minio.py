# from airflow import DAG
# from airflow.providers.ssh.operators.ssh import SSHOperator
# from airflow.utils.dates import days_ago
# from datetime import timedelta
# from airflow.providers.ssh.hooks.ssh import SSHHook
# from airflow.providers.ssh.operators.ssh import SSHOperator

# default_args = {
#     'owner': 'airflow',
#     'retries': 1,
#     'retry_delay': timedelta(minutes=2),
#      "execution_timeout": timedelta(minutes=30),
     
# }

# with DAG(
#     dag_id='extract_people_from_minio',
#     default_args=default_args,
#     schedule_interval=None,
#     start_date=days_ago(1),
#     catchup=False,
#     tags=['extract', 'minio', 'spark'],
# ) as dag:

#     extract_task = SSHOperator(
#         task_id='extract_people_csv',
#         ssh_hook= SSHHook(ssh_conn_id='spark_master_ssh', cmd_timeout=200),
#         # ssh_conn_id='spark_master_ssh',
#         command=(
#             "export JAVA_HOME=/usr/local/openjdk-11 && "
#             "/opt/spark/bin/spark-submit "
#             "--master spark://spark-master:7077 --deploy-mode client "
#             "/opt/spark-jobs/read_people_csv.py "
#             "{{ dag_run.conf['bucket'] }} {{ dag_run.conf['filename'] }}"
#         ),
#               # Optional: SSH connection timeout (seconds)
#     )


from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.providers.ssh.hooks.ssh import SSHHook

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=30),
}

with DAG(
    dag_id='extract_people_from_minio',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['extract', 'minio', 'spark'],
) as dag:

    # Step 1: Extract CSV from Minio (user-supplied bucket & filename)
    extract_task = SSHOperator(
        task_id='extract_people_csv',
        ssh_hook=SSHHook(ssh_conn_id='spark_master_ssh', cmd_timeout=200),
        command=(
            "export JAVA_HOME=/usr/local/openjdk-11 && "
            "/opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 --deploy-mode client "
            "/opt/spark-jobs/read_people_csv.py "
            "{{ dag_run.conf['bucket'] }} {{ dag_run.conf['filename'] }}"
        ),
        do_xcom_push=False
    )

    # Step 2: Run Spark streaming (batch) job to Iceberg
    stream_to_iceberg_task = SSHOperator(
        task_id='stream_people_to_iceberg',
        ssh_hook=SSHHook(ssh_conn_id='spark_master_ssh', cmd_timeout=200),
        command=(
            "export JAVA_HOME=/usr/local/openjdk-11 && "
            "/opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 --deploy-mode client "
            "--jars /opt/spark/external-jars/iceberg-spark-runtime-3.5_2.12-1.9.0.jar "
            "/opt/spark-jobs/stream_to_iceberg.py "
            # add more args if your script expects them, e.g., paths or params
        ),
        do_xcom_push=False
    )


    # Step 3: Read/process from Iceberg table
    read_from_iceberg_task = SSHOperator(
        task_id='read_from_iceberg',
        ssh_hook=SSHHook(ssh_conn_id='spark_master_ssh', cmd_timeout=200),
        command=(
            "export JAVA_HOME=/usr/local/openjdk-11 && "
            "/opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 --deploy-mode client "
            "--jars /opt/spark/external-jars/iceberg-spark-runtime-3.5_2.12-1.9.0.jar "
            "/opt/spark-jobs/read_from_iceberg_table.py"
            # Add arguments if needed, like table names, output paths, etc.
        ),
        do_xcom_push=False
    )








    extract_task >> stream_to_iceberg_task >> read_from_iceberg_task
