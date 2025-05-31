from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
     "execution_timeout": timedelta(minutes=30),
     
}

with DAG(
    dag_id='extract_people_from_minio',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['extract', 'minio', 'spark'],
) as dag:

    extract_task = SSHOperator(
        task_id='extract_people_csv',
        ssh_hook= SSHHook(ssh_conn_id='spark_master_ssh', cmd_timeout=200),
        # ssh_conn_id='spark_master_ssh',
        command=(
            "export JAVA_HOME=/usr/local/openjdk-11 && "
            "/opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 --deploy-mode client "
            "/opt/spark-jobs/read_people_csv.py "
            "{{ dag_run.conf['bucket'] }} {{ dag_run.conf['filename'] }}"
        ),
              # Optional: SSH connection timeout (seconds)
    )
