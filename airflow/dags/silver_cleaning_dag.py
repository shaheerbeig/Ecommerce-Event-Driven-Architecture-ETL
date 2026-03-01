"""
Silver Cleaning DAG - Bronze to Silver Layer
Runs hourly to clean and validate Bronze events, write to Silver
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=10),
}

dag = DAG(
    'silver_cleaning',
    default_args=default_args,
    description='Clean Bronze events and write to Silver layer',
    schedule_interval='0 * * * *',  # Every hour at :00
    start_date=datetime(2026, 2, 20),
    catchup=False,
    max_active_runs=1,
    tags=['transformation', 'silver', 'cleaning'],
)

# Process previous hour's data
# {{ ds }} = execution date (YYYY-MM-DD)
# {{ execution_date.hour }} = hour of execution
silver_cleaning = BashOperator(
    task_id='bronze_to_silver_clean',
    bash_command=''' 
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master local[*] \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        --conf spark.hadoop.fs.s3a.endpoint=$MINIO_ENDPOINT \
        --conf spark.hadoop.fs.s3a.access.key=$MINIO_ROOT_USER \
        --conf spark.hadoop.fs.s3a.secret.key=$MINIO_ROOT_PASSWORD \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
        --conf spark.executor.memory=1g \
        /opt/project/transformations/bronze_to_silver_clean.py {{ ds }} {{ execution_date.hour }}
    ''',
    dag=dag,
)

silver_cleaning
