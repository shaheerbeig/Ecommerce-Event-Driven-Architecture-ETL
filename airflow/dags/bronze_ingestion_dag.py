"""
Bronze Ingestion DAG - RabbitMQ to MinIO Bronze Layer
Runs every 30 minutes to drain RabbitMQ and write to Bronze
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
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=25),
}

dag = DAG(
    'bronze_ingestion',
    default_args=default_args,
    description='Ingest events from RabbitMQ to Bronze layer (MinIO)',
    schedule_interval='*/30 * * * *',  # Every 30 minutes
    start_date=datetime(2026, 2, 20),
    catchup=False,
    max_active_runs=1,
    tags=['ingestion', 'bronze', 'rabbitmq'],
)

# Note: This runs in batch mode - processes messages then exits
# For continuous ingestion, bronze-events-ingestion service in docker-compose runs 24/7
bronze_ingestion = BashOperator(
    task_id='ingest_rabbitmq_to_bronze',
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
        /opt/project/ingestion/rabbitmq_to_bronze.py
    ''',
    dag=dag,
)

bronze_ingestion
