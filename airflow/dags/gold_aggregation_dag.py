"""
Gold Aggregation DAG - Silver to Gold Layer
Runs daily to enrich events with dimensions and create aggregated fact tables
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': True,  # Wait for previous day's run
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=15),
}

dag = DAG(
    'gold_aggregation',
    default_args=default_args,
    description='Enrich Silver events with dimensions and aggregate to Gold layer',
    schedule_interval='0 2 * * *',  # Daily at 2:00 AM
    start_date=datetime(2026, 2, 20),
    catchup=False,
    max_active_runs=1,
    tags=['transformation', 'gold', 'aggregation'],
)

# Process previous day's data
# {{ ds }} = execution date (YYYY-MM-DD)
# {{ yesterday_ds }} = previous day
gold_aggregation = BashOperator(
    task_id='silver_to_gold_aggregate',
    bash_command='''
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master local[*] \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.1 \
        --conf spark.hadoop.fs.s3a.endpoint=$MINIO_ENDPOINT \
        --conf spark.hadoop.fs.s3a.access.key=$MINIO_ROOT_USER \
        --conf spark.hadoop.fs.s3a.secret.key=$MINIO_ROOT_PASSWORD \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
        --conf spark.executor.memory=1g \
        --conf spark.sql.shuffle.partitions=50 \
        /opt/project/transformations/silver_to_gold.py {{ yesterday_ds }}
    ''',
    dag=dag,
)

gold_aggregation
