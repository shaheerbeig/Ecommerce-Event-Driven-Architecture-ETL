"""
Master Pipeline DAG - Full Bronze → Silver → Gold Flow
Orchestrates the complete data pipeline with dependencies
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'master_pipeline_daily',
    default_args=default_args,
    description='Full pipeline: Bronze → Silver → Gold with dependencies',
    schedule_interval='0 3 * * *',  # Daily at 3:00 AM
    start_date=datetime(2026, 2, 20),
    catchup=False,
    max_active_runs=1,
    tags=['master', 'pipeline', 'full-flow'],
)

start = DummyOperator(
    task_id='start_pipeline',
    dag=dag,
)

# Process all hours for previous day (Silver layer)
process_silver = BashOperator(
    task_id='process_all_hours_to_silver',
    bash_command=r"""
    for hour in {0..23}; do
        hour_padded=$(printf "%02d" $hour)
        echo "🔵 Processing hour: $hour_padded"
        docker exec spark-master bash -c "
            export SPARK_HOME=/opt/spark && 
            /opt/spark/bin/spark-submit \
                --master local[2] \
                --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
                --conf spark.hadoop.fs.s3a.endpoint=${MINIO_ENDPOINT} \
                --conf spark.hadoop.fs.s3a.access.key=${MINIO_ROOT_USER} \
                --conf spark.hadoop.fs.s3a.secret.key=${MINIO_ROOT_PASSWORD} \
                --conf spark.hadoop.fs.s3a.path.style.access=true \
                --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
                /opt/project/transformations/bronze_to_silver_clean.py {{ yesterday_ds }} $hour_padded || exit 1
        "
    done
    echo "✅ All silver hours processed"
    """,
    dag=dag,
)

# Aggregate to Gold layer
process_gold = BashOperator(
    task_id='aggregate_to_gold',
    bash_command=r"""
    docker exec spark-master bash -c "
        export SPARK_HOME=/opt/spark && 
        /opt/spark/bin/spark-submit \
            --master local[2] \
            --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.1 \
            --conf spark.hadoop.fs.s3a.endpoint=${MINIO_ENDPOINT} \
            --conf spark.hadoop.fs.s3a.access.key=${MINIO_ROOT_USER} \
            --conf spark.hadoop.fs.s3a.secret.key=${MINIO_ROOT_PASSWORD} \
            --conf spark.hadoop.fs.s3a.path.style.access=true \
            --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
            --conf spark.executor.memory=4g \
            /opt/project/transformations/silver_to_gold.py {{ yesterday_ds }}
    "
    """,
    dag=dag,
)

end = DummyOperator(
    task_id='pipeline_complete',
    dag=dag,
)

# Define dependencies
start >> process_silver >> process_gold >> end
