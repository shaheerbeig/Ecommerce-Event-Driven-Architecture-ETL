# ingestion/rabbitmq_to_bronze.py
from logging import config
import yaml
import json
import time
from datetime import datetime, timezone
import pika
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, to_date, hour, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


def load_config(path):
    with open(path, "r") as f:
        config = yaml.safe_load(f)
    return config

def build_spark(minio_endpoint: str, access_key: str, secret_key: str):
    return (
        SparkSession.builder
        .appName("rabbitmq_to_bronze")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def connect_rabbit(host, port, user, password):
    creds = pika.PlainCredentials(user, password)
    params = pika.ConnectionParameters(host=host, port=port, credentials=creds, heartbeat=60)
    return pika.BlockingConnection(params)

def event_schema():
    return StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),

        StructField("ingest_source", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("ip", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),

        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("cart_value", DoubleType(), True),
        StructField("cart_item_count", IntegerType(), True),
        StructField("order_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("payment_method", StringType(), True),

        # keep complex fields as string in Bronze
        StructField("items", StringType(), True),
        StructField("search_term", StringType(), True),
        StructField("results_count", IntegerType(), True),
    ])


def drain_messages(channel, queue_name: str, max_messages: int):
    bodies = []
    delivery_tags = []

    for _ in range(max_messages):
        method_frame, _, body = channel.basic_get(queue=queue_name, auto_ack=False)
        if method_frame is None:
            break
        bodies.append(body.decode("utf-8"))
        delivery_tags.append(method_frame.delivery_tag)

    return bodies, delivery_tags


def ack_messages(channel, delivery_tags):
    for tag in delivery_tags:
        channel.basic_ack(tag)

def main():
    config = load_config("config/app_config.docker.yaml")

    RABBIT_HOST = config["rabbitmq"]["host"]
    RABBIT_PORT = config["rabbitmq"]["port"]
    RABBIT_USER = config["rabbitmq"]["user"]
    RABBIT_PASS = config["rabbitmq"]["password"]
    QUEUE = config["rabbitmq"]["queue_clickstream"]
    BUCKET = config["minio"]["bucket"]
    BRONZE_PATH = f"s3a://{BUCKET}/bronze/events"
    MAX_MESSAGES_PER_BATCH = 5000
    SLEEP_SECONDS = 10

    spark = (
        SparkSession.builder
        .master(config["spark"]["master"])
        .appName("rabbitmq_to_bronze")
        .config("spark.hadoop.fs.s3a.endpoint",config["minio"]["endpoint"])
        .config("spark.hadoop.fs.s3a.access.key", config["minio"]["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", config["minio"]["secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

    # RabbitMQ connection
    conn = connect_rabbit(RABBIT_HOST, RABBIT_PORT, RABBIT_USER, RABBIT_PASS)
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE, durable=True)

    print("✅ Ingestion service started: draining RabbitMQ -> writing Bronze in MinIO")

    try:
        while True:
            msgs, tags = drain_messages(ch, QUEUE, MAX_MESSAGES_PER_BATCH)

            if not msgs:
                time.sleep(SLEEP_SECONDS)
                continue

            # Parse JSON
            rdd = spark.sparkContext.parallelize(msgs)
            df = spark.read.schema(event_schema()).json(rdd)

            # Add ingestion timestamp partitions (Bronze partitioning)
            df = df.withColumn("ingest_time", current_timestamp())
            df = df.withColumn("event_date", to_date(col("ingest_time")))
            df = df.withColumn("event_hour", hour(col("ingest_time")))

            # Write Parquet (append)
            (
                df.write.mode("append")
                .partitionBy("event_date", "event_hour")
                .parquet(BRONZE_PATH)
            )

            # ACK only after write success
            ack_messages(ch, tags)

            print(f"✅ Wrote {len(msgs)} events to {BRONZE_PATH} and ACKed them.")

            time.sleep(1)

    finally:
        try:
            conn.close()
        except Exception:
            pass
        spark.stop()


if __name__ == "__main__":
    main()
