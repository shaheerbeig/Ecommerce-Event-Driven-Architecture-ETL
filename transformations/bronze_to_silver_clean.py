import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)

def create_spark():
    spark = (
        SparkSession.builder
        .appName("bronze_to_silver_clean")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark

def main():

    if len(sys.argv) != 3:
        print("Usage: bronze_to_silver_clean.py <event_date> <event_hour>")
        sys.exit(1)

    
    target_date = sys.argv[1]   # e.g. 2026-02-18
    target_hour = sys.argv[2]  # e.g. 20

    spark = create_spark()

    bronze_path = f"s3a://datalake/bronze/events/event_date={target_date}/event_hour={target_hour}/"
    silver_path = "s3a://datalake/silver/events_clean/"

    print(f"Reading Bronze from: {bronze_path}")

    df = spark.read.parquet(bronze_path)

    # parse timestamps
    df2 = (
        df
        .withColumn("event_time_ts", F.to_timestamp("event_time"))  # parses 2026-02-18T20:39:41Z too
        .withColumn("ingest_time_ts", F.to_timestamp("ingest_time"))
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("event_type").isin("product_view","add_to_cart","purchase","search"))
    )

    # derive partition cols FROM EVENT TIME (source of truth)
    df3 = (
        df2
        .withColumn("event_date", F.date_format(F.col("event_time_ts"), "yyyy-MM-dd"))
        .withColumn("event_hour", F.format_string("%02d", F.hour(F.col("event_time_ts"))))
    )

    # enforce we only write the partition we were asked for
    df_target = df3.filter(
        (F.col("event_date") == target_date) &
        (F.col("event_hour") == target_hour)
    )

    (
        df_target
        .write
        .mode("overwrite")
        .partitionBy("event_date", "event_hour")
        .parquet(silver_path)
    )

    print("✅ Silver partition written successfully.")

    spark.stop()

if __name__ == "__main__":
    main()
