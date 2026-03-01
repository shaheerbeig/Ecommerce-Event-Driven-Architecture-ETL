import sys
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, to_timestamp, date_format, hour, window, count, sum as spark_sum,
    avg, min, max, approx_count_distinct, collect_list
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


def create_spark():
    """Create Spark session with S3A and PostgreSQL support."""
    spark = (
        SparkSession.builder
        .appName("silver_to_gold")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "minio"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "minio12345"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark


def load_silver_events(spark, event_date: str, event_hour: str = None):
    if event_hour:
        path = f"s3a://datalake/silver/events_clean/event_date={event_date}/event_hour={event_hour}/"
        print(f"Loading Silver: {path}")
    else:
        path = f"s3a://datalake/silver/events_clean/event_date={event_date}/"
        print(f"Loading Silver: {path}")
    
    return spark.read.parquet(path)

# here we are extracting user deatils from databse
def load_users_dimension(spark):
    
    users_df = (
        spark.read.format("jdbc")
        .option("url", f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'postgres')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'data_storage_db')}")
        .option("dbtable", "app_users")
        .option("user", os.getenv("POSTGRES_USER", "shaheer"))
        .option("password", os.getenv("POSTGRES_PASSWORD", "shaheer"))
        .load()
    )
    
    users_dim = (
        users_df
        .select(col("user_id"),col("country").alias("user_country"),col("city").alias("user_city"),
            col("signup_channel").alias("user_signup_channel"),
            col("device_preference").alias("user_device_preference"),
            col("created_at").alias("user_created_at"),
            col("is_active").alias("user_is_active")
        )
    ) 
    return users_dim

# here we are extracting product deatils from databse
def load_products_dimension(spark):
    
    products_df = (
        spark.read.format("jdbc")
        .option("url", f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'postgres')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'data_storage_db')}")
        .option("dbtable", "app_products")
        .option("user", os.getenv("POSTGRES_USER", "shaheer"))
        .option("password", os.getenv("POSTGRES_PASSWORD", "shaheer"))
        .load()
    )
    
    products_dim = (
        products_df
        .select(col("product_id"),col("category").alias("product_category"),
            col("brand").alias("product_brand"),col("price").alias("product_price"),
            col("currency").alias("product_currency"),col("inventory_status").alias("product_inventory_status"),
            col("is_active").alias("product_is_active")
        )
    )
    
    return products_dim

# making a join with user and product with the event data
def enrich_events_with_dimensions(events_df, users_dim, products_dim): 
    enriched = (
        events_df.join(users_dim, on="user_id", how="left")
    )    
    enriched = (
        enriched
        .join(products_dim, on="product_id", how="left")
    )
    
    return enriched


def create_facts_events(enriched_df):
    # Add event_date and event_hour from event_time_ts
    enriched_df = (
        enriched_df
        .withColumn("event_date", F.date_format(F.col("event_time_ts"), "yyyy-MM-dd"))
        .withColumn("event_hour", F.hour(F.col("event_time_ts")))
    )
    
    facts_events = (
        enriched_df
        .select(
            col("event_id"),col("event_type"),col("event_time_ts").alias("event_timestamp"),
            col("ingest_time_ts").alias("ingest_timestamp"),
            F.date_format(F.col("event_time_ts"), "yyyy-MM-dd").alias("event_date"),
            F.hour(F.col("event_time_ts")).alias("event_hour"),
            
            # User dimensions
            col("user_id"),col("user_country"),col("user_city"),col("user_signup_channel"),
            col("user_device_preference"),col("user_created_at"),col("user_is_active"),
            
            # Product dimensions
            col("product_id"),col("product_category"),col("product_brand"),col("product_price"),
            col("product_currency"),col("product_inventory_status"),
            
            # Event context
            col("session_id"),col("device_type"),col("ip"),col("country"),col("city"),
            
            # Event-specific metrics
            col("quantity"),col("cart_value"),col("cart_item_count"),col("order_id"),
            col("amount").cast(DoubleType()),col("currency"),col("payment_method"),col("search_term"),
            col("results_count"),            
        )
    )
    
    return facts_events


def create_facts_purchases(enriched_df):
    # Filter to purchase events only
    purchases = enriched_df.filter(col("event_type") == "purchase")
    
    facts_purchases = (
        purchases
        .select(
            col("event_id").alias("purchase_event_id"),
            F.date_format(F.col("event_time_ts"), "yyyy-MM-dd").alias("event_date"),
            F.hour(F.col("event_time_ts")).alias("event_hour"),
            col("event_time_ts").alias("purchase_timestamp"),
            col("order_id"),col("user_id"),col("user_signup_channel"),col("user_country"),
            col("product_id"),col("product_category"),col("product_brand"),
            col("amount").cast(DoubleType()).alias("purchase_amount"),col("currency"),
            col("payment_method"),col("session_id"),col("device_type"),col("user_device_preference"),
            F.date_format(F.col("event_time_ts"), "yyyy-MM-dd").alias("purchase_date"),
            F.hour(F.col("event_time_ts")).alias("purchase_hour")
        )
    )
    
    return facts_purchases


def create_facts_user_sessions(enriched_df):
    
    session_metrics = (
        enriched_df
        .groupBy(
            F.date_format(F.col("event_time_ts"), "yyyy-MM-dd").alias("event_date"),
            F.hour(F.col("event_time_ts")).alias("event_hour"),
            col("session_id"),col("user_id"),
            col("user_signup_channel"),col("user_country"),col("device_type")
        )
        .agg(
            count("event_id").alias("event_count"),
            approx_count_distinct("product_id").alias("unique_products_viewed"),
            F.sum(F.when(col("event_type") == "product_view", 1).otherwise(0)).alias("product_views"),
            F.sum(F.when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
            F.sum(F.when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
            F.max(col("amount")).alias("max_purchase_amount"),
            F.sum(col("amount")).alias("total_purchase_amount"),
            F.min(col("event_time_ts")).alias("session_start_time"),
            F.max(col("event_time_ts")).alias("session_end_time"),
        )
    )
    
    return session_metrics


def write_to_gold(facts_events, facts_purchases, facts_user_sessions):

    (
        facts_events
        .write
        .mode("overwrite")
        .partitionBy("event_date", "event_hour")
        .parquet("s3a://datalake/gold/facts_events/")
    )
    print("✅ Wrote fact_events")
    
    (
        facts_purchases
        .write
        .mode("overwrite")
        .partitionBy("event_date", "event_hour")
        .parquet("s3a://datalake/gold/facts_purchases/")
    )
    print("✅ Wrote fact_purchases")
    
    # Write fact_user_sessions
    (
        facts_user_sessions
        .write
        .mode("overwrite")
        .partitionBy("event_date", "event_hour")
        .parquet("s3a://datalake/gold/facts_user_sessions/")
    )
    print("✅ Wrote fact_user_sessions")


def main():
    if len(sys.argv) < 2:
        print("Usage: silver_to_gold.py <event_date> [event_hour]")
        print("  event_date: YYYY-MM-DD (e.g., 2026-02-20)")
        print("  event_hour: HH (optional, e.g., 15 for hour 15)")
        sys.exit(1)
    
    event_date = sys.argv[1]
    event_hour = sys.argv[2] if len(sys.argv) > 2 else None
    
    spark = create_spark()
    
    try:
        # Load data
        events_df = load_silver_events(spark, event_date, event_hour)
        users_dim = load_users_dimension(spark)
        products_dim = load_products_dimension(spark)
        
        # Parse event timestamps (already parsed in Silver, but ensure timestamp type)
        if "event_time_ts" not in events_df.columns:
            events_df = events_df.withColumn("event_time_ts", to_timestamp("event_time"))
        if "ingest_time_ts" not in events_df.columns:
            events_df = events_df.withColumn("ingest_time_ts", to_timestamp("ingest_time"))
        
        # Enrich
        enriched = enrich_events_with_dimensions(events_df, users_dim, products_dim)
        
        # Create fact tables
        facts_events = create_facts_events(enriched)
        facts_purchases = create_facts_purchases(enriched)
        facts_user_sessions = create_facts_user_sessions(enriched)
        
        # Write to Gold
        write_to_gold(facts_events, facts_purchases, facts_user_sessions)
        
    except Exception as e:
        print(f"Error: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
