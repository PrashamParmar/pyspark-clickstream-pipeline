from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


RAW_SCHEMA = T.StructType(
    [
        T.StructField("user_id", T.StringType(), True),
        T.StructField("session_id", T.StringType(), True),
        T.StructField("timestamp", T.StringType(), True),
        T.StructField("action", T.StringType(), True),
        T.StructField("product_id", T.StringType(), True),
        T.StructField("category", T.StringType(), True),
        T.StructField("price", T.DoubleType(), True),
        T.StructField("device_type", T.StringType(), True),
    ]
)


VALID_ACTIONS = ["view", "add_to_cart", "purchase"]


@dataclass(frozen=True)
class MongoConfig:
    uri: str
    database: str
    user_session_metrics_collection: str
    trending_products_collection: str
    conversion_funnel_collection: str
    write_batch_size: int
    write_concern_w: str


def build_spark(app_name: str, mongo: MongoConfig, extra_jvm_opens: bool) -> SparkSession:
    """Create SparkSession with Mongo connector settings.

    For local Windows testing with Java 17, set driver/executor extraJavaOptions
    to avoid InaccessibleObjectException.
    """

    # These are safe to include even if you are running with Docker Java 11.
    # Bitnami spark images already handle Java module access.
    add_opens_args: List[str] = [
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
    ]

    jvm_extra = " ".join(add_opens_args) if extra_jvm_opens else ""

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.mongodb.write.connection.uri", mongo.uri)
        .config("spark.mongodb.database", mongo.database)
    )

    if jvm_extra:
        builder = builder.config("spark.driver.extraJavaOptions", jvm_extra)
        builder = builder.config("spark.executor.extraJavaOptions", jvm_extra)

    return builder.getOrCreate()


def read_raw_clickstream(spark: SparkSession, input_path: str) -> DataFrame:
    """Read raw clickstream JSON files."""
    return (
        spark.read.option("mode", "PERMISSIVE")
        .schema(RAW_SCHEMA)
        .json(input_path)
    )


def cleanse_events(df: DataFrame) -> DataFrame:
    """Cleansing: null handling, timestamp parsing, invalid row filtering."""

    # Parse timestamp; allow ISO strings.
    # to_timestamp covers ISO-8601 forms.
    df2 = (
        df.withColumn("timestamp_ts", F.to_timestamp(F.col("timestamp")))
        .withColumn("price_clean", F.col("price").cast(T.DoubleType()))
        .withColumn("action_clean", F.lower(F.trim(F.col("action"))))
        .withColumn("user_id_clean", F.trim(F.col("user_id")))
        .withColumn("session_id_clean", F.trim(F.col("session_id")))
        .withColumn("product_id_clean", F.trim(F.col("product_id")))
        .withColumn("category_clean", F.trim(F.col("category")))
        .withColumn("device_type_clean", F.lower(F.trim(F.col("device_type"))))
    )

    valid_device = ["mobile", "desktop", "tablet"]

    cleaned = df2.filter(
        F.col("user_id_clean").isNotNull()
        & F.col("session_id_clean").isNotNull()
        & F.col("timestamp_ts").isNotNull()
        & F.col("action_clean").isin(VALID_ACTIONS)
        & F.col("product_id_clean").isNotNull()
        & F.col("category_clean").isNotNull()
        & (F.col("price_clean").isNotNull())
        & (F.col("price_clean") >= F.lit(0.01))
        & F.col("device_type_clean").isin(valid_device)
    )

    return cleaned.select(
        F.col("user_id_clean").alias("user_id"),
        F.col("session_id_clean").alias("session_id"),
        F.col("timestamp_ts").alias("event_ts"),
        F.col("action_clean").alias("action"),
        F.col("product_id_clean").alias("product_id"),
        F.col("category_clean").alias("category"),
        F.col("price_clean").alias("price"),
        F.col("device_type_clean").alias("device_type"),
    )


def compute_session_analytics(events: DataFrame) -> DataFrame:
    """User Session Analytics: avg session duration, bounce rate, activity counts per user."""

    session_w = Window.partitionBy("user_id", "session_id")

    # Session duration as (max - min). bounce: session has only views (no add_to_cart/purchase)
    per_session = (
        events.groupBy("user_id", "session_id")
        .agg(
            F.min("event_ts").alias("session_start_ts"),
            F.max("event_ts").alias("session_end_ts"),
            F.count(F.lit(1)).alias("event_count"),
            F.sum(F.when(F.col("action") == "view", 1).otherwise(0)).alias("view_count"),
            F.sum(F.when(F.col("action").isin(["add_to_cart", "purchase"]), 1).otherwise(0)).alias(
                "meaningful_action_count"
            ),
        )
        .withColumn("session_duration_seconds", F.col("session_end_ts").cast("long") - F.col("session_start_ts").cast("long"))
        .withColumn("is_bounce", F.when(F.col("meaningful_action_count") == 0, F.lit(1)).otherwise(F.lit(0)))
    )

    per_user = (
        per_session.groupBy("user_id")
        .agg(
            F.avg("session_duration_seconds").alias("avg_session_duration_seconds"),
            (F.sum("is_bounce").cast("double") / F.count(F.lit(1))).alias("bounce_rate"),
            F.sum("event_count").alias("total_activity_count"),
            F.avg("event_count").alias("avg_activity_count_per_session"),
            F.count(F.lit(1)).alias("sessions_count"),
        )
    )

    return per_user


def compute_trending_products(events: DataFrame, top_n: int = 5) -> DataFrame:
    """Trending Products: top N selling products per category using window functions."""

    # Use purchase events to count "selling".
    purchase_counts = (
        events.filter(F.col("action") == "purchase")
        .groupBy("category", "product_id")
        .agg(F.count(F.lit(1)).alias("purchase_count"))
    )

    rank_w = Window.partitionBy("category").orderBy(F.desc("purchase_count"), F.asc("product_id"))

    ranked = purchase_counts.withColumn("rn", F.row_number().over(rank_w))

    return (
        ranked.filter(F.col("rn") <= F.lit(top_n))
        .select(
            "category",
            "product_id",
            "purchase_count",
            "rn",
        )
        .orderBy("category", "rn")
    )


def compute_conversion_funnel(events: DataFrame) -> DataFrame:
    """Conversion Funnel: view -> add_to_cart -> purchase.

    Approach:
    - For each user_id, session_id, category, device_type, compute boolean flags.
    - Then aggregate counts of funnels.
    """

    # funnel at session grain
    per_session = (
        events.withColumn("event_date", F.to_date("event_ts"))
        .groupBy("event_date", "category", "device_type", "user_id", "session_id")
        .agg(
            F.max(F.when(F.col("action") == "view", 1).otherwise(0)).alias("has_view"),
            F.max(F.when(F.col("action") == "add_to_cart", 1).otherwise(0)).alias("has_add_to_cart"),
            F.max(F.when(F.col("action") == "purchase", 1).otherwise(0)).alias("has_purchase"),
        )
    )

    funnel = (
        per_session.groupBy("event_date", "category", "device_type")
        .agg(
            F.sum("has_view").alias("sessions_with_view"),
            F.sum("has_add_to_cart").alias("sessions_with_add_to_cart"),
            F.sum("has_purchase").alias("sessions_with_purchase"),
            F.count(F.lit(1)).alias("sessions_total"),
        )
        .withColumn(
            "view_to_add_to_cart_rate",
            F.when(F.col("sessions_with_view") > 0, F.col("sessions_with_add_to_cart") / F.col("sessions_with_view")).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "add_to_cart_to_purchase_rate",
            F.when(F.col("sessions_with_add_to_cart") > 0, F.col("sessions_with_purchase") / F.col("sessions_with_add_to_cart")).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "view_to_purchase_rate",
            F.when(F.col("sessions_with_view") > 0, F.col("sessions_with_purchase") / F.col("sessions_with_view")).otherwise(F.lit(0.0)),
        )
    )

    return funnel


def persist_if_reused(df: DataFrame) -> DataFrame:
    """Persist events as they are reused by multiple aggregations."""
    return df.persist(StorageLevel.MEMORY_AND_DISK)


def write_to_mongo(df: DataFrame, mongo: MongoConfig, collection: str) -> None:
    """Write DataFrame to MongoDB via Mongo Spark connector."""

    writer = (
        df.write.format("mongodb")
        .mode("append")
        .option("database", mongo.database)
        .option("collection", collection)
        .option("uri", mongo.uri)
        .option("batchSize", str(mongo.write_batch_size))
        .option("writeConcern.w", mongo.write_concern_w)
    )

    writer.save()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clickstream analytics pipeline (PySpark -> MongoDB).")
    parser.add_argument("--input", type=str, default="data/raw/clickstream/*.json")
    parser.add_argument("--app-name", type=str, default="clickstream-analytics")

    parser.add_argument("--mongo-uri", type=str, default="mongodb://mongo:27017")
    parser.add_argument("--mongo-database", type=str, default="analytics_db")
    parser.add_argument("--mongo-user-session-collection", type=str, default="user_session_metrics")
    parser.add_argument("--mongo-trending-collection", type=str, default="trending_products")
    parser.add_argument("--mongo-funnel-collection", type=str, default="conversion_funnel_metrics")

    parser.add_argument("--write-batch-size", type=int, default=500)
    parser.add_argument("--write-concern-w", type=str, default="1")
    parser.add_argument("--top-n", type=int, default=5)

    # If you run locally with Java 17, set this to true.
    parser.add_argument("--enable-java17-add-opens", action="store_true")

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    mongo = MongoConfig(
        uri=args.mongo_uri,
        database=args.mongo_database,
        user_session_metrics_collection=args.mongo_user_session_collection,
        trending_products_collection=args.mongo_trending_collection,
        conversion_funnel_collection=args.mongo_funnel_collection,
        write_batch_size=args.write_batch_size,
        write_concern_w=args.write_concern_w,
    )

    spark = build_spark(app_name=args.app_name, mongo=mongo, extra_jvm_opens=args.enable_java17_add_opens)

    try:
        raw = read_raw_clickstream(spark, args.input)
        cleansed = cleanse_events(raw)
        events = persist_if_reused(cleansed)


        # Session analytics
        session_metrics = compute_session_analytics(events)

        # Trending products
        trending = compute_trending_products(events, top_n=args.top_n)

        # Conversion funnel
        funnel = compute_conversion_funnel(events)

        # Write outputs
        write_to_mongo(session_metrics, mongo, mongo.user_session_metrics_collection)
        write_to_mongo(trending, mongo, mongo.trending_products_collection)
        write_to_mongo(funnel, mongo, mongo.conversion_funnel_collection)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

