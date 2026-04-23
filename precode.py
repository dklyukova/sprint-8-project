import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    to_json,
    col,
    lit,
    struct,
    unix_timestamp,
    current_timestamp,
)
from pyspark.sql.types import StructType, StructField, StringType, LongType

KAFKA_BOOTSTRAP_SERVERS = "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091"
KAFKA_USER = "de-student"
KAFKA_PASSWORD = "ltcneltyn"

TOPIC_NAME_IN = "your_login_in"
TOPIC_NAME_OUT = "your_login_out"

POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
POSTGRES_DB = "de"
POSTGRES_USER = "jovyan"
POSTGRES_PASSWORD = "jovyan"

CHECKPOINT_PATH = "/home/jovyan/checkpoints/subscribers_feedback"

spark_jars_packages = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        "org.postgresql:postgresql:42.4.0",
    ]
)

spark = (
    SparkSession.builder
    .appName("RestaurantSubscribeStreamingService")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.jars.packages", spark_jars_packages)
    .getOrCreate()
)

incoming_message_schema = StructType([
    StructField("restaurant_id", StringType(), True),
    StructField("adv_campaign_id", StringType(), True),
    StructField("adv_campaign_content", StringType(), True),
    StructField("adv_campaign_owner", StringType(), True),
    StructField("adv_campaign_owner_contact", StringType(), True),
    StructField("adv_campaign_datetime_start", LongType(), True),
    StructField("adv_campaign_datetime_end", LongType(), True),
    StructField("datetime_created", LongType(), True),
])

restaurant_read_stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
    .option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{KAFKA_USER}" password="{KAFKA_PASSWORD}";'
    )
    .option("subscribe", TOPIC_NAME_IN)
    .option("startingOffsets", "latest")
    .load()
)

current_timestamp_utc = unix_timestamp(current_timestamp())

filtered_read_stream_df = (
    restaurant_read_stream_df
    .select(from_json(col("value").cast("string"), incoming_message_schema).alias("data"))
    .select("data.*")
    .filter(
        current_timestamp_utc.between(
            col("adv_campaign_datetime_start"),
            col("adv_campaign_datetime_end"),
        )
    )
)

subscribers_restaurant_df = (
    spark.read
    .format("jdbc")
    .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    .option("driver", "org.postgresql.Driver")
    .option("dbtable", "public.subscribers_restaurants")
    .option("user", POSTGRES_USER)
    .option("password", POSTGRES_PASSWORD)
    .load()
)

result_df = (
    filtered_read_stream_df.alias("c")
    .join(
        subscribers_restaurant_df.alias("s"),
        on=col("c.restaurant_id") == col("s.restaurant_id"),
        how="inner",
    )
    .select(
        col("c.restaurant_id").alias("restaurant_id"),
        col("c.adv_campaign_id").alias("adv_campaign_id"),
        col("c.adv_campaign_content").alias("adv_campaign_content"),
        col("c.adv_campaign_owner").alias("adv_campaign_owner"),
        col("c.adv_campaign_owner_contact").alias("adv_campaign_owner_contact"),
        col("c.adv_campaign_datetime_start").alias("adv_campaign_datetime_start"),
        col("c.adv_campaign_datetime_end").alias("adv_campaign_datetime_end"),
        col("c.datetime_created").alias("datetime_created"),
        col("s.client_id").alias("client_id"),
    )
    .dropDuplicates(["restaurant_id", "adv_campaign_id", "client_id"])
)

def foreach_batch_function(df, epoch_id):
    df.persist()

    try:
        postgres_df = (
            df
            .withColumn("trigger_datetime_created", current_timestamp().cast("long").cast("int"))
            .withColumn("feedback", lit(None).cast("string"))
            .select(
                "restaurant_id",
                "adv_campaign_id",
                "adv_campaign_content",
                "adv_campaign_owner",
                "adv_campaign_owner_contact",
                "adv_campaign_datetime_start",
                "adv_campaign_datetime_end",
                "datetime_created",
                "client_id",
                "trigger_datetime_created",
                "feedback",
            )
        )
        (
            postgres_df.write
            .format("jdbc")
            .mode("append")
            .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
            .option("driver", "org.postgresql.Driver")
            .option("dbtable", "public.subscribers_feedback")
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASSWORD)
            .save()
        )
        kafka_df = (
            df
            .withColumn("trigger_datetime_created", current_timestamp().cast("long").cast("int"))
            .select(
                to_json(
                    struct(
                        col("restaurant_id"),
                        col("adv_campaign_id"),
                        col("adv_campaign_content"),
                        col("adv_campaign_owner"),
                        col("adv_campaign_owner_contact"),
                        col("adv_campaign_datetime_start"),
                        col("adv_campaign_datetime_end"),
                        col("client_id"),
                        col("datetime_created"),
                        col("trigger_datetime_created"),
                    )
                ).alias("value")
            )
        )
        (
            kafka_df.write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
            .option(
                "kafka.sasl.jaas.config",
                f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{KAFKA_USER}" password="{KAFKA_PASSWORD}";'
            )
            .option("topic", TOPIC_NAME_OUT)
            .save()
        )

    finally:
        df.unpersist()

(
    result_df.writeStream
    .foreachBatch(foreach_batch_function)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start()
    .awaitTermination()
)
