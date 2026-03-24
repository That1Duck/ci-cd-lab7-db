import dlt
from pyspark.sql.functions import * 
from pyspark.sql.types import *

def get_kafka_options():
    conn_str = dbutils.secrets.get(scope="mazhara-scope", key="eh-conn-str-mazhara")
    topic_name = "eh-mazhara-dbr_dev"
    eh_namespace = "evhgigdata"

    return {
        "kafka.bootstrap.servers": f"{eh_namespace}.servicebus.windows.net:9093",
        "subscribe": topic_name,
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.jaas.config": f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{conn_str}\";",
        "kafka.request.timeout.ms" : 10000,
        "kafka.session.timeout.ms" : 20000,
        "maxOffsetsPerTrigger"     : 1000,
        "startingOffsets": "latest",
        "failOnDataLoss": "false"
    }


@dlt.table(
    name = "orders_bronze",
    comment = "Raw stream from Event Hub", 
    table_properties = {
        "quality": "bronze"
    }
)
def orders_bronze():
    return (
        spark.readStream
        .format("kafka")
        .options(**get_kafka_options())
        .load()
        .withColumn("timestamp_ingestion", current_timestamp())
        .withColumn("source_system", lit("azure_event_hub"))
        .withColumn("offset_val", col("offset"))
        .select(
            col("value").cast("string").alias("raw_payload"),
            "timestamp_ingestion",
            "source_system",
            "offset_val"
        )
    )