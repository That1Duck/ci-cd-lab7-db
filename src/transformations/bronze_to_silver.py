import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

json_schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("category", StringType(), True),
    StructField("city", StringType(), True),
    StructField("user_email", StringType(), True),
    StructField("status", StringType(), True),
    StructField("timestamp", DoubleType(), True)
])

@dlt.table(
    name="mazhara_silver.orders_silver_clean", 
    comment="Cleaned data from Bronze with a total amount check"
)
@dlt.expect_or_drop("valid_amount", "amount < 100000")
@dlt.expect_or_fail("has_order_id", "order_id IS NOT NULL")
def orders_silver_clean():
    return (
        dlt.read_stream("orders_bronze")
        .withColumn("data", from_json(col("raw_payload"), json_schema))
        .select("data.*", "timestamp_ingestion")
        .withColumn("event_timestamp", from_unixtime(col("timestamp")).cast("timestamp"))
        .drop("timestamp")
    )

dlt.table(
    name="mazhara_silver.orders_silver_scd",
    comment="History of orders (SCD Type 2)",
    table_properties={
        "quality": "silver"
    }
)
dlt.apply_changes(
    target = "mazhara_silver.orders_silver_scd",
    source = "mazhara_silver.orders_silver_clean",
    keys = ["order_id"],
    sequence_by = col("event_timestamp"),
    stored_as_scd_type = "2",
    track_history_column_list = ["status", "amount", "city"]
)