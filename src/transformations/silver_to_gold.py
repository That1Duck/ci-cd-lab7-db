import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="dim_orders_history_gold", 
    comment="Complete Order Change History (SCD 2)"
)
def dim_orders_history():
    return (
        dlt.read("orders_silver_scd") 
        .select(
            "order_id", 
            "status", 
            "city", 
            "category",
            "__start_at", 
            "__end_at"
        )
    )

@dlt.table(
    name="fact_sales_gold", 
)
def fact_sales():
    return (
        dlt.read("orders_silver_clean") # Читаем чистое имя
        .filter(col("event_type") == "ORDER")
        .withColumn("user_email_masked", regexp_replace(col("user_email"), r"(?<=.).(?=.*@)", "*"))
        .select("order_id", "amount", "user_email_masked", "event_timestamp")
    )

@dlt.table(
    name="report_sales_by_city_gold" 
)
def city_sales_report():
    facts = dlt.read("fact_sales_gold")
    dims = dlt.read("dim_orders_history_gold").filter(col("__is_current") == True)
    
    return (
        facts.join(dims, "order_id", "inner")
        .groupBy("city", "category")
        .agg(
            sum("amount").alias("total_revenue"),
            count("order_id").alias("total_orders")
        )
    )