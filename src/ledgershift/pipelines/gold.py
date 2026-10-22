"""Gold analytical marts."""
from pyspark.sql import DataFrame
from pyspark.sql.functions import count, sum as spark_sum, to_date

def daily_revenue(orders: DataFrame) -> DataFrame:
    return (orders.filter("status = 'paid'").withColumn("revenue_date", to_date("order_date"))
        .groupBy("revenue_date")
        .agg(count("*").alias("paid_order_count"), spark_sum("amount").alias("gross_revenue"))
        .orderBy("revenue_date"))
