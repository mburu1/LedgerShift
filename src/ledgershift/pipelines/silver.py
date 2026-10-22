"""Silver quality gates: invalid rows are quarantined, accepted rows deduplicated."""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number, when
from pyspark.sql.window import Window

def clean_orders(frame: DataFrame) -> tuple[DataFrame, DataFrame]:
    required = ["order_id", "customer_id", "order_date", "amount"]
    missing = [name for name in required if name not in frame.columns]
    if missing:
        raise ValueError(f"orders missing required columns: {', '.join(missing)}")
    invalid = (col("order_id").isNull() | col("customer_id").isNull() |
        col("order_date").isNull() | col("amount").isNull() |
        (col("amount").cast("decimal(18,2)") < 0))
    rejected = frame.filter(invalid).withColumn("_reject_reason", when(col("order_id").isNull(), "missing_order_id")
        .when(col("customer_id").isNull(), "missing_customer_id")
        .when(col("order_date").isNull(), "missing_order_date")
        .when(col("amount").isNull(), "invalid_amount")
        .otherwise("negative_amount"))
    accepted = frame.filter(~invalid)
    order_column = col("_batch_id").desc_nulls_last() if "_batch_id" in frame.columns else col("order_id").desc()
    latest = Window.partitionBy("order_id").orderBy(order_column)
    accepted = accepted.withColumn("_row_number", row_number().over(latest)).filter(col("_row_number") == 1)
    return accepted.drop("_row_number"), rejected
