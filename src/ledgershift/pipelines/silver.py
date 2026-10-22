"""Silver quality gates: invalid rows are quarantined, accepted rows deduplicated."""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number, to_date, when
from pyspark.sql.window import Window


def clean_orders(frame: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Validate order records without silently accepting malformed values."""
    required = ["order_id", "customer_id", "order_date", "amount"]
    missing = [name for name in required if name not in frame.columns]
    if missing:
        raise ValueError("orders missing required columns: " + ", ".join(missing))
    parsed_amount = col("amount").cast("decimal(18,2)")
    parsed_date = to_date(col("order_date").cast("string"))
    invalid = (
        col("order_id").isNull() | col("customer_id").isNull()
        | col("order_date").isNull() | parsed_date.isNull()
        | col("amount").isNull() | parsed_amount.isNull() | (parsed_amount < 0)
    )
    rejected = frame.filter(invalid).withColumn(
        "_reject_reason",
        when(col("order_id").isNull(), "missing_order_id")
        .when(col("customer_id").isNull(), "missing_customer_id")
        .when(col("order_date").isNull() | parsed_date.isNull(), "invalid_order_date")
        .when(col("amount").isNull() | parsed_amount.isNull(), "invalid_amount")
        .when(parsed_amount < 0, "negative_amount")
        .otherwise("invalid_record"),
    )
    accepted = frame.filter(~invalid)
    if "lsn" in frame.columns:
        order_column = col("lsn").desc_nulls_last()
    elif "_batch_id" in frame.columns:
        order_column = col("_batch_id").desc_nulls_last()
    else:
        order_column = col("order_id").desc()
    latest = Window.partitionBy("order_id").orderBy(order_column)
    accepted = accepted.withColumn("_row_number", row_number().over(latest)).filter(
        col("_row_number") == 1
    ).drop("_row_number")
    return accepted, rejected
