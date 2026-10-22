"""Shared Spark factory for local Delta development and managed Databricks runs."""
import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from ledgershift.config import settings

def get_spark(app: str = "ledgershift") -> SparkSession:
    if os.getenv("DATABRICKS_RUNTIME_VERSION"):
        return SparkSession.builder.getOrCreate()
    builder = (SparkSession.builder.appName(app).master(settings.spark_master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC"))
    return configure_spark_with_delta_pip(builder).getOrCreate()
