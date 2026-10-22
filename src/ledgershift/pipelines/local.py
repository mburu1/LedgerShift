"""Execute the local bronze -> silver -> gold pipeline against generated orders."""
from ledgershift.config import settings
from ledgershift.pipelines.gold import daily_revenue
from ledgershift.pipelines.silver import clean_orders
from ledgershift.spark import get_spark

def main() -> None:
    source = settings.data_dir / "source" / "snapshot" / "orders.jsonl"
    if not source.exists():
        raise SystemExit("Source data missing; run make generate first.")
    spark = get_spark()
    try:
        raw = spark.read.json(str(source))
        accepted, rejected = clean_orders(raw)
        paths = {"bronze": settings.data_dir / "bronze" / "orders",
                 "silver": settings.data_dir / "silver" / "orders",
                 "quarantine": settings.data_dir / "silver" / "quarantine_orders",
                 "gold": settings.data_dir / "gold" / "mart_daily_revenue"}
        raw.write.format("delta").mode("overwrite").save(str(paths["bronze"]))
        accepted.write.format("delta").mode("overwrite").save(str(paths["silver"]))
        rejected.write.format("delta").mode("overwrite").save(str(paths["quarantine"]))
        daily_revenue(accepted).write.format("delta").mode("overwrite").save(str(paths["gold"]))
        print(f"Pipeline complete: accepted={accepted.count()}, quarantined={rejected.count()}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
