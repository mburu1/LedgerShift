"""Generate reproducible synthetic customers, orders and a fault manifest."""
import json
import random
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from faker import Faker
from ledgershift.config import settings
from ledgershift.generator.faults import inject_order_faults

def generate(data_dir: Path | None = None, customers: int | None = None, orders: int | None = None, seed: int | None = None) -> dict:
    settings.validate_ranges()
    root = data_dir or settings.data_dir
    customer_count = customers if customers is not None else settings.customers
    order_count = orders if orders is not None else settings.orders
    seed_value = seed if seed is not None else settings.seed
    if customer_count < 1 or order_count < 0:
        raise ValueError("customers must be positive and orders cannot be negative")
    fake = Faker()
    fake.seed_instance(seed_value)
    rng = random.Random(seed_value)
    now = datetime(2025, 1, 1, tzinfo=timezone.utc)
    customer_rows = [{"customer_id": i, "full_name": fake.name(), "email": f"customer{i}@example.test", "country": rng.choice(["KE", "UG", "TZ", "RW", "GB"]), "created_at": (now - timedelta(days=rng.randint(0, 900))).isoformat()} for i in range(1, customer_count + 1)]
    order_rows = [{"order_id": i, "customer_id": rng.randint(1, customer_count), "order_date": (date(2023, 1, 1) + timedelta(days=rng.randint(0, 730))).isoformat(), "status": rng.choice(["pending", "paid", "cancelled", "refunded"]), "amount": str(Decimal(rng.randint(100, 2000000)) / Decimal(100))} for i in range(1, order_count + 1)]
    faulty_orders, manifest = inject_order_faults(order_rows, settings.fault_rate, seed_value)
    snapshot = root / "source" / "snapshot"
    snapshot.mkdir(parents=True, exist_ok=True)
    for name, rows in (("customers", customer_rows), ("orders", faulty_orders)):
        with (snapshot / f"{name}.jsonl").open("w", encoding="utf-8") as stream:
            for row in rows:
                stream.write(json.dumps(row, sort_keys=True) + "\n")
    (root / "source" / "faults_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return manifest
