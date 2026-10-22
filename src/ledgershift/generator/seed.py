"""Generate reproducible synthetic customers, orders and a fault manifest."""
from __future__ import annotations

import json
import random
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from faker import Faker

from ledgershift.config import settings
from ledgershift.generator.faults import inject_order_faults


def generate(
    data_dir: Path | None = None,
    customers: int | None = None,
    orders: int | None = None,
    seed: int | None = None,
) -> dict:
    """Generate a deterministic source snapshot and its fault-injection manifest."""
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
    now = datetime(2025, 1, 1, tzinfo=UTC)
    countries = ["KE", "UG", "TZ", "RW", "GB"]
    statuses = ["pending", "paid", "cancelled", "refunded"]

    customer_rows = []
    for customer_id in range(1, customer_count + 1):
        customer_rows.append(
            {
                "customer_id": customer_id,
                "full_name": fake.name(),
                "email": f"customer{customer_id}@example.test",
                "country": rng.choice(countries),
                "created_at": (
                    now - timedelta(days=rng.randint(0, 900))
                ).isoformat(),
            }
        )

    order_rows = []
    for order_id in range(1, order_count + 1):
        order_rows.append(
            {
                "order_id": order_id,
                "customer_id": rng.randint(1, customer_count),
                "order_date": (
                    date(2023, 1, 1) + timedelta(days=rng.randint(0, 730))
                ).isoformat(),
                "status": rng.choice(statuses),
                "amount": str(
                    Decimal(rng.randint(100, 2_000_000)) / Decimal(100)
                ),
            }
        )

    faulty_orders, manifest = inject_order_faults(
        order_rows, settings.fault_rate, seed_value
    )
    snapshot = root / "source" / "snapshot"
    snapshot.mkdir(parents=True, exist_ok=True)
    for name, rows in (("customers", customer_rows), ("orders", faulty_orders)):
        with (snapshot / f"{name}.jsonl").open("w", encoding="utf-8") as stream:
            for row in rows:
                stream.write(json.dumps(row, sort_keys=True) + "\n")
    manifest_path = root / "source" / "faults_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest
