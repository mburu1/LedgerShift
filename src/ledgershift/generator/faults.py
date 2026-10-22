"""Deterministic fault injection with a machine-readable manifest."""
from __future__ import annotations

import random
from typing import Any

FAULTS = (
    "duplicate_rows", "late_arriving", "out_of_order", "null_required",
    "bad_type", "schema_drift", "orphan_fk", "negative_amount",
)


def inject_order_faults(
    rows: list[dict[str, Any]], rate: float, seed: int
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Inject reproducible faults and record exact counts without mutating input."""
    if not 0 <= rate <= 1:
        raise ValueError("rate must be between 0 and 1")
    rng = random.Random(seed)
    output = [dict(row) for row in rows]
    manifest: dict[str, Any] = {name: 0 for name in FAULTS}
    selected = list(range(len(output)))
    rng.shuffle(selected)
    selected = selected[:int(len(output) * rate)]
    fault_order = list(FAULTS)
    rng.shuffle(fault_order)
    for position, index in enumerate(selected):
        fault = fault_order[position % len(fault_order)]
        row = output[index]
        if fault == "duplicate_rows":
            output.append(dict(row))
        elif fault == "late_arriving":
            row["_fault"] = fault
            row["order_date"] = "2000-01-01"
        elif fault == "out_of_order":
            row["_fault"] = fault
            row["lsn"] = -1
        elif fault == "null_required":
            row["_fault"] = fault
            row["customer_id"] = None
        elif fault == "bad_type":
            row["_fault"] = fault
            row["amount"] = "not-a-decimal"
        elif fault == "schema_drift":
            row["_fault"] = fault
            row["unexpected_source_column"] = "drift"
        elif fault == "orphan_fk":
            row["_fault"] = fault
            row["customer_id"] = 9999999999
        elif fault == "negative_amount":
            row["_fault"] = fault
            row["amount"] = "-1.00"
        manifest[fault] += 1
    manifest.update(input_rows=len(rows), output_rows=len(output), injected_rows=len(selected), seed=seed, version=1)
    return output, manifest
