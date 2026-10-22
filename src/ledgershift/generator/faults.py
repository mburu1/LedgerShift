"""Deterministic fault injection with a machine-readable manifest."""
import random
from typing import Any

FAULTS = ("duplicate_rows", "late_arriving", "null_required", "negative_amount", "orphan_fk")

def inject_order_faults(rows: list[dict[str, Any]], rate: float, seed: int) -> tuple[list[dict[str, Any]], dict]:
    if not 0 <= rate <= 1:
        raise ValueError("rate must be between 0 and 1")
    rng = random.Random(seed)
    output = [dict(row) for row in rows]
    manifest = {name: 0 for name in FAULTS}
    indexes = list(range(len(output)))
    rng.shuffle(indexes)
    for index in indexes[:int(len(output) * rate)]:
        fault = FAULTS[index % len(FAULTS)]
        row = output[index]
        if fault == "duplicate_rows":
            output.append(dict(row))
        elif fault == "late_arriving":
            row["_fault"] = fault
            row["order_date"] = "2000-01-01"
        elif fault == "null_required":
            row["_fault"] = fault
            row["customer_id"] = None
        elif fault == "negative_amount":
            row["_fault"] = fault
            row["amount"] = "-1.00"
        else:
            row["_fault"] = fault
            row["customer_id"] = 9999999999
        manifest[fault] += 1
    manifest.update(input_rows=len(rows), output_rows=len(output), seed=seed)
    return output, manifest
