from ledgershift.generator.faults import FAULTS, inject_order_faults


def test_fault_injection_is_reproducible_and_does_not_mutate_input():
    rows = [{"order_id": i, "customer_id": 1, "amount": "10.00", "order_date": "2025-01-01"}
            for i in range(1, 21)]
    original = [dict(row) for row in rows]
    first, first_manifest = inject_order_faults(rows, 0.8, 42)
    second, second_manifest = inject_order_faults(rows, 0.8, 42)
    assert first == second
    assert first_manifest == second_manifest
    assert rows == original
    assert first_manifest["injected_rows"] == 16
    assert sum(first_manifest[name] for name in FAULTS) == 16
    assert first_manifest["output_rows"] == len(first)


def test_duplicate_fault_is_accounted_for_in_output_cardinality():
    rows = [{"order_id": i} for i in range(100)]
    output, manifest = inject_order_faults(rows, 1, 7)
    assert manifest["output_rows"] == len(rows) + manifest["duplicate_rows"]
    assert len(output) == manifest["output_rows"]


def test_fault_rate_must_be_between_zero_and_one():
    import pytest
    with pytest.raises(ValueError):
        inject_order_faults([], 1.1, 1)
