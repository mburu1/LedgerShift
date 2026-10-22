from ledgershift.generator.faults import inject_order_faults

def test_fault_injection_is_reproducible():
    rows = [{"order_id": i, "customer_id": 1, "amount": "1.00", "order_date": "2024-01-01"} for i in range(1, 101)]
    first, manifest_a = inject_order_faults(rows, 0.2, 42)
    second, manifest_b = inject_order_faults(rows, 0.2, 42)
    assert first == second
    assert manifest_a == manifest_b
    assert manifest_a["output_rows"] == len(first)

def test_fault_injection_does_not_mutate_input():
    rows = [{"order_id": 1, "customer_id": 1, "amount": "1.00", "order_date": "2024-01-01"}]
    original = [dict(row) for row in rows]
    inject_order_faults(rows, 1, 42)
    assert rows == original
