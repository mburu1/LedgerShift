from ledgershift.validation.checks import compare_row_counts, summarize

def test_row_count_passes_when_counts_match():
    assert compare_row_counts("orders", 4, 4).status == "PASS"

def test_row_count_fails_when_counts_differ():
    assert compare_row_counts("orders", 4, 3).status == "FAIL"

def test_empty_report_fails_closed():
    assert summarize([])["verdict"] == "FAIL"
