# ADR 0002: Medallion data layers

- Status: Accepted
- Context: raw ingestion, data quality remediation, and analytics have different audit and access needs.
- Decision: separate bronze raw data, silver validated/deduplicated data with quarantine, and gold business aggregates.
- Consequences: data lineage and rejected records remain inspectable; layer contracts require tests.
