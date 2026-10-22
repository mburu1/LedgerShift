# ADR 0004: Independent migration validation

- Status: Accepted
- Context: a successful pipeline run does not prove source and target equivalence.
- Decision: validate row counts, aggregates, key uniqueness, null profiles, referential integrity, checksums, and deterministic samples; produce explicit PASS/FAIL reports.
- Consequences: validation failure must gate promotion. The current implementation only contains a starter row-count check and does not yet meet this full strategy.
