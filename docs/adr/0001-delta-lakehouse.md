# ADR 0001: Delta Lake as the lakehouse storage format

- Status: Accepted for the reference architecture
- Context: the migration requires ACID table writes, schema evolution controls, and repeatable Spark processing.
- Decision: use Delta tables for bronze, silver, and gold; keep bronze append-only and apply validated changes downstream.
- Consequences: local execution needs compatible Spark/Delta versions. Cloud execution and optimization features must be verified in the target workspace.
