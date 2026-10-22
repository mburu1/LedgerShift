# ADR 0003: Partitioning and clustering

- Status: Proposed; validate with measured workloads
- Context: partitioning and clustering can improve selective reads but may create small files or skew.
- Decision: do not partition small local datasets by default. Benchmark candidate date clustering/partitioning against representative filters before adopting it in cloud.
- Consequences: performance decisions must be backed by repeatable timings and equivalent output checksums.
