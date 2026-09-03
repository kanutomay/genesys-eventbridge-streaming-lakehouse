# Daily Export (CTAS Compaction)

> **Documented pattern, not implemented in this repository.** This describes an architectural extension used in production; no CTAS SQL templates or scheduled Glue job for it are included here. See "Two-tier analytics" in the [root README](../../README.md#key-technical-features) for what ships instead and why.

Scheduled Glue jobs that materialize each curated view into a partitioned, query-optimized daily Parquet table via `CREATE TABLE AS SELECT`, trading the near-real-time view's per-query deduplication cost for fast, compaction-friendly historical scans.
