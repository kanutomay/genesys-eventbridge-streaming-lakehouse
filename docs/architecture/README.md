# Architecture Notes

See the [root README's architecture diagram](../../README.md#architecture) for the platform-wide overview.

Six of the seven event-driven pipelines (`acd-end`, `attributes`, `customer-end`, `flow-end`, `flow-outcome`, `user-end`) and, with one addition, `transcription`, share a single structural pattern — so rather than seven near-identical diagrams, there are two:

- [`event-pipeline-pattern.md`](event-pipeline-pattern.md) — the shared EventBridge → Firehose → Lambda → S3 → Glue → CloudWatch shape, plus transcription's dual-output variant.
- [`mappers-pattern.md`](mappers-pattern.md) — the one pipeline that doesn't fit that shape: a scheduled Glue Python Shell batch job against the Genesys REST API, with no EventBridge ingestion at all.

Both are drawn independently from the documented architecture (not derived from or edited from the original production diagrams, which show account-specific AWS resource identifiers) — see [`docs/data-dictionary.md`](../data-dictionary.md) for the per-pipeline field-level differences these diagrams don't cover.
