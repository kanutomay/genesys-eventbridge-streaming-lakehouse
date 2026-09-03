# Curated Views

The platform actually spans two Glue/Athena databases: a raw ingestion database holding the append-only, `dt`-partitioned base tables Firehose writes directly (`genesys_streaming_lakehouse`), and a separate curated/consumer-facing database (`genesys_streaming_lakehouse_curated`) holding the views below, which is what dashboards, ad-hoc analysis, and [`outages_acd.sql`](outages_acd.sql) actually query against. See [`docs/data-dictionary.md`](../../docs/data-dictionary.md) for the full column-level schema behind each view.

**Per-pipeline dedup views** — one per event pipeline, each built on its raw base table:

| View | Base table | Notes |
|---|---|---|
| [`voice_acd_end.sql`](voice_acd_end.sql) | `voice_acd_end` | Enriched with `queue_name` / `answered_user_name`, joined from the two mapper reference views below. |
| [`voice_attributes.sql`](voice_attributes.sql) | `voice_attributes` | Outage-flag fields used by `outages_acd.sql`. |
| [`voice_customer_end.sql`](voice_customer_end.sql) | `voice_customer_end` | |
| [`voice_flow_end.sql`](voice_flow_end.sql) | `voice_flow_end` | |
| [`voice_flow_outcome.sql`](voice_flow_outcome.sql) | `voice_flow_outcome` | |
| [`voice_user_end.sql`](voice_user_end.sql) | `voice_user_end` | Enriched with `queue_name` / `user_name`, joined from the two mapper reference views below. |
| [`voice_transcription_sessions.sql`](voice_transcription_sessions.sql) | `voice_transcription_sessions` | Partitions by `(conversation_id, communication_id)` — no `participant_id` on this event type. |
| [`voice_transcription_utterances.sql`](voice_transcription_utterances.sql) | `voice_transcription_utterances` | Flattens the event's `transcripts` JSON array (`CROSS JOIN UNNEST`) into one row per utterance before deduplicating on `(conversation_id, communication_id, utterance_id)`. |

All eight follow the same shape: a `base` CTE computes a `recency_ts` as `COALESCE(from_iso8601_timestamp(event_time), from_iso8601_timestamp(ingest_time))` (falling back to ingest time when an event carries no usable `event_time`), then a `ranked` CTE applies `ROW_NUMBER() OVER (PARTITION BY conversation_id, participant_id ORDER BY recency_ts DESC, event_id DESC)` and the final `SELECT` keeps only `rn = 1` — the most recent row per conversation/participant. This is deliberately more than a plain `PARTITION BY conversation_id`: Genesys can emit multiple in-flight participants per conversation (e.g. a transfer), so partitioning on the pair is what keeps each participant's own latest state instead of collapsing them together.

[`outages_acd.sql`](outages_acd.sql) is the one composed, cross-pipeline view in the platform: it joins the latest outage-flagged `voice_attributes` row to the latest `voice_acd_end` routing outcome per conversation, with no new ingestion required.

[`voice_mapper_users_current.sql`](voice_mapper_users_current.sql) and [`voice_mapper_queues_current.sql`](voice_mapper_queues_current.sql) are the reference-data equivalent: the Mappers Glue job writes a new dt-partitioned snapshot to `voice_mapper_users` / `voice_mapper_queues` every day, and these views expose only the most recent partition — a `MAX(dt)` filter rather than the `ROW_NUMBER()` pattern used elsewhere, since each day's pull is a full snapshot rather than a progressive per-conversation event stream. `voice_acd_end` and `voice_user_end` both `LEFT JOIN` these two views to surface human-readable queue/agent names alongside the raw IDs.
