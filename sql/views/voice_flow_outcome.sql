-- voice_flow_outcome
-- Deduplicated "latest state" view over the append-only voice_flow_outcome base table.

CREATE OR REPLACE VIEW "genesys_streaming_lakehouse_curated"."voice_flow_outcome" AS
WITH base AS (
  SELECT
    *,
    COALESCE(
      from_iso8601_timestamp(event_time),   -- ✅ Works with "2025-12-14T08:43:58Z"
      from_iso8601_timestamp(ingest_time)   -- ✅ Works with ISO format
    ) AS recency_ts
  FROM "genesys_streaming_lakehouse"."voice_flow_outcome"
),
ranked AS (
  SELECT
    b.*,
    ROW_NUMBER() OVER (
      PARTITION BY coalesce(conversation_id, interaction_id), participant_id
      ORDER BY recency_ts DESC, event_id DESC
    ) AS rn
  FROM base b
)
SELECT
  --interaction_id is retained for backward compatibility.
  event_id, event_time, ingest_time, interaction_id,
  coalesce(conversation_id, interaction_id) as conversation_id, participant_id,
  eb_version, eb_account, eb_region, eb_source, eb_detail_type, eb_time,
  topic_name, topic_version, correlation_id, genesys_timestamp, genesys_event_time,
  session_id, media_type, provider, direction, ani, dnis,
  flow_type, flow_id, division_id, flow_version, flow_outcome_id,
  flow_outcome_start_time, flow_outcome_end_time,
  flow_outcome_value, flow_milestones,
  conversation_external_contact_ids,
  raw_event,
  opco, dt
FROM ranked
WHERE rn = 1;
