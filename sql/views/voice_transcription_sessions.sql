-- voice_transcription_sessions
-- Deduplicated "latest state" view over the append-only voice_transcription_sessions base table
-- (partitioned by conversation_id + communication_id, not participant_id -- a transcription session
-- has no participant_id).

CREATE OR REPLACE VIEW "genesys_streaming_lakehouse_curated"."voice_transcription_sessions" AS
WITH base AS (
  SELECT
    *,
    COALESCE(
      from_iso8601_timestamp(event_time),
      from_iso8601_timestamp(ingest_time)
    ) AS recency_ts
  FROM "genesys_streaming_lakehouse"."voice_transcription_sessions"
),
ranked AS (
  SELECT
    b.*,
    ROW_NUMBER() OVER (
      PARTITION BY conversation_id, communication_id
      ORDER BY recency_ts DESC, event_id DESC
    ) AS rn
  FROM base b
)
SELECT
  event_id, event_time, ingest_time, conversation_id, interaction_id, communication_id,
  eb_version, eb_account, eb_region, eb_source, eb_detail_type, eb_time,
  topic_name, topic_version, correlation_id, genesys_timestamp, genesys_event_time,
  organization_id, session_start_time_ms, transcription_start_time_ms,
  status, status_offset_ms, is_session_ended, is_session_ongoing, has_transcripts, num_transcripts,
  raw_event,
  opco, dt
FROM ranked
WHERE rn = 1;
