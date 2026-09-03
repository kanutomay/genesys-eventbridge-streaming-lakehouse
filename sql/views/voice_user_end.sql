-- voice_user_end
-- Deduplicated "latest state" view over the append-only voice_user_end base table, enriched with
-- agent/queue names joined from the mapper reference views.

CREATE OR REPLACE VIEW "genesys_streaming_lakehouse_curated"."voice_user_end" AS
WITH base AS (
  SELECT
    *,
    COALESCE(
      from_iso8601_timestamp(event_time),   -- ✅ Works with "2025-12-14T08:43:58Z"
      from_iso8601_timestamp(ingest_time)   -- ✅ Works with ISO format
    ) AS recency_ts
  FROM "genesys_streaming_lakehouse"."voice_user_end"
),
ranked AS (
  SELECT
    b.*,
    ROW_NUMBER() OVER (
      PARTITION BY coalesce(conversation_id, interaction_id), participant_id
      ORDER BY recency_ts DESC, event_id DESC
    ) AS rn
  FROM base b
),
dedup AS (
  SELECT *
  FROM ranked
  WHERE rn = 1
)
SELECT
  -- interaction_id is retained for backward compatibility.
  d.event_id, d.event_time, d.ingest_time, d.interaction_id,
  coalesce(d.conversation_id, d.interaction_id) AS conversation_id,
  d.participant_id,
  d.eb_version, d.eb_account, d.eb_region, d.eb_source, d.eb_detail_type, d.eb_time,
  d.topic_name, d.topic_version, d.correlation_id, d.genesys_timestamp, d.genesys_event_time,
  d.session_id, d.disconnect_type, d.media_type, d.provider, d.direction,
  d.ani, d.dnis,
  d.user_id,
  u.name AS user_name,
  d.division_id,
  d.interacting_duration_ms, d.held_duration_ms, d.alerting_duration_ms,
  d.contacting_duration_ms, d.dialing_duration_ms, d.callback_duration_ms,
  d.queue_id,
  q.name AS queue_name,
  d.conversation_external_contact_ids,
  d.raw_event,
  d.opco, d.dt
FROM dedup d
LEFT JOIN "genesys_streaming_lakehouse_curated"."voice_mapper_users_current" u
  ON d.user_id = u.id
LEFT JOIN "genesys_streaming_lakehouse_curated"."voice_mapper_queues_current" q
  ON d.queue_id = q.id;
