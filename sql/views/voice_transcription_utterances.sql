-- voice_transcription_utterances
-- Flattens each event's transcripts array (one row per event covering the whole utterance batch to
-- that point) into one row per utterance, then deduplicates to the latest version of each
-- (conversation_id, communication_id, utterance_id).

CREATE OR REPLACE VIEW "genesys_streaming_lakehouse_curated"."voice_transcription_utterances" AS
WITH base AS (
  SELECT
    conversation_id,
    interaction_id,
    communication_id,
    event_id,
    event_time,
    ingest_time,
    num_transcripts,
    opco,
    dt,
    COALESCE(
      from_iso8601_timestamp(event_time),
      from_iso8601_timestamp(ingest_time)
    ) AS recency_ts,
    CAST(json_parse(transcripts) AS ARRAY(MAP(VARCHAR, JSON))) AS transcripts_array
  FROM "genesys_streaming_lakehouse"."voice_transcription_utterances"
),
exploded AS (
  SELECT
    b.conversation_id,
    b.interaction_id,
    b.communication_id,
    b.event_id,
    b.event_time,
    b.ingest_time,
    b.num_transcripts,
    b.opco,
    b.dt,
    b.recency_ts,
    t AS utterance_map
  FROM base b
  CROSS JOIN UNNEST(b.transcripts_array) AS t(t)
),
typed AS (
  SELECT
    conversation_id,
    interaction_id,
    communication_id,
    event_id,
    event_time,
    ingest_time,
    num_transcripts,
    json_extract_scalar(utterance_map['utterance_id'], '$') AS utterance_id,
    CAST(json_extract_scalar(utterance_map['is_final'], '$') AS BOOLEAN) AS is_final,
    json_extract_scalar(utterance_map['channel'], '$') AS channel,
    CAST(json_extract_scalar(utterance_map['confidence'], '$') AS DOUBLE) AS confidence,
    CAST(json_extract_scalar(utterance_map['offset_ms'], '$') AS BIGINT) AS offset_ms,
    CAST(json_extract_scalar(utterance_map['duration_ms'], '$') AS BIGINT) AS duration_ms,
    json_extract_scalar(utterance_map['transcript'], '$') AS transcript,
    json_extract_scalar(utterance_map['decorated_transcript'], '$') AS decorated_transcript,
    json_extract_scalar(utterance_map['words'], '$') AS words,
    json_extract_scalar(utterance_map['decorated_words'], '$') AS decorated_words,
    json_extract_scalar(utterance_map['engine_provider'], '$') AS engine_provider,
    json_extract_scalar(utterance_map['engine_id'], '$') AS engine_id,
    json_extract_scalar(utterance_map['engine_name'], '$') AS engine_name,
    json_extract_scalar(utterance_map['dialect'], '$') AS dialect,
    CAST(json_extract_scalar(utterance_map['agent_assist_enabled'], '$') AS BOOLEAN) AS agent_assist_enabled,
    json_extract_scalar(utterance_map['agent_assistant_id'], '$') AS agent_assistant_id,
    CAST(json_extract_scalar(utterance_map['voice_transcription_enabled'], '$') AS BOOLEAN) AS voice_transcription_enabled,
    json_extract_scalar(utterance_map['speech_text_analytics_program_id'], '$') AS speech_text_analytics_program_id,
    opco,
    dt,
    recency_ts
  FROM exploded
),
ranked AS (
  SELECT
    t.*,
    ROW_NUMBER() OVER (
      PARTITION BY conversation_id, communication_id, utterance_id
      ORDER BY recency_ts DESC, event_id DESC
    ) AS rn
  FROM typed t
)
SELECT
  conversation_id,
  interaction_id,
  communication_id,
  event_id,
  event_time,
  ingest_time,
  num_transcripts,
  utterance_id,
  is_final,
  channel,
  confidence,
  offset_ms,
  duration_ms,
  transcript,
  decorated_transcript,
  words,
  decorated_words,
  engine_provider,
  engine_id,
  engine_name,
  dialect,
  agent_assist_enabled,
  agent_assistant_id,
  voice_transcription_enabled,
  speech_text_analytics_program_id,
  opco,
  dt
FROM ranked
WHERE rn = 1;
