-- voice_outages_acd: composed analytical view
-- Joins the latest outage-flagged voice_attributes row to the latest voice_acd_end routing
-- outcome per conversation, so an outage's call-handling impact can be queried directly —
-- no new ingestion required, just a view over two existing near-real-time tables.

-- 1. Create (or replace) the view
-- Via the Athena/Glue console, or the AWS CLI query below.

CREATE OR REPLACE VIEW "genesys_streaming_lakehouse_curated"."voice_outages_acd" AS
WITH outage_attr AS (
  SELECT
    conversation_id,
    interaction_id,
    ani,
    act_acct_cd,
    outage_flag,
    outage_playback,
    dt,
    -- helps when there are multiple attribute events per conversation
    genesys_event_time,
    event_id,
    ROW_NUMBER() OVER (
      PARTITION BY conversation_id, dt
      ORDER BY genesys_event_time DESC, event_id DESC
    ) AS rn
  FROM "genesys_streaming_lakehouse_curated"."voice_attributes"
  WHERE outage_flag = 'Y'
),
acd AS (
  SELECT
    conversation_id,
    interaction_id,
    queue_id,
    queue_name,
    acd_outcome,
    answered_user_id,
    answered_user_name,
    dt,
    genesys_event_time,
    event_id,
    ROW_NUMBER() OVER (
      PARTITION BY conversation_id, dt
      ORDER BY genesys_event_time DESC, event_id DESC
    ) AS rn
  FROM "genesys_streaming_lakehouse_curated"."voice_acd_end"
)
SELECT
  o.conversation_id,
  COALESCE(o.interaction_id, a.interaction_id) AS interaction_id, -- legacy-safe
  o.ani,
  o.act_acct_cd,
  o.outage_flag,
  o.outage_playback,
  a.queue_id,
  a.queue_name,
  a.acd_outcome,
  a.answered_user_id,
  a.answered_user_name,
  o.dt
FROM outage_attr o
LEFT JOIN acd a
  ON o.conversation_id = a.conversation_id
 AND o.dt = a.dt
 AND a.rn = 1
WHERE o.rn = 1;

-- 2. Equivalent, via the AWS CLI (useful for CI/CD or a one-off deploy without console access):
--
-- aws athena start-query-execution \
--   --query-string 'CREATE OR REPLACE VIEW "genesys_streaming_lakehouse_curated"."voice_outages_acd" AS ...' \
--   --result-configuration OutputLocation=s3://aws-athena-query-results-123456789012-us-east-1/ \
--   --region us-east-1

-- 3. Sample queries

-- 3a. Browse recent outage-flagged calls
SELECT *
FROM "genesys_streaming_lakehouse_curated"."voice_outages_acd"
LIMIT 10;

-- 3b. Daily answered-rate during outage windows
SELECT
  dt,
  COUNT(*) AS outage_calls,
  SUM(CASE WHEN answered_user_id IS NOT NULL OR acd_outcome = 'ANSWERED' THEN 1 ELSE 0 END) AS answered_calls,
  100.0 * SUM(CASE WHEN answered_user_id IS NOT NULL OR acd_outcome = 'ANSWERED' THEN 1 ELSE 0 END) / COUNT(*) AS answered_pct
FROM "genesys_streaming_lakehouse_curated"."voice_outages_acd"
GROUP BY 1
ORDER BY 1 DESC;

-- 4. To drop the view:
-- DROP VIEW "genesys_streaming_lakehouse_curated"."voice_outages_acd";
