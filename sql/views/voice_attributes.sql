-- voice_attributes
-- Deduplicated "latest state" view over the append-only voice_attributes base table. Several fields
-- fall back to a regex extraction from the raw IVR log_trace string when the structured attribute is absent.

CREATE OR REPLACE VIEW "genesys_streaming_lakehouse_curated"."voice_attributes" AS
WITH base AS (
  SELECT
    *,
    COALESCE(
      from_iso8601_timestamp(event_time),   -- ✅ Works with "2025-12-14T08:43:58Z"
      from_iso8601_timestamp(ingest_time)   -- ✅ Works with ISO format
    ) AS recency_ts
  FROM "genesys_streaming_lakehouse"."voice_attributes"
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
  event_id, event_time, ingest_time,
  coalesce(conversation_id, interaction_id) as conversation_id, interaction_id, participant_id,
  eb_version, eb_account, eb_region, eb_source, eb_detail_type, eb_time,
  topic_name, topic_version, correlation_id, genesys_timestamp, genesys_event_time,
  communications, attributes,
  ani, caller_phone_number, ani_in_identity_platform, identity_platform_unified, identity_platform_voice_status_code,
  log_trace,
  COALESCE(ident_business_group, regexp_extract(log_trace, 'IDENT_BUSINESS_GROUP:\s*([^\s\-]+)', 1)) AS ident_business_group,
  COALESCE(ident_service_type, regexp_extract(log_trace, 'IDENT_SERVICE_TYPE:\s*([^\s\-]+)', 1)) AS ident_service_type,
  COALESCE(ident_subscrip_type, regexp_extract(log_trace, 'IDENT_SUBSCRIP_TYPE:\s*([^\s\-]+)', 1)) AS ident_subscrip_type,
  COALESCE(ident_service_lines, regexp_extract(log_trace, 'IDENT_SERVICE_LINES:\s*([^\s\-]+)', 1)) AS ident_service_lines,
  search_ani, trace_called_address,
  flag_total, act_acct_cd,
  COALESCE(identity_platform_account_id, attributes['IDENTITY PLATFORM ACCOUNT ID']) AS identity_platform_account_id,
  COALESCE(fixed_account_numbers, attributes['Fixed_Account_Numbers']) AS fixed_account_numbers,
  COALESCE(postpaid_account_numbers, attributes['Postpaid_Account_Numbers']) AS postpaid_account_numbers,
  act_cust_type_grp, cust_type_1, cust_subtype_1,
  cust_type_2, cust_subtype_2, cust_type_3, cust_subtype_3,
  COALESCE(counterfixed_b2b, attributes['CounterFixed B2B']) AS counterfixed_b2b,
  COALESCE(counterfixed_b2c, attributes['CounterFixed B2C']) AS counterfixed_b2c,
  COALESCE(counterpostpaid_b2b, attributes['CounterPostpaid B2B']) AS counterpostpaid_b2b,
  COALESCE(counterpostpaid_b2c, attributes['CounterPostpaid B2C']) AS counterpostpaid_b2c,
  COALESCE(counterprepaid_b2b, attributes['CounterPrepaid B2B']) AS counterprepaid_b2b,
  COALESCE(counterprepaid_b2c, attributes['CounterPrepaid B2C']) AS counterprepaid_b2c,
  outage_flag, outage_playback, pd_bb_tech, pd_mix_nm,
  numero_vip, deflection, whitelisted, blacklisted, account_typed, whatsapp_time,
  llm_aw_id, llm_aw_intents, llm_aw_language, llm_aw_question, llm_aw_answer,
  agent_kindness, customer_satisfaction, net_promoter_score, issue_resolution_flag, effort,
  ticket_number, etr, fc_flag, act_prio_cat,
  main_language, idioma,
  opco, dt
FROM ranked
WHERE rn = 1;
