# Data Dictionary

Every event-driven pipeline exposes a **near real-time curated view** (2–10 minute latency, deduplicated to the latest state per conversation) — the column sets below describe that view and its underlying base table. In production this platform also materializes a **daily export table** (partitioned Parquet, same column set with a `dt` freeze at export time) for historical/BI workloads; that daily-export job and its CTAS SQL are a documented pattern (see [`sql/daily_export/README.md`](../sql/daily_export/README.md)) but aren't implemented in this sanitized repository — treat any reference to the daily table below as describing the intended, not the shipped, query surface.

Table and view names below use a generic naming convention (`voice_<event>` / `voice_<event>_daily`) representative of the platform's actual catalog structure, with environment-specific prefixes removed.

## Shared conventions across all tables

| Field | Type | Description |
|---|---|---|
| `event_id` | string | Unique event identifier assigned by EventBridge |
| `event_time` | string | Event occurrence time, ISO 8601 |
| `ingest_time` | string | S3 write time from Firehose, ISO 8601 |
| `conversation_id` | string | Primary join key across all event tables |
| `interaction_id` | string | Legacy alias for `conversation_id`, kept for backward compatibility |
| `eb_version` / `eb_account` / `eb_region` / `eb_source` / `eb_detail_type` / `eb_time` | string | EventBridge envelope metadata |
| `topic_name` / `topic_version` | string | Genesys topic name and schema version |
| `correlation_id` | string | Genesys-assigned correlation identifier |
| `genesys_timestamp` / `genesys_event_time` | string / bigint | Genesys-native event timestamp (ISO 8601 and epoch ms) |
| `raw_event` | string | Complete original EventBridge event as JSON — preserved for replay, audit, and schema evolution |
| `opco` | string | Operating company / market partition key (partition key) |
| `dt` | string | Partition date, `YYYY-MM-DD` (partition key) |

## `voice_acd_end` — Queue routing outcomes

| Column | Type | Description |
|---|---|---|
| `participant_id` | string | Participant identifier |
| `session_id` | string | Session identifier |
| `disconnect_type` | string | How the call ended: `TRANSFER`, `PEER`, etc. |
| `media_type` | string | Always `VOICE` for this table |
| `provider` | string | Media provider (e.g. Edge) |
| `direction` | string | `INBOUND` or `OUTBOUND` |
| `ani` | string | Caller number, `tel:+...` format |
| `dnis` | string | Dialed number, `tel:+...` format |
| `queue_id` / `queue_name` | string | Queue that handled the call |
| `division_id` | string | Division identifier |
| `acd_outcome` | string | `ANSWERED`, `ABANDON`, `UNKNOWN`, `FLOW_OUT` |
| `used_routing` | string | Routing method used: `STANDARD`, `MANUAL`, etc. |
| `routing_priority` | int | Routing priority, 0–100 |
| `connected_duration_ms` | bigint | Connected duration in ms |
| `utilization_label` | string | Utilization label identifier |
| `answered_user_id` / `answered_user_name` | string | Agent who answered (only when `acd_outcome = ANSWERED`) |
| `conversation_external_contact_ids` | array\<string\> | External contact identifiers (present on nearly every conversation, illustrative) |
| `requested_routings` | array\<string\> | Requested routing methods (present on most conversations, illustrative) |
| `flow_type` | string | Flow type, e.g. `inqueuecall` (present on most conversations, illustrative) |

## `voice_attributes` — Conversation attributes & customer identifiers

| Column | Type | Description |
|---|---|---|
| `communication_id` | string | Communication channel identifier within the conversation |
| `organization_id` | string | Genesys organization identifier |
| `attributes` | map\<string,string\> | Free-form Genesys attribute key/value map |
| `ani` | string | Caller number |
| `act_acct_cd` | string | Account identifier |
| `ident_business_group` | string | IVR-identified business segment (`B2C`/`B2B`) |
| `ident_service_type` | string | IVR-identified service type (`FIJO`/`MOVIL`) |
| `ident_subscrip_type` | string | IVR-identified subscription type (`POSTPAID`/`PREPAID`) |
| `cust_type_1..3` / `cust_subtype_1..3` | string | Customer type/subtype classification, up to 3 levels |
| `counter{fixed,postpaid,prepaid}_{b2b,b2c}` | string | Active service-line counters by product and segment |
| `outage_flag` / `outage_playback` | string | Outage indicator and outage-message playback flag |
| `fc_flag` | string | Frequent-caller flag (see [Frequent Caller Logic](#related-frequent-caller-enrichment)) |
| `main_language` / `idioma` | string | Detected language |
| `customer_satisfaction` / `net_promoter_score` / `effort` | string | Post-interaction survey scores, when present |
| `ticket_number` / `etr` | string | Associated ticket reference and estimated time to repair, for service-related contacts |

*(Full column list — 77 fields including LLM-assisted-answer metadata, agent soft-skill scores, and whitelist/blacklist flags — available on request; abbreviated here to the fields most relevant to downstream consumers.)*

## `voice_customer_end` — Customer journey completion

| Column | Type | Description |
|---|---|---|
| `external_contact_id` | string | External contact identifier |
| `conversation_external_contact_ids` | array\<string\> | External contact identifiers (present on nearly every conversation, illustrative) |
| `connected_duration_ms` | bigint | Duration connected, ms |

## `voice_flow_end` — IVR flow execution tracking

| Column | Type | Description |
|---|---|---|
| `flow_id` / `flow_version` | string | Flow identifier and version |
| `flow_type` | string | `BOT`, `INBOUNDCALL`, `INQUEUECALL`, etc. |
| `division_id` | string | Division identifier |
| `ani` / `dnis` | string | Caller / dialed number |

## `voice_flow_outcome` — IVR flow decisions & outcomes

| Column | Type | Description |
|---|---|---|
| `flow_id` / `flow_version` | string | Flow identifier and version |
| `flow_outcome_id` | string | Flow outcome identifier |
| `flow_outcome_start_time` / `flow_outcome_end_time` | string | Outcome window, ISO 8601 |
| `flow_outcome_value` | string | Value assigned to the outcome |
| `exit_reason` | string | Reason for flow exit |
| `flow_milestones` | array\<struct\<milestoneId, milestoneTime\>\> | Ordered milestones reached during the flow |

## `voice_transcription_sessions` / `voice_transcription_utterances`

**Sessions:**

| Column | Type | Description |
|---|---|---|
| `session_start_time_ms` | bigint | Session start, epoch ms |
| `transcription_start_time_ms` | bigint | Transcription start, epoch ms |
| `status` | string | `SESSION_ONGOING` or `SESSION_ENDED` |
| `is_session_ended` / `is_session_ongoing` | boolean | Query-optimized status flags |
| `agent_assist_enabled` / `agent_assistant_id` | boolean / string | Agent-assist feature state |
| `voice_transcription_enabled` | boolean | Whether transcription was enabled for this session |
| `speech_text_analytics_program_id` | string | Speech analytics program identifier |

**Utterances:**

| Column | Type | Description |
|---|---|---|
| `num_transcripts` | int | Total utterances in the parent session |
| `utterance_id` | string | Unique utterance identifier |
| `is_final` | boolean | Whether this is the final version of the utterance |
| `channel` | string | `INTERNAL` (agent) or `EXTERNAL` (customer) |
| `confidence` | double | Transcription confidence score, 0.0–1.0 |
| `offset_ms` / `duration_ms` | bigint | Position and length of the utterance within the session |
| `transcript` / `decorated_transcript` | string | Raw and formatted transcribed text |
| `words` / `decorated_words` | string (JSON) | Word-level timing information |
| `dialect` | string | Language dialect used for transcription |
| `engine_provider` / `engine_id` / `engine_name` | string | Transcription engine metadata |

## `voice_user_end` — Agent session tracking

| Column | Type | Description |
|---|---|---|
| `user_id` / `user_name` | string | Agent identifier and name |
| `interacting_duration_ms` / `held_duration_ms` / `alerting_duration_ms` / `contacting_duration_ms` / `dialing_duration_ms` / `callback_duration_ms` | bigint | Per-phase agent handle-time breakdown |
| `queue_id` / `queue_name` | string | Queue associated with the session |

## `voice_mapper_users_current` / `voice_mapper_queues_current` — Reference data

Deduplicated "latest state" views over the daily Genesys Users/Queues API pull, refreshed every day at 06:00 UTC.

| Column | Type | Description |
|---|---|---|
| `id` | string | User or queue identifier |
| `name` | string | Full name |
| `email` *(users only)* | string | User email |
| `department` *(users only)* | string | User department |
| `division_id` / `division_name` | string | Division hierarchy |

## `voice_outages_acd` — Composed analytical view

A pure Athena view (no new ingestion) joining the latest `voice_attributes` outage-flagged rows to the latest `voice_acd_end` routing outcome per conversation:

| Column | Type | Description |
|---|---|---|
| `conversation_id` / `interaction_id` | string | Join keys |
| `ani` | string | Caller number |
| `act_acct_cd` | string | Account identifier |
| `outage_flag` / `outage_playback` | string | Outage indicators from Attributes |
| `queue_id` / `queue_name` / `acd_outcome` / `answered_user_id` / `answered_user_name` | — | Routing outcome from ACD.End |

## Related: Frequent Caller Logic

The `fc_flag` field on `voice_attributes` is populated by a separate, non-streaming enrichment job: an Athena query counts distinct conversations per account over a trailing 7-day window and flags accounts with 3+ contacts as frequent callers. That job draws on a broader subscriber/account reference pipeline that sits outside this platform's EventBridge streaming pattern and is not included in this repository.
