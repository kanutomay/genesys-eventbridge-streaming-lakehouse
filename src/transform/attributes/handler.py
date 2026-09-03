# Name: genesys_voice_attributes_transformation.py
# Revision: 1.0.5
# Author: Mario May
# Date: 2025-12-21
# Description: This AWS Lambda function is designed to transform Genesys EventBridge attributes events for ingestion into Kinesis Firehose.
# Change log:
# v1.0.5 - 2025-12-21 by Mario May:
# - Added the following additional fields to the transformed output: identity_platform_account_id, fixed_account_numbers, postpaid_account_numbers, counterfixed_b2b, counterfixed_b2c, counterpostpaid_b2b, counterpostpaid_b2c, counterprepaid_b2b, counterprepaid_b2c.
# - Added regex extraction of LOG_TRACE fields: ident_business_group, ident_service_type, ident_subscrip_type, ident_service_lines for improved query performance.
# v1.0.4 - 2025-12-19 by Mario May:
# - Added additional fields to the transformed output for commonly used attributes: outage_playback, cust_type_2, cust_subtype_2, cust_type_3 and  cust_subtype_3.
# v1.0.3 - 2025-12-17 by Mario May:
# - Added conversation_id field for clarity and parity with Genesys terminology.
# - Kept interaction_id field (legacy) for backward compatibility.
# - v1.0.2 - 2025-12-15 by Mario May:
# - Removed unused extraction of 'eb_resources' and 'organization_id' fields.
# - v1.0.1 - 2025-12-14 by Mario May:
# - Corrected ingestion timestamp timezone setting to UTC
# - Preserve event_time in original ISO 8601 format (removed parse_iso_timestamp conversion)
# - Extract partition date 'dt' via string slicing instead of datetime parsing
# - Remove unused parse_iso_timestamp() and extract_date() functions
# - Eliminate redundant event_time_iso variable
# - Changed output field from 'ingestion_time' to 'ingest_time' for consistency with glue table schema
# v1.0.0 - 2025-12-11 by Mario May:
# - Initial release

"""
Kinesis Firehose Data Transformation Lambda
============================================
Transforms Genesys EventBridge attributes events from JSON to Parquet-ready format

Purpose:
- Minimal transformation (keep near-raw data)
- Extract key metadata fields for partitioning and querying
- Preserve all attributes in a MAP structure
- Add ingestion timestamp

Input: EventBridge JSON events (may be concatenated JSON Lines)
Output: Transformed JSON for Firehose → Parquet conversion

Environment Variables:
- OPCO: Operating company code (default: ACME)
"""

import json
import base64
import os
import re
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
ENV = os.getenv("ENVIRONMENT", "production")  # Default to production if not set
OPCO = os.environ.get('OPCO', 'ACME')  # Operating company code
ENABLE_DEBUG = os.getenv("ENABLE_DEBUG", "0") == "1"  # Enable debug logging


def lambda_handler(event: Dict, context: Any) -> Dict:
    """
    Lambda handler for Firehose transformation

    Args:
        event: Firehose transformation event with records
        context: Lambda context (unused)

    Returns:
        Transformed records for Firehose
    """
    # Set request ID for structured logging
    structured_log.request_id = context.aws_request_id

    # Processing statistics
    stats = {
        'total_input': len(event['records']),
        'successful': 0,
        'failed': 0,
        'missing_conversation_id': 0,
        'missing_ani': 0,
        'json_parse_errors': 0
    }

    structured_log("INFO", "lambda_start",
                   message=f"Processing {stats['total_input']} records",
                   input_record_count=stats['total_input'])

    output_records = []

    for record in event['records']:
        record_id = record['recordId']

        try:
            # Decode the base64 payload
            payload = base64.b64decode(record['data']).decode('utf-8')

            # Handle JSON Lines format (concatenated JSON objects)
            parsed_events = parse_json_lines(payload, record_id, stats)

            for parsed_event in parsed_events:
                # Transform the event
                transformed = transform_attributes_event(parsed_event, opco=OPCO, record_id=record_id, stats=stats)

                # Encode back to base64 for Firehose
                output_data = json.dumps(transformed) + '\n'
                encoded_data = base64.b64encode(output_data.encode('utf-8')).decode('utf-8')

                output_records.append({
                    'recordId': record_id,
                    'result': 'Ok',
                    'data': encoded_data
                })

                stats['successful'] += 1

        except Exception as e:
            stats['failed'] += 1

            structured_log("ERROR", "record_processing_failed", None, record_id,
                           f"Failed to process record: {str(e)}",
                           error_type=type(e).__name__,
                           payload_length=len(payload) if 'payload' in locals() else 0)

            # Return original record with ProcessingFailed status
            output_records.append({
                'recordId': record_id,
                'result': 'ProcessingFailed',
                'data': record['data']
            })

    # Calculate success rate
    success_rate = round(stats['successful'] / stats['total_input'] * 100, 2) if stats['total_input'] > 0 else 0

    structured_log("INFO", "lambda_complete",
                   message="Lambda processing complete",
                   total_input=stats['total_input'],
                   successful=stats['successful'],
                   failed=stats['failed'],
                   missing_conversation_id=stats['missing_conversation_id'],
                   missing_ani=stats['missing_ani'],
                   json_parse_errors=stats['json_parse_errors'],
                   success_rate=success_rate)

    return {'records': output_records}

def parse_json_lines(payload: str, record_id: str = None, stats: Dict = None) -> List[Dict]:
    """
    Parse JSON Lines format (concatenated JSON objects)

    Args:
        payload: String containing one or more JSON objects
        record_id: Record identifier for logging
        stats: Statistics dictionary to update

    Returns:
        List of parsed JSON objects
    """
    json_objects = []
    brace_count = 0
    current_obj_start = 0

    for i, char in enumerate(payload):
        if char == '{':
            if brace_count == 0:
                current_obj_start = i
            brace_count += 1
        elif char == '}':
            brace_count -= 1
            if brace_count == 0:
                obj_str = payload[current_obj_start:i+1]
                try:
                    json_objects.append(json.loads(obj_str))
                except json.JSONDecodeError as e:
                    if stats:
                        stats['json_parse_errors'] += 1

                    structured_log("ERROR", "json_parse_error", None, record_id,
                                   f"JSON parse error at position {current_obj_start}",
                                   position=current_obj_start,
                                   error=str(e),
                                   payload_snippet=obj_str[:200])

    # If no objects found, try parsing the entire payload as single JSON
    if not json_objects:
        try:
            json_objects.append(json.loads(payload))
        except json.JSONDecodeError as e:
            if stats:
                stats['json_parse_errors'] += 1

            structured_log("ERROR", "json_parse_failed", None, record_id,
                           f"Failed to parse entire payload as JSON: {str(e)}",
                           payload_length=len(payload),
                           payload_snippet=payload[:200])

    return json_objects


def extract_log_trace_field(log_trace: str, field_name: str) -> str:
    """
    Extract a specific field from LOG_TRACE string using regex

    Args:
        log_trace: LOG_TRACE string value
        field_name: Field name to extract (e.g., 'IDENT_BUSINESS_GROUP')

    Returns:
        Extracted value or None if not found
    """
    if not log_trace:
        return None

    pattern = rf'{field_name}:\s*([^\s\-]+)'
    match = re.search(pattern, log_trace)
    return match.group(1) if match else None


def transform_attributes_event(event: Dict, opco: str = 'ACME', record_id: str = None, stats: Dict = None) -> Dict:
    """
    Transform a single Genesys attributes event

    Args:
        event: Original EventBridge event
        opco: Operating company code (default: ACME)
        record_id: Record identifier for logging
        stats: Statistics dictionary to update

    Returns:
        Transformed event ready for Parquet storage
    """
    detail = event.get('detail', {})
    event_body = detail.get('eventBody', {})
    metadata = detail.get('metadata', {})
    attributes = event_body.get('attributes', {})
    communications = event_body.get('communications', [])

    # Extract timestamps
    event_time = event.get('time')
    genesys_timestamp = detail.get('timestamp')

    # Current time for ingestion timestamp
    ingest_time = datetime.now(timezone.utc).isoformat() # Ensure UTC timezone. Format as ISO 8601 (e.g., 2025-12-14T12:34:56.789Z)

    # Generate current date in UTC+0 format
    if event_time and len(event_time) >= 10:
        # Extract date from event time: "2025-08-08"
        dt = event_time[:10]
    else:
        # Fallback to current UTC+0 date for data extraction timestamp
        dt = ingest_time[:10]

    # Extract critical fields for validation
    conversation_id = event_body.get('conversationId')
    participant_id = event_body.get('participantId')
    ani = attributes.get('ANI') or attributes.get('Ani')

    # Track missing critical fields
    if not conversation_id:
        if stats:
            stats['missing_conversation_id'] += 1
        structured_log("WARNING", "missing_conversation_id", None, record_id,
                       "Missing conversationId in event",
                       event_id=event.get('id'),
                       participant_id=participant_id)

    if not ani:
        if stats:
            stats['missing_ani'] += 1
        structured_log("WARNING", "missing_ani", conversation_id, record_id,
                       "ANI not found in attributes",
                       event_id=event.get('id'),
                       num_attributes=len(attributes))

    # Log successful extraction
    structured_log("INFO", "record_processing", conversation_id, record_id,
                   "Processing attributes event",
                   participant_id=participant_id,
                   num_attributes=len(attributes),
                   num_communications=len(communications),
                   has_ani=bool(ani),
                   has_conversation_id=bool(conversation_id))

    # Build transformed record
    transformed = {
        # ====================================================================
        # Metadata & Identifiers
        # ====================================================================
        'event_id': event.get('id'),
        'event_time': event_time,
        'ingest_time': ingest_time,
        'conversation_id': conversation_id,
        'interaction_id': conversation_id, # Legacy field for compatibility
        'participant_id': participant_id,

        # ====================================================================
        # EventBridge Envelope
        # ====================================================================
        'eb_version': event.get('version'),
        'eb_account': event.get('account'),
        'eb_region': event.get('region'),
        'eb_source': event.get('source'),
        'eb_detail_type': event.get('detail-type'),
        'eb_time': event_time,

        # ====================================================================
        # Genesys Event Body
        # ====================================================================
        'topic_name': detail.get('topicName'),
        'topic_version': detail.get('version'),
        'correlation_id': metadata.get('CorrelationId'),
        'genesys_timestamp': genesys_timestamp,
        'genesys_event_time': event_body.get('eventTime'),

        # ====================================================================
        # Communications Array
        # ====================================================================
        'communications': [
            {
                'id': comm.get('id'),
                'media_type': comm.get('mediaType')
            }
            for comm in communications
        ],

        # ====================================================================
        # All Attributes as MAP
        # ====================================================================
        'attributes': attributes,

        # ====================================================================
        # Commonly Used Attributes (Promoted for easier querying)
        # ====================================================================
        # Customer Identification
        'ani': ani,
        'caller_phone_number': attributes.get('CallerPhoneNumber'),
        'ani_in_identity_platform': attributes.get(' ani_in_identity_platform'),  # Note: space in key
        'identity_platform_unified': attributes.get('IDENTITY_PLATFORM_UNIFIED'),
        'identity_platform_voice_status_code': attributes.get('IDENTITY_PLATFORM_VOICE_STATUS_CODE'),

        # Call Flow & Routing
        'log_trace': attributes.get('LOG_TRACE'),
        'ident_business_group': extract_log_trace_field(attributes.get('LOG_TRACE'), 'IDENT_BUSINESS_GROUP'),
        'ident_service_type': extract_log_trace_field(attributes.get('LOG_TRACE'), 'IDENT_SERVICE_TYPE'),
        'ident_subscrip_type': extract_log_trace_field(attributes.get('LOG_TRACE'), 'IDENT_SUBSCRIP_TYPE'),
        'ident_service_lines': extract_log_trace_field(attributes.get('LOG_TRACE'), 'IDENT_SERVICE_LINES'),
        'search_ani': attributes.get('searchANI'),
        'trace_called_address': attributes.get('Trace CalledAddress'),

        # Customer Segmentation
        'flag_total': attributes.get('Flag_Total'),
        'act_acct_cd': attributes.get('act_acct_cd'),
        'identity_platform_account_id': attributes.get('IDENTITY PLATFORM ACCOUNT ID'),
        'fixed_account_numbers': attributes.get('Fixed_Account_Numbers'),
        'postpaid_account_numbers': attributes.get('Postpaid_Account_Numbers'),
        'act_cust_type_grp': attributes.get('act_cust_type_grp'),
        'cust_type_1': attributes.get('cust_type_1'),
        'cust_subtype_1': attributes.get('cust_subtype_1'),
        'cust_type_2': attributes.get('cust_type_2'),
        'cust_subtype_2': attributes.get('cust_subtype_2'),
        'cust_type_3': attributes.get('cust_type_3'),
        'cust_subtype_3': attributes.get('cust_subtype_3'),
        'fixed_visible': attributes.get('FixedVisible'),
        'postpaid_visible': attributes.get('PostpaidVisible'),
        'prepaid_visible': attributes.get('PrepaidVisible'),

        # Service Counters
        'counterfixed_b2b': attributes.get('CounterFixed B2B'),
        'counterfixed_b2c': attributes.get('CounterFixed B2C'),
        'counterpostpaid_b2b': attributes.get('CounterPostpaid B2B'),
        'counterpostpaid_b2c': attributes.get('CounterPostpaid B2C'),
        'counterprepaid_b2b': attributes.get('CounterPrepaid B2B'),
        'counterprepaid_b2c': attributes.get('CounterPrepaid B2C'),

        # Network & Technical
        'outage_flag': attributes.get('outage_flag'),
        'outage_playback': attributes.get('outage_playback'),
        'pd_bb_tech': attributes.get('pd_bb_tech'),
        'pd_mix_nm': attributes.get('pd_mix_nm'),

        # VIP & Priority
        'numero_vip': attributes.get('Numero  VIP'),  # Note: double space
        'deflection': attributes.get('Deflection'),
        'whitelisted': attributes.get('whitelisted'),
        'blacklisted': attributes.get('blacklisted'),
        'account_typed': attributes.get('acctnumber_typed'),
        'whatsapp_time': attributes.get('WhatsAppTime'),

        # LLM/AI Fields
        'llm_aw_id': attributes.get('LLM (AW) Id'),
        'llm_aw_intents': attributes.get('LLM (AW) Intents'),
        'llm_aw_language': attributes.get('LLM (AW) Language'),
        'llm_aw_question': attributes.get('LLM (AW) question'),
        'llm_aw_answer': attributes.get('LLM (AW) answer'),

        # Survey/CSAT Fields
        'agent_kindness': attributes.get('agent_kindness'),
        'customer_satisfaction': attributes.get('customer_satisfaction_grade'),
        'net_promoter_score': attributes.get('net_promoter_score'),
        'issue_resolution_flag': attributes.get('issue_resolution_flag'),
        'effort': attributes.get('effort'),

        # Service Details
        'ticket_number': attributes.get('Ticket_Number'),
        'etr': attributes.get('ETR'),
        'fc_flag': attributes.get('fc_flag'),
        'act_prio_cat': attributes.get('act_prio_cat'),

        # Language & Localization
        'main_language': attributes.get('Main_language'),
        'idioma': attributes.get('IDIOMA'),

        # ====================================================================
        # Partitioning Fields
        # ====================================================================
        'opco': opco,
        'dt': dt
    }

    # Debug logging of transformed record
    if ENABLE_DEBUG:
        structured_log("INFO", "transformed_record_debug", conversation_id, record_id,
                    "Transformed record summary",
                    event_id=transformed.get('event_id'),
                    num_attributes=len(transformed.get('attributes', {})),
                    partition_dt=transformed.get('dt'),
                    has_ani=bool(transformed.get('ani')),
                    has_log_trace=bool(transformed.get('log_trace')))

    # Return the transformed record
    return transformed

def structured_log(level: str, event_type: str, conversation_id: str = None, record_id: str = None,
                  message: str = "", **kwargs):
    """
    Emit structured log entries for monitoring and alerting.

    Args:
        level: Log level (INFO, WARNING, ERROR)
        event_type: Type of event for filtering
        conversation_id: Conversation identifier
        record_id: Record identifier
        message: Human readable message
        **kwargs: Additional structured fields
    """
    log_entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": event_type,
        "level": level,
        "conversation_id": conversation_id,
        "record_id": record_id,
        "message": message,
        "lambda_request_id": getattr(structured_log, 'request_id', None),
        **kwargs
    }

    # Remove None values for cleaner logs
    log_entry = {k: v for k, v in log_entry.items() if v is not None}

    log_message = f"STRUCTURED_LOG: {json.dumps(log_entry, separators=(',', ':'))}"

    if level == "ERROR":
        logger.error(log_message)
    elif level == "WARNING":
        logger.warning(log_message)
    else:
        logger.info(log_message)

# ============================================================================
# Testing Helper
# ============================================================================
if __name__ == "__main__":
    """
    Local testing
    """
    # Sample test event
    test_payload = {
        "version": "0",
        "id": "test-event-id",
        "detail-type": "v2.detail.events.conversation.{id}.attributes",
        "source": "aws.partner/genesys.com/cloud/test",
        "account": "123456789012",
        "time": "2025-12-10T08:43:58Z",
        "region": "us-east-1",
        "resources": [],
        "detail": {
            "topicName": "v2.conversations.test.attributes",
            "version": "2",
            "eventBody": {
                "conversationId": "test-conversation-id",
                "participantId": "test-participant-id",
                "eventTime": 1765356238647,
                "organizationId": "test-org-id",
                "communications": [
                    {"id": "comm-123", "mediaType": "VOICE"}
                ],
                "attributes": {
                    "ANI": "000-0000-0000",
                    "IDENTITY_PLATFORM_UNIFIED": "12345",
                    "outage_flag": "true",
                    "LLM (AW) question": "How do I pay my bill?"
                }
            },
            "metadata": {
                "CorrelationId": "test-correlation-id"
            },
            "timestamp": "2025-12-10T08:43:58.648Z"
        }
    }

    result = transform_attributes_event(test_payload, opco='ACME')
    print(json.dumps(result, indent=2))
    print(f"\nPartition path: opco={result['opco']}/dt={result['dt']}")
