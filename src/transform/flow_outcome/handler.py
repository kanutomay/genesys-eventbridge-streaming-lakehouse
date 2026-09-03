# Name: genesys_voice_flowoutcome_transformation.py
# Revision: 1.0.1
# Author: Mario May
# Date: 2025-12-17
# Description: This AWS Lambda function is designed to transform Genesys EventBridge flow outcome events for ingestion into Kinesis Firehose.
# Change log:
# Change log:
# v1.0.1 - 2025-12-17 by Mario May:
# - Added conversation_id field for clarity and parity with Genesys terminology.
# - Kept interaction_id field (legacy) for backward compatibility.
# v1.0.0 - 2025-12-16 by Mario May:
# - Initial release

"""
Kinesis Firehose Data Transformation Lambda
============================================
Transforms Genesys EventBridge flow.outcome events from JSON to Parquet-ready format

Purpose:
- Minimal transformation (keep near-raw data)
- Extract key metadata fields for partitioning and querying
- Add ingestion timestamp

Input: EventBridge JSON events (may be concatenated JSON Lines)
Output: Transformed JSON for Firehose → Parquet conversion

Environment Variables:
- OPCO: Operating company code (default: ACME)
"""

import json
import base64
import os
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
                transformed = transform_input_event(parsed_event, opco=OPCO, record_id=record_id, stats=stats)

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


def transform_input_event(event: Dict, opco: str = 'ACME', record_id: str = None, stats: Dict = None) -> Dict:
    """
    Transform a single Genesys input event

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

    # Extract timestamps
    event_time = event.get('time')
    genesys_timestamp = detail.get('timestamp')

    # Current time for ingestion timestamp
    ingest_time = datetime.now(timezone.utc).isoformat() # Ensure UTC timezone. Format as ISO 8601 (e.g., 2025-12-14T12:34:56.789Z)

    # Extract partition date from event time, fallback to ingest time if unavailable - date set using Zulu timezone
    dt = event_time[:10] if event_time and len(event_time) >= 10 else ingest_time[:10]

    # Extract critical fields for validation
    conversation_id = event_body.get('conversationId')
    participant_id = event_body.get('participantId')
    ani = event_body.get('ani')

    # Track missing critical fields
    if not conversation_id:
        if stats:
            stats['missing_conversation_id'] += 1
        structured_log("WARNING", "missing_conversation_id", None, record_id,
                       "Event missing conversationId (used as conversation_id)",
                       event_id=event.get('id'),
                       participant_id=participant_id)

    # Log successful extraction
    structured_log("INFO", "record_processing", conversation_id, record_id,
                   "Processing flow.outcome event",
                   participant_id=participant_id,
                   num_fields=len(event_body),
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
        # Core Event Data (Always Present in VOICE events)
        # ====================================================================
        'session_id': event_body.get('sessionId'),
        'media_type': event_body.get('mediaType'),  # Always VOICE
        'provider': event_body.get('provider'),
        'direction': event_body.get('direction'),
        'ani': ani,
        'dnis': event_body.get('dnis'),
        'flow_type': event_body.get('flowType'),
        'flow_id': event_body.get('flowId'),
        'division_id': event_body.get('divisionId'),
        'flow_version': event_body.get('flowVersion'),
        'flow_outcome_id': event_body.get('flowOutcomeId'),
        'flow_outcome_start_time': event_body.get('flowOutcomeStartTime'),
        'flow_outcome_end_time': event_body.get('flowOutcomeEndTime'),
        'flow_outcome_value': event_body.get('flowOutcomeValue'),
        'flow_milestones': event_body.get('flowMilestones', []),
        'conversation_external_contact_ids': event_body.get('conversationExternalContactIds', []),

        # ====================================================================
        # Optional Fields (NULLABLE - may not be present)
        # ====================================================================


        # ====================================================================
        # Raw Event (Keep full event for flexibility and debugging)
        # ====================================================================
        'raw_event': json.dumps(event),

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
                    num_fields=len(event_body),
                    partition_dt=transformed.get('dt'),
                    has_ani=bool(transformed.get('ani')))

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
        "id": "test-eventbridge-id",
        "detail-type": "v2.detail.events.conversation.{id}.flow.outcome",
        "source": "aws.partner/genesys.com/cloud/00000000-0000-0000-0000-000000000000/genesys_acme",
        "account": "123456789012",
        "time": "2025-12-12T15:02:30Z",
        "region": "us-east-1",
        "resources": [],
        "detail": {
            "topicName": "v2.detail.events.conversation.test-conversation-id.flow.outcome",
            "version": "2",
            "eventBody": {
            "eventTime": 1765551750107,
            "conversationId": "test-conversation-id",
            "participantId": "test-participant-id",
            "sessionId": "test-session-id",
            "mediaType": "VOICE",
            "provider": "Edge",
            "direction": "INBOUND",
            "ani": "tel:+15555550520",
            "dnis": "tel:+15555550000",
            "flowType": "INBOUNDCALL",
            "flowId": "test-flow-id",
            "divisionId": "test-division-id",
            "flowVersion": "7.0",
            "flowOutcomeId": "test-flow-outcome-id",
            "flowOutcomeStartTime": 1765551656458,
            "flowOutcomeEndTime": 1765551660853,
            "flowOutcomeValue": "SUCCESS",
            "flowMilestones": [
                {
                "milestoneId": "test-milestone-id-1",
                "milestoneTime": 1765551656458
                },
                {
                "milestoneId": "test-milestone-id-2",
                "milestoneTime": 1765551660851
                },
                {
                "milestoneId": "test-milestone-id-3",
                "milestoneTime": 1765551660852
                },
                {
                "milestoneId": "test-milestone-id-4",
                "milestoneTime": 1765551660883
                },
                {
                "milestoneId": "test-milestone-id-5",
                "milestoneTime": 1765551699797
                }
            ],
            "conversationExternalContactIds": [
                "test-external-contact-id"
            ]
            },
            "metadata": {
            "CorrelationId": "test-correlation-id"
            },
            "timestamp": "2025-12-12T15:02:30.395Z"
        }
    }

    result = transform_input_event(test_payload, opco='ACME')
    print(json.dumps(result, indent=2))
    print(f"\nPartition path: opco={result['opco']}/dt={result['dt']}")
