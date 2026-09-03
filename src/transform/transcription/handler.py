# Name: genesys_voice_transcription_transformation.py
# Revision: 1.0.1
# Author: Mario May
# Date: 2025-12-31
# Description: This AWS Lambda function transforms Genesys EventBridge transcription events for dual-output ingestion into Kinesis Firehose.
#              Outputs to TWO destinations: sessions table (metadata) and utterances table (transcripts as JSON string).
# Change log:
# v1.0.1 - 2025-12-31 by Mario May:
# - Changed transcripts storage from array<struct<>> to JSON string (Firehose schema compatibility)
# - Utterances table now stores transcripts as serialized JSON string
# - JSON parsing and exploding handled in Athena view layer using json_parse/UNNEST
# v1.0.0 - 2025-12-30 by Mario May:
# - Initial release with dual-output pattern (sessions + utterances)
# - Handles progressive transcription events (same conversation, multiple events)
# - Normalizes dialect field to lowercase for consistency
# - Stores words/decoratedWords arrays as JSON strings for storage efficiency

"""
Kinesis Firehose Data Transformation Lambda - Stream-Aware Dual Output
=======================================================================
Transforms Genesys EventBridge transcription events for EITHER:
1. Sessions stream: Session-level metadata (one record per event)
2. Utterances stream: Transcripts data as JSON string (zero or more records per event)

The Lambda uses the OUTPUT_TYPE environment variable to determine which stream
it's processing for, and returns ONLY the appropriate records for that stream.

Architecture:
  EventBridge Event
       ↓
  EventBridge Rule
    ↙    ↘
Target 1  Target 2
(Sessions)(Utterances)
    ↓        ↓
Firehose  Firehose
    ↓        ↓
Lambda    Lambda
(Sessions)(Utterances)
    ↓        ↓
1 Session  1 Record with Transcripts
Record     as JSON string

Purpose:
- Extract session lifecycle metadata (SESSION_ONGOING, SESSION_ENDED)
- Store transcripts array as JSON string (Athena views handle parsing/exploding)
- Normalize dialect field (es-US → es-us)
- Store words arrays as JSON strings for query flexibility
- Maintain near-raw data with key metadata fields

Input: EventBridge JSON events (may be concatenated JSON Lines)
Output: Transformed JSON stream for Firehose → Parquet conversion

Environment Variables:
- OPCO: Operating company code (default: ACME)
- OUTPUT_TYPE: Stream type - 'sessions' or 'utterances' (REQUIRED)
- ENABLE_DEBUG: Enable debug logging (default: 0)
"""

import json
import base64
import os
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Tuple

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
ENV = os.getenv("ENVIRONMENT", "production")
OPCO = os.environ.get('OPCO', 'ACME')
ENABLE_DEBUG = os.getenv("ENABLE_DEBUG", "0") == "1"
OUTPUT_TYPE = os.environ.get('OUTPUT_TYPE', 'sessions')  # 'sessions' or 'utterances'


def lambda_handler(event: Dict, context: Any) -> Dict:
    """
    Lambda handler for Firehose transformation with STREAM-AWARE OUTPUT

    This function processes transcription events and outputs records for EITHER:
    1. Sessions stream (OUTPUT_TYPE='sessions'): Session-level metadata only
    2. Utterances stream (OUTPUT_TYPE='utterances'): Transcripts as JSON string

    The OUTPUT_TYPE environment variable determines which records are returned.

    Args:
        event: Firehose transformation event with records
        context: Lambda context

    Returns:
        Transformed records for Firehose (filtered based on OUTPUT_TYPE)
    """
    # Set request ID for structured logging
    structured_log.request_id = context.aws_request_id

    # Processing statistics
    stats = {
        'total_input': len(event['records']),
        'successful_sessions': 0,
        'successful_utterances': 0,
        'total_utterances_extracted': 0,
        'events_without_transcripts': 0,
        'dropped_records': 0,
        'failed': 0,
        'missing_conversation_id': 0,
        'json_parse_errors': 0
    }

    structured_log("INFO", "lambda_start",
                   message=f"Processing {stats['total_input']} records (output_type={OUTPUT_TYPE})",
                   input_record_count=stats['total_input'],
                   output_type=OUTPUT_TYPE)

    # Process each record
    output_records = []

    for record in event['records']:
        record_id = record['recordId']

        try:
            # Decode base64 payload
            payload = base64.b64decode(record['data']).decode('utf-8')

            # Handle JSON Lines format (concatenated JSON objects)
            parsed_events = parse_json_lines(payload, record_id, stats)

            for parsed_event in parsed_events:
                # Transform into TWO outputs: sessions + utterances
                session_record, utterance_records = transform_transcription_event(
                    parsed_event,
                    opco=OPCO,
                    record_id=record_id,
                    stats=stats
                )

                # ================================================================
                # STREAM-AWARE OUTPUT: Return only records for THIS stream
                # ================================================================

                if OUTPUT_TYPE == 'sessions':
                    # Output 1: SESSION RECORD ONLY
                    session_data = json.dumps(session_record) + '\n'
                    encoded_session = base64.b64encode(session_data.encode('utf-8')).decode('utf-8')

                    output_records.append({
                        'recordId': record_id,
                        'result': 'Ok',
                        'data': encoded_session
                    })
                    stats['successful_sessions'] += 1

                elif OUTPUT_TYPE == 'utterances':
                    # Output 2: UTTERANCE RECORDS ONLY (return ONE record with ALL utterances as JSON string
                    if len(utterance_records) > 0:
                        # Create single record with all utterances as JSON string
                        utterances_data = {
                            'conversation_id': session_record['conversation_id'],
                            'interaction_id': session_record['interaction_id'], # Legacy field for compatibility
                            'communication_id': session_record['communication_id'],
                            'event_id': session_record['event_id'],
                            'event_time': session_record['event_time'],
                            'ingest_time': session_record['ingest_time'],
                            'num_transcripts': len(utterance_records),
                            'transcripts': json.dumps(utterance_records),  # ← JSON string of utterances array
                            'opco': session_record['opco'],
                            'dt': session_record['dt']
                        }

                        utterances_json = json.dumps(utterances_data) + '\n'
                        encoded_utterances = base64.b64encode(utterances_json.encode('utf-8')).decode('utf-8')

                        output_records.append({
                            'recordId': record_id,  # ← ONE record per input
                            'result': 'Ok',
                            'data': encoded_utterances
                        })
                        stats['successful_utterances'] += 1
                        stats['total_utterances_extracted'] += len(utterance_records)
                    else:
                        # No transcripts - drop the record (SESSION_ENDED events, etc.)
                        output_records.append({
                            'recordId': record_id,
                            'result': 'Dropped',
                            'data': record['data']
                        })
                        stats['dropped_records'] += 1
                        stats['events_without_transcripts'] += 1

        except Exception as e:
            stats['failed'] += 1

            structured_log("ERROR", "record_processing_failed", None, record_id,
                           f"Failed to process record: {str(e)}",
                           error=str(e),
                           error_type=type(e).__name__,
                           payload_length=len(payload) if 'payload' in locals() else 0)

            # Return original record with ProcessingFailed status
            output_records.append({
                'recordId': record_id,
                'result': 'ProcessingFailed',
                'data': record['data']
            })

    # Calculate success rates
    if OUTPUT_TYPE == 'sessions':
        success_rate = round(stats['successful_sessions'] / stats['total_input'] * 100, 2) if stats['total_input'] > 0 else 0
    else:
        success_rate = round(stats['successful_utterances'] / stats['total_input'] * 100, 2) if stats['total_input'] > 0 else 0

    avg_utterances_per_event = round(stats['total_utterances_extracted'] / max(stats['successful_sessions'], 1), 2)

    structured_log("INFO", "lambda_complete",
                   message=f"Lambda processing complete (output_type={OUTPUT_TYPE})",
                   output_type=OUTPUT_TYPE,
                   total_input=stats['total_input'],
                   successful_sessions=stats['successful_sessions'],
                   successful_utterances=stats['successful_utterances'],
                   total_utterances_extracted=stats['total_utterances_extracted'],
                   avg_utterances_per_event=avg_utterances_per_event,
                   events_without_transcripts=stats['events_without_transcripts'],
                   dropped_records=stats['dropped_records'],
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


def transform_transcription_event(event: Dict, opco: str = 'ACME', record_id: str = None,
                                  stats: Dict = None) -> Tuple[Dict, List[Dict]]:
    """
    Transform a single Genesys transcription event into TWO outputs:
    1. Session record: Metadata about the transcription session
    2. Utterance records: Individual utterance dictionaries (later serialized as JSON string)

    Note: The utterance records list is converted to JSON string in the lambda_handler
    before being sent to Firehose. This approach allows Parquet storage while enabling
    JSON parsing/exploding in Athena views.

    Args:
        event: Original EventBridge event
        opco: Operating company code (default: ACME)
        record_id: Record identifier for logging
        stats: Statistics dictionary to update

    Returns:
        Tuple of (session_record, list_of_utterance_records)
    """
    detail = event.get('detail', {})
    event_body = detail.get('eventBody', {})
    metadata = detail.get('metadata', {})

    # Extract timestamps
    event_time = event.get('time')
    genesys_timestamp = detail.get('timestamp')
    ingest_time = datetime.now(timezone.utc).isoformat()  # Ensure UTC timezone. Format as ISO 8601 (e.g., 2025-12-14T12:34:56.789Z)

    # Extract partition date from event time, fallback to ingest time if unavailable - date set using Zulu timezone
    dt = event_time[:10] if event_time and len(event_time) >= 10 else ingest_time[:10]

    # Extract key identifiers
    conversation_id = event_body.get('conversationId')
    communication_id = event_body.get('communicationId')
    session_start_ms = event_body.get('sessionStartTimeMs')

    # Extract status and calculate boolean flags
    status = event_body.get('status', {}).get('status')
    is_session_ended = (status == 'SESSION_ENDED')
    is_session_ongoing = (status == 'SESSION_ONGOING')

    # Track missing conversation_id
    if not conversation_id:
        if stats:
            stats['missing_conversation_id'] += 1

        structured_log("WARNING", "missing_conversation_id", None, record_id,
                       "Event missing conversationId",
                       event_id=event.get('id'))

    # Log successful extraction
    has_transcripts = bool(event_body.get('transcripts'))
    num_transcripts = len(event_body.get('transcripts', []))

    structured_log("INFO", "record_processing", conversation_id, record_id,
                   f"Processing transcription event (session + {num_transcripts} utterances)",
                   communication_id=communication_id,
                   status=event_body.get('status', {}).get('status'),
                   has_transcripts=has_transcripts,
                   num_transcripts=num_transcripts)

    # ========================================================================
    # BUILD SESSION RECORD
    # ========================================================================
    session_record = {
        # ====================================================================
        # Metadata & Identifiers
        # ====================================================================
        'event_id': event.get('id'),
        'event_time': event_time,
        'ingest_time': ingest_time,
        'conversation_id': conversation_id,
        'interaction_id': conversation_id, # Legacy field for compatibility
        'communication_id': communication_id,

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
        # Genesys Event Metadata
        # ====================================================================
        'topic_name': detail.get('topicName'),
        'topic_version': detail.get('version'),
        'correlation_id': metadata.get('CorrelationId'),
        'genesys_timestamp': genesys_timestamp,
        'genesys_event_time': event_body.get('eventTime'),

        # ====================================================================
        # Session-Level Data (Always Present)
        # ====================================================================
        'organization_id': event_body.get('organizationId'),
        'session_start_time_ms': session_start_ms,
        'transcription_start_time_ms': event_body.get('transcriptionStartTimeMs'),

        # Status information
        'status': status,
        'status_offset_ms': event_body.get('status', {}).get('offsetMs'),
        'is_session_ended': is_session_ended,
        'is_session_ongoing': is_session_ongoing,

        # Transcript metadata
        'has_transcripts': has_transcripts,
        'num_transcripts': num_transcripts,

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

    # ========================================================================
    # BUILD UTTERANCE RECORDS (build list of utterance dictionaries)
    # ========================================================================
    utterance_records = []

    if has_transcripts:
        for transcript in event_body['transcripts']:
            # Extract first alternative (typically only one alternative exists)
            alternative = transcript['alternatives'][0] if transcript.get('alternatives') else {}

            # Normalize dialect to lowercase for consistency (es-US → es-us)
            dialect = transcript.get('dialect', '').lower()

            # Convert words arrays to JSON strings for storage efficiency
            words_json = json.dumps(alternative.get('words', [])) if alternative.get('words') else None
            decorated_words_json = json.dumps(alternative.get('decoratedWords', [])) if alternative.get('decoratedWords') else None

            utterance_record = {
                # ================================================================
                # Utterance Identification
                # ================================================================
                'utterance_id': transcript.get('utteranceId'),
                'is_final': transcript.get('isFinal'),
                'channel': transcript.get('channel'),  # INTERNAL or EXTERNAL

                # ================================================================
                # Transcription Content
                # ================================================================
                'confidence': alternative.get('confidence'),
                'offset_ms': alternative.get('offsetMs'),
                'duration_ms': alternative.get('durationMs'),
                'transcript': alternative.get('transcript'),
                'decorated_transcript': alternative.get('decoratedTranscript'),

                # ================================================================
                # Word-Level Data (stored as JSON strings for flexibility)
                # ================================================================
                'words': words_json,
                'decorated_words': decorated_words_json,

                # ================================================================
                # Engine Information
                # ================================================================
                'engine_provider': transcript.get('engineProvider'),
                'engine_id': transcript.get('engineId'),
                'engine_name': transcript.get('engineName'),  # May be null
                'dialect': dialect,  # Normalized to lowercase

                # ================================================================
                # Feature Flags
                # ================================================================
                'agent_assist_enabled': transcript.get('agentAssistEnabled'),
                'agent_assistant_id': transcript.get('agentAssistantId'),  # May be null
                'voice_transcription_enabled': transcript.get('voiceTranscriptionEnabled'),
                'speech_text_analytics_program_id': transcript.get('speechTextAnalyticsProgramId')  # May be null
            }

            utterance_records.append(utterance_record)

    # Debug logging
    if ENABLE_DEBUG:
        structured_log("INFO", "transformed_record_debug", conversation_id, record_id,
                       "Transformed records summary",
                       event_id=session_record.get('event_id'),
                       session_status=session_record.get('status'),
                       num_utterances=len(utterance_records),
                       partition_dt=dt,
                       has_transcripts=has_transcripts)

    return session_record, utterance_records


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
    Local testing with sample transcription events
    """

    # Test Case 1: SESSION_ONGOING with transcripts
    test_payload_with_transcripts = {
        "version": "0",
        "id": "test-eventbridge-id-1",
        "detail-type": "v2.conversations.{id}.transcription",
        "source": "aws.partner/genesys.com/cloud/00000000-0000-0000-0000-000000000000/genesys_acme",
        "account": "123456789012",
        "time": "2025-12-10T08:44:30Z",
        "region": "us-east-1",
        "resources": [],
        "detail": {
            "topicName": "v2.conversations.test-conversation-id-1.transcription",
            "version": "2",
            "eventBody": {
                "eventTime": "2025-12-10T08:44:30.264Z",
                "organizationId": "00000000-0000-0000-0000-000000000000",
                "conversationId": "test-conversation-id-1",
                "communicationId": "test-communication-id-1",
                "sessionStartTimeMs": 1765356240994,
                "transcriptionStartTimeMs": 1765356240942,
                "transcripts": [
                    {
                        "utteranceId": "test-utterance-id-1",
                        "isFinal": True,
                        "channel": "INTERNAL",
                        "alternatives": [
                            {
                                "confidence": 0.985,
                                "offsetMs": 2300,
                                "durationMs": 6980,
                                "transcript": "gracias por llamar",
                                "words": [
                                    {"confidence": 1.0, "offsetMs": 2300, "durationMs": 40, "word": "gracias"},
                                    {"confidence": 0.999, "offsetMs": 2659, "durationMs": 40, "word": "por"},
                                    {"confidence": 0.999, "offsetMs": 2898, "durationMs": 40, "word": "llamar"}
                                ],
                                "decoratedTranscript": "gracias por llamar",
                                "decoratedWords": [
                                    {"confidence": 1.0, "offsetMs": 2300, "durationMs": 40, "word": "gracias"}
                                ]
                            }
                        ],
                        "engineProvider": "GENESYS",
                        "engineId": "r2d2",
                        "dialect": "es-US",
                        "agentAssistEnabled": False,
                        "voiceTranscriptionEnabled": True
                    },
                    {
                        "utteranceId": "test-utterance-id-2",
                        "isFinal": True,
                        "channel": "EXTERNAL",
                        "alternatives": [
                            {
                                "confidence": 0.864,
                                "offsetMs": 23280,
                                "durationMs": 240,
                                "transcript": "español",
                                "words": [
                                    {"confidence": 0.864, "offsetMs": 23280, "durationMs": 240, "word": "español"}
                                ],
                                "decoratedTranscript": "español",
                                "decoratedWords": [
                                    {"confidence": 0.86, "offsetMs": 23280, "durationMs": 240, "word": "español"}
                                ]
                            }
                        ],
                        "engineProvider": "GENESYS",
                        "engineId": "r2d2",
                        "dialect": "es-us",
                        "agentAssistEnabled": False,
                        "voiceTranscriptionEnabled": True
                    }
                ],
                "status": {
                    "offsetMs": 23280,
                    "status": "SESSION_ONGOING"
                }
            },
            "metadata": {
                "CorrelationId": "test-correlation-id-1"
            },
            "timestamp": "2025-12-10T08:44:30.264Z"
        }
    }

    # Test Case 2: SESSION_ENDED (no transcripts)
    test_payload_session_ended = {
        "version": "0",
        "id": "test-eventbridge-id-2",
        "detail-type": "v2.conversations.{id}.transcription",
        "source": "aws.partner/genesys.com/cloud/00000000-0000-0000-0000-000000000000/genesys_acme",
        "account": "123456789012",
        "time": "2025-12-10T08:45:01Z",
        "region": "us-east-1",
        "resources": [],
        "detail": {
            "topicName": "v2.conversations.test-conversation-id-2.transcription",
            "version": "2",
            "eventBody": {
                "eventTime": "2025-12-10T08:45:01.912Z",
                "organizationId": "00000000-0000-0000-0000-000000000000",
                "conversationId": "test-conversation-id-2",
                "communicationId": "test-communication-id-2",
                "sessionStartTimeMs": 1765356238647,
                "transcriptionStartTimeMs": 1765356238625,
                "status": {
                    "offsetMs": 63020,
                    "status": "SESSION_ENDED"
                }
            },
            "metadata": {
                "CorrelationId": "test-correlation-id-2"
            },
            "timestamp": "2025-12-10T08:45:01.912Z"
        }
    }

    print("=" * 80)
    print("TEST CASE 1: Event with transcripts (2 utterances)")
    print("=" * 80)
    session1, utterances1 = transform_transcription_event(test_payload_with_transcripts, opco='ACME')

    print("\nSESSION RECORD:")
    print(json.dumps(session1, indent=2))

    print(f"\n\nUTTERANCE RECORDS ({len(utterances1)} total):")
    for idx, utt in enumerate(utterances1, 1):
        print(f"\n--- Utterance {idx} ---")
        print(json.dumps(utt, indent=2))

    print("\n" + "=" * 80)
    print("TEST CASE 2: Session ended (no transcripts)")
    print("=" * 80)
    session2, utterances2 = transform_transcription_event(test_payload_session_ended, opco='ACME')

    print("\nSESSION RECORD:")
    print(json.dumps(session2, indent=2))

    print(f"\n\nUTTERANCE RECORDS ({len(utterances2)} total):")
    if not utterances2:
        print("(None - session ended without new transcripts)")
        print("  → This record would be DROPPED in utterances stream")

    print(f"\n\n{'=' * 80}")
    print("STREAM-AWARE OUTPUT SIMULATION")
    print(f"{'=' * 80}")
    print(f"\nSessions Stream (OUTPUT_TYPE='sessions'):")
    print(f"  • Test 1: Returns 1 session record")
    print(f"  • Test 2: Returns 1 session record")
    print(f"\nUtterances Stream (OUTPUT_TYPE='utterances'):")
    print(f"  • Test 1: Returns 1 record with {len(utterances1)} utterances as JSON string")
    print(f"  • Test 2: Returns 0 records (DROPPED - no transcripts)")

    print(f"\n\nPARTITION PATHS:")
    print(f"Sessions:   opco={session1['opco']}/dt={session1['dt']}")
    print(f"Utterances: opco={session1['opco']}/dt={session1['dt']}")

    print(f"\n\nJSON STRING SAMPLE (first 200 chars):")
    print(f"transcripts: {json.dumps(utterances1)[:200]}...")
