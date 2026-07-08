"""
Lambda function to automatically track data lineage on S3 PutObject events.

Triggered by EventBridge when objects are uploaded to Bronze layer buckets.
Writes ingestion lineage annotations with source metadata.
"""

import json
import os
import re
import boto3
from datetime import datetime, timezone
from typing import Dict, Any

# Initialize clients
s3_client = boto3.client('s3')

# Environment variables
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
ANNOTATION_NAME = os.environ.get('ANNOTATION_NAME', 'lineage')

# Cache operator ARN (never changes within a Lambda deployment)
_OPERATOR_ARN = None


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for S3 PutObject events.

    Args:
        event: EventBridge event containing S3 object details
        context: Lambda context

    Returns:
        Response dict with status and message
    """
    try:
        log_event(event)

        # Parse S3 event details
        detail = event.get('detail', {})
        bucket_name = detail.get('bucket', {}).get('name')
        object_key = detail.get('object', {}).get('key')
        object_size = detail.get('object', {}).get('size', 0)

        if not bucket_name or not object_key:
            return error_response("Missing bucket or object key in event")

        log_info(f"Processing: s3://{bucket_name}/{object_key}")

        # Get object metadata
        try:
            head_response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
            content_type = head_response.get('ContentType', 'unknown')
            metadata = head_response.get('Metadata', {})
        except Exception as e:
            log_error(f"Failed to get object metadata: {str(e)}")
            content_type = 'unknown'
            metadata = {}

        # Build ingestion lineage annotation
        lineage_data = build_ingestion_lineage(
            bucket=bucket_name,
            key=object_key,
            size=object_size,
            content_type=content_type,
            user_metadata=metadata
        )

        # Write lineage annotation
        success = write_lineage_annotation(
            bucket=bucket_name,
            key=object_key,
            lineage_data=lineage_data
        )

        # Write consent annotation (tracks which patients' data is in this object)
        consent_data = build_consent_annotation(
            bucket=bucket_name,
            key=object_key,
            user_metadata=metadata,
            data_classification=lineage_data.get('data_classification', 'PHI')
        )
        write_annotation(bucket_name, object_key, 'consent', consent_data)

        if success:
            log_info(f"✓ Lineage + consent annotations written")
            return success_response(f"Lineage tracked for s3://{bucket_name}/{object_key}")
        else:
            return error_response(f"Failed to write annotation for s3://{bucket_name}/{object_key}")

    except Exception as e:
        log_error(f"Unhandled exception: {str(e)}")
        # T-07: Don't return raw exception to caller (may contain PHI)
        return error_response("Lineage tracking failed")


def build_ingestion_lineage(
    bucket: str,
    key: str,
    size: int,
    content_type: str,
    user_metadata: Dict[str, str]
) -> Dict[str, Any]:
    """
    Build ingestion lineage metadata.

    Args:
        bucket: S3 bucket name
        key: S3 object key
        size: Object size in bytes
        content_type: Object content type
        user_metadata: User-defined metadata from S3 object

    Returns:
        Dict containing lineage metadata
    """
    # Determine source system from metadata or key pattern
    source_system = user_metadata.get('source_system', extract_source_system(key))

    # Determine file format from extension
    file_format = determine_file_format(key, content_type)

    # Estimate record count (placeholder - real implementation would parse file)
    record_count = estimate_record_count(size, file_format)

    # Get caller identity
    operator_arn = get_operator_arn()

    # Determine data classification
    data_classification = user_metadata.get('data_classification', 'PHI')

    # Compliance tags
    compliance_tags = ['HIPAA', 'PII']
    if 'financial' in key.lower():
        compliance_tags.append('SOX')
    if 'eu' in key.lower() or 'europe' in key.lower():
        compliance_tags.append('GDPR')

    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

    return {
        "lineage_type": "ingestion",
        "provenance": {
            "target": f"s3://{bucket}/{key}",
            "recorded": timestamp,
            "activity": "ingestion",
            "agent": {
                "who": operator_arn,
                "role": "assembler"
            },
            "entity": {
                "role": "source",
                "what": source_system
            }
        },
        "source_system": source_system,
        "timestamp": timestamp,
        "operator": operator_arn,
        "record_count": record_count,
        "file_format": file_format,
        "file_size_bytes": size,
        "content_type": content_type,
        "data_classification": data_classification,
        "compliance_tags": compliance_tags,
        "s3_uri": f"s3://{bucket}/{key}"
    }


def write_lineage_annotation(
    bucket: str,
    key: str,
    lineage_data: Dict[str, Any]
) -> bool:
    """
    Write lineage annotation to S3 object.

    Args:
        bucket: S3 bucket name
        key: S3 object key
        lineage_data: Lineage metadata dict

    Returns:
        True if successful, False otherwise
    """
    try:
        annotation_value = json.dumps(lineage_data, indent=2)

        s3_client.put_object_annotation(
            Bucket=bucket,
            Key=key,
            AnnotationName=ANNOTATION_NAME,
            AnnotationPayload=annotation_value.encode('utf-8')
        )

        log_info(f"Lineage annotation written via S3 Annotations API")

        return True

    except Exception as e:
        log_error(f"Failed to write annotation: {str(e)}")
        return False


def write_annotation(bucket: str, key: str, name: str, data: Dict[str, Any]) -> bool:
    """Write a named annotation to an S3 object."""
    try:
        s3_client.put_object_annotation(
            Bucket=bucket,
            Key=key,
            AnnotationName=name,
            AnnotationPayload=json.dumps(data, indent=2).encode('utf-8')
        )
        return True
    except Exception as e:
        log_error(f"Failed to write {name} annotation: {str(e)}")
        return False


def build_consent_annotation(
    bucket: str,
    key: str,
    user_metadata: Dict[str, str],
    data_classification: str
) -> Dict[str, Any]:
    """
    Build consent tracking annotation.

    Tracks which patients' data is contained in this object,
    enabling GDPR right-to-erasure queries across the data lake.
    """
    # Patient IDs from metadata (set by upstream ingestion systems)
    # Supports comma or pipe delimiter (pipe needed when passing via AWS CLI --metadata)
    raw_ids = user_metadata.get('patient_ids', '')
    patient_ids = [pid.strip() for pid in re.split(r'[,|]', raw_ids) if pid.strip()] if raw_ids else []

    return {
        "consent_type": "data_subject_tracking",
        "timestamp": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        "data_classification": data_classification,
        "patient_ids": patient_ids,
        "consent_basis": user_metadata.get('consent_basis', 'legitimate_interest'),
        "retention_period": user_metadata.get('retention_period', '7_years'),
        "erasure_eligible": data_classification in ('PII', 'PHI'),
        "s3_uri": f"s3://{bucket}/{key}"
    }


def extract_source_system(key: str) -> str:
    """Extract source system from object key pattern."""
    key_lower = key.lower()

    if 'ehr' in key_lower or 'epic' in key_lower:
        return 'EHR_Epic'
    elif 'lab' in key_lower or 'laboratory' in key_lower:
        return 'Laboratory'
    elif 'imaging' in key_lower or 'dicom' in key_lower:
        return 'Imaging_PACS'
    elif 'claims' in key_lower:
        return 'Claims_EDI'
    elif 'pharmacy' in key_lower:
        return 'Pharmacy'
    else:
        return 'Unknown'


def determine_file_format(key: str, content_type: str) -> str:
    """Determine file format from extension and content type."""
    extension = key.split('.')[-1].lower() if '.' in key else ''

    format_map = {
        'hl7': 'HL7',
        'json': 'JSON',
        'fhir': 'FHIR',
        'csv': 'CSV',
        'parquet': 'Parquet',
        'xml': 'XML',
        'dcm': 'DICOM',
        'edi': 'EDI'
    }

    return format_map.get(extension, content_type.split('/')[-1].upper() if content_type else 'Unknown')


def estimate_record_count(size_bytes: int, file_format: str) -> int:
    """
    Estimate record count based on file size.

    Rough estimates:
    - HL7: ~2KB per message
    - JSON/FHIR: ~5KB per record
    - CSV: ~1KB per record
    - Parquet: ~500 bytes per record (compressed)
    """
    format_avg_size = {
        'HL7': 2000,
        'JSON': 5000,
        'FHIR': 5000,
        'CSV': 1000,
        'Parquet': 500,
        'XML': 3000
    }

    avg_size = format_avg_size.get(file_format, 2000)
    estimated = max(1, size_bytes // avg_size)

    return estimated


def get_operator_arn() -> str:
    """Get ARN of current execution role (cached after first call)."""
    global _OPERATOR_ARN
    if _OPERATOR_ARN:
        return _OPERATOR_ARN
    try:
        sts_client = boto3.client('sts')
        identity = sts_client.get_caller_identity()
        _OPERATOR_ARN = identity['Arn']
        return _OPERATOR_ARN
    except Exception as e:
        log_error(f"Failed to get caller identity: {str(e)}")
        return "arn:aws:iam::unknown:role/unknown"


def log_event(event: Dict[str, Any]) -> None:
    """Log incoming event for debugging."""
    if LOG_LEVEL == 'DEBUG':
        print(f"Event: {json.dumps(event)}")


def log_info(message: str) -> None:
    """Log info message."""
    print(f"[INFO] {message}")


def log_error(message: str) -> None:
    """
    Log error message with PHI redaction.

    T-07 Mitigation: Redact PHI from error messages before logging.
    """
    redacted = redact_phi(message)
    print(f"[ERROR] {redacted}")


def redact_phi(message: str) -> str:
    """
    Redact Protected Health Information from log messages.

    Redacts:
    - HL7 patient identifiers (PID segment)
    - Social Security Numbers (###-##-####)
    - Medical Record Numbers (MRN patterns)
    - Patient names in common formats
    - Date of birth patterns

    Args:
        message: Original log message potentially containing PHI

    Returns:
        Sanitized message with PHI replaced by [REDACTED] markers
    """
    # Redact SSN patterns
    message = re.sub(r'\b\d{3}-\d{2}-\d{4}\b', '[SSN-REDACTED]', message)
    message = re.sub(r'\b\d{9}\b', '[SSN-REDACTED]', message)

    # Redact HL7 PID segment (contains patient identifiers)
    message = re.sub(r'PID\|[^|]*\|[^|]*\|[^\r\n]*', 'PID|[REDACTED]', message)

    # Redact MRN patterns (common formats: MRN12345, MR-12345, etc)
    message = re.sub(r'\bMRN?[-:\s]?\d+\b', '[MRN-REDACTED]', message, flags=re.IGNORECASE)

    # Redact DOB patterns (MM/DD/YYYY only — ISO dates like 2026-06-18 are too
    # common in S3 keys and timestamps to redact without context)
    message = re.sub(r'\b\d{2}/\d{2}/\d{4}\b', '[DOB-REDACTED]', message)

    # Redact common name patterns in HL7 (LastName^FirstName)
    message = re.sub(r'\b[A-Z][a-z]+\^[A-Z][a-z]+', '[NAME-REDACTED]', message)

    # Redact phone numbers
    message = re.sub(r'\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b', '[PHONE-REDACTED]', message)

    # Redact email addresses
    message = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '[EMAIL-REDACTED]', message)

    return message


def success_response(message: str) -> Dict[str, Any]:
    """Build success response."""
    return {
        'statusCode': 200,
        'body': json.dumps({'message': message})
    }


def error_response(message: str) -> Dict[str, Any]:
    """Build error response."""
    return {
        'statusCode': 500,
        'body': json.dumps({'error': message})
    }
