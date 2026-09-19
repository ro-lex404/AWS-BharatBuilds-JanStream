"""
JanStream Native AWS Lambda Entrypoint
Zero-dependency, pure-Python router for AWS Lambda Function URL.
Runs on standard Amazon Linux Python 3.11/3.12 without any binary compilation issues.
"""

import os
import json
import logging
from typing import Dict, Any

from app.services.s3_service import S3Service
from app.services.sqs_service import SQSService
from app.services.bedrock_service import BedrockService
from app.services.dynamodb_service import DynamoDBService
from app.services.metrics_service import MetricsService
from app.worker import JanStreamWorker

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize services once per Lambda container (Warm start reuse)
s3_svc = S3Service()
sqs_svc = SQSService()
db_svc = DynamoDBService()
metrics_svc = MetricsService()
worker = JanStreamWorker(s3_service=s3_svc, sqs_service=sqs_svc, dynamodb_service=db_svc, metrics_service=metrics_svc)

CORS_HEADERS = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type,Authorization,X-Requested-With",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS"
}


from decimal import Decimal

def build_response(status_code: int, data: Any) -> Dict[str, Any]:
    def decimal_default(obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return str(obj)

    return {
        "statusCode": status_code,
        "headers": CORS_HEADERS,
        "body": json.dumps(data, default=decimal_default)
    }


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Handles HTTP requests from AWS Lambda Function URL."""
    # 1. Extract HTTP method and path
    http_ctx = event.get("requestContext", {}).get("http", {})
    method = http_ctx.get("method", "GET").upper()
    raw_path = event.get("rawPath", "/").rstrip("/")

    # Handle CORS Preflight
    if method == "OPTIONS":
        return build_response(200, {"status": "CORS_OK"})

    # Parse body if present
    body = {}
    raw_body = event.get("body")
    if raw_body:
        try:
            body = json.loads(raw_body)
        except Exception:
            body = {}

    logger.info(f"Incoming {method} {raw_path}")

    try:
        # Route: GET /health
        if (raw_path == "/health" or raw_path == "") and method == "GET":
            return build_response(200, {
                "status": "HEALTHY",
                "service": "JanStream Ingestion Engine (AWS Lambda Native)",
                "aws_region": os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
            })

        # Route: POST /api/submissions/presigned-url
        if raw_path == "/api/submissions/presigned-url" and method == "POST":
            filename = body.get("filename", "citizen_evidence.txt")
            content_type = body.get("content_type", "text/plain")
            phone = body.get("citizen_phone", "N/A")

            presigned_data = s3_svc.generate_presigned_put_url(filename, content_type)
            db_svc.create_submission_record(
                submission_id=presigned_data["submission_id"],
                filename=filename,
                s3_key=presigned_data["s3_key"],
                citizen_phone=phone
            )
            metrics_svc.record_metric("PresignedUrlsIssued", 1.0)
            return build_response(200, presigned_data)

        # Route: POST /api/submissions/confirm-upload
        if raw_path == "/api/submissions/confirm-upload" and method == "POST":
            submission_id = body.get("submission_id")
            if not submission_id:
                return build_response(400, {"error": "Missing submission_id"})

            record = db_svc.get_submission(submission_id)
            s3_key = body.get("s3_key") or (record.get("s3_key") if record else f"uploads/{submission_id}/evidence.txt")
            desc = body.get("description", "")
            file_content = body.get("file_content")

            # Physical S3 Persistence Guarantee: Ensure file is written to S3 bucket
            s3_persisted = False
            try:
                s3_svc.s3_client.head_object(Bucket=s3_svc.bucket_name, Key=s3_key)
                s3_persisted = True
                logger.info(f"File {s3_key} already exists in S3.")
            except Exception:
                # If not uploaded by client (e.g. browser CORS prevented direct PUT), write directly to S3
                logger.info(f"Writing file {s3_key} directly to S3 bucket {s3_svc.bucket_name}")
                data_bytes = (
                    file_content.encode("utf-8") if file_content else
                    f"JANSTREAM CITIZEN EVIDENCE\nSubmission ID: {submission_id}\nIncident: {desc}\n".encode("utf-8")
                )
                s3_persisted = s3_svc.put_object(s3_key, data_bytes, content_type="text/plain")

            # Push to SQS
            queue_result = sqs_svc.send_upload_event(
                submission_id=submission_id,
                s3_key=s3_key,
                metadata={"description": desc}
            )

            # Process immediately in worker for demo response
            worker.process_single_submission({
                "submission_id": submission_id,
                "s3_key": s3_key,
                "metadata": {"description": desc}
            })

            metrics_svc.record_metric("SubmissionsQueued", 1.0)
            return build_response(200, {
                "submission_id": submission_id,
                "status": "QUEUED_AND_PROCESSED",
                "s3_persisted": s3_persisted,
                "s3_key": s3_key,
                "bucket": s3_svc.bucket_name,
                "queue_result": queue_result
            })

        # Route: GET /api/submissions/{id}
        if raw_path.startswith("/api/submissions/") and method == "GET":
            sub_id = raw_path.split("/")[-1]
            if sub_id != "submissions":
                record = db_svc.get_submission(sub_id)
                if not record:
                    return build_response(404, {"error": "Submission not found"})
                return build_response(200, record)

        # Route: GET /api/submissions (list recent)
        if raw_path == "/api/submissions" and method == "GET":
            recent = db_svc.list_recent_submissions(limit=15)
            return build_response(200, recent)

        # Route: POST /api/worker/process-queue
        if raw_path == "/api/worker/process-queue" and method == "POST":
            count = worker.poll_and_process_batch(max_messages=10)
            return build_response(200, {"status": "SUCCESS", "messages_processed": count})

        # Route Not Found
        return build_response(404, {"error": f"Route not found: {method} {raw_path}"})

    except Exception as e:
        logger.error(f"Error handling request: {e}", exc_info=True)
        return build_response(500, {"error": str(e)})
