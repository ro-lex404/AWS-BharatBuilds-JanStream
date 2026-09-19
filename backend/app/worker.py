"""
Worker Pipeline - JanStream Resilient Ingestion
Executes asynchronous processing: SQS -> S3 fetch -> Bedrock Triage -> DynamoDB -> CloudWatch.
Can be invoked as an AWS Lambda function or run as a standalone worker process.
"""

import time
import json
import logging
from typing import Dict, Any, Optional

from app.services.s3_service import S3Service
from app.services.sqs_service import SQSService
from app.services.bedrock_service import BedrockService
from app.services.dynamodb_service import DynamoDBService
from app.services.metrics_service import MetricsService

logger = logging.getLogger("janstream.worker")


class JanStreamWorker:
    def __init__(
        self,
        s3_service: Optional[S3Service] = None,
        sqs_service: Optional[SQSService] = None,
        bedrock_service: Optional[BedrockService] = None,
        dynamodb_service: Optional[DynamoDBService] = None,
        metrics_service: Optional[MetricsService] = None
    ):
        self.s3 = s3_service or S3Service()
        self.sqs = sqs_service or SQSService()
        self.bedrock = bedrock_service or BedrockService()
        self.db = dynamodb_service or DynamoDBService()
        self.metrics = metrics_service or MetricsService()

    def process_single_submission(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Processes a single citizen submission through the triage pipeline."""
        submission_id = payload.get("submission_id")
        s3_key = payload.get("s3_key")
        metadata = payload.get("metadata", {})
        start_time = time.time()

        self.metrics.log_event("PROCESSING_STARTED", submission_id, details={"s3_key": s3_key})

        try:
            # 1. Read document text / content from S3 (or mock if local)
            try:
                raw_bytes = self.s3.get_object_bytes(s3_key)
                content_text = raw_bytes.decode("utf-8", errors="ignore")
            except Exception as s3_err:
                logger.warning(f"Using metadata payload for {submission_id} (S3: {s3_err})")
                content_text = metadata.get("description", "Citizen grievance document submitted.")

            filename = s3_key.split("/")[-1] if s3_key else "document.txt"

            # 2. Invoke Amazon Bedrock for Triage
            triage_start = time.time()
            triage_result = self.bedrock.triage_document(content_text, filename)
            triage_duration_ms = (time.time() - triage_start) * 1000

            # 3. Update State in DynamoDB
            updated_record = self.db.update_triage_result(submission_id, triage_result)

            # 4. Emit Observability Metrics
            total_duration_ms = (time.time() - start_time) * 1000
            self.metrics.record_metric("SubmissionsProcessed", 1.0)
            self.metrics.record_metric("TriageDurationMs", total_duration_ms, unit="Milliseconds")

            if triage_result.get("is_emergency"):
                self.metrics.record_metric("EmergencyIncidentsDetected", 1.0)
                self.metrics.log_event("EMERGENCY_ESCALATION", submission_id, details={
                    "category": triage_result.get("category"),
                    "urgency": triage_result.get("urgency_score"),
                    "summary": triage_result.get("administrative_summary")
                }, level="WARNING")

            self.metrics.log_event("PROCESSING_COMPLETED", submission_id, duration_ms=total_duration_ms)
            return updated_record

        except Exception as e:
            total_duration_ms = (time.time() - start_time) * 1000
            self.metrics.record_metric("ProcessingFailures", 1.0)
            self.metrics.log_event("PROCESSING_FAILED", submission_id, duration_ms=total_duration_ms, details={"error": str(e)}, level="ERROR")
            
            # Send to DLQ if permanently failed
            self.sqs.send_to_dlq(payload, failure_reason=str(e))
            raise

    def poll_and_process_batch(self, max_messages: int = 5) -> int:
        """Polls SQS queue and processes all incoming messages."""
        messages = self.sqs.receive_messages(max_messages=max_messages, wait_time_seconds=2)
        processed_count = 0

        for msg in messages:
            try:
                body = json.loads(msg["Body"])
                self.process_single_submission(body)
                self.sqs.delete_message(msg["ReceiptHandle"])
                processed_count += 1
            except Exception as e:
                logger.error(f"Error handling message: {e}")

        return processed_count


# AWS Lambda Entrypoint
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Standard AWS Lambda handler triggered by SQS Event Source Mapping.
    """
    worker = JanStreamWorker()
    processed = 0

    records = event.get("Records", [])
    for record in records:
        try:
            body = json.loads(record["body"])
            worker.process_single_submission(body)
            processed += 1
        except Exception as e:
            logger.error(f"Lambda batch record error: {e}")

    return {
        "statusCode": 200,
        "body": json.dumps({"processed_count": processed})
    }


if __name__ == "__main__":
    print("🚀 Starting JanStream Worker in polling mode...")
    worker = JanStreamWorker()
    count = worker.poll_and_process_batch()
    print(f"✅ Polling complete. Processed {count} items.")
