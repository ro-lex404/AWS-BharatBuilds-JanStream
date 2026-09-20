"""
SQS Service - JanStream Resilient Ingestion
Handles SQS message buffering, backpressure control, and Dead Letter Queue (DLQ) routing.
"""

import os
import json
import logging
from typing import Dict, Any, List, Optional
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger("janstream.sqs")


class SQSService:
    def __init__(
        self,
        queue_url: Optional[str] = None,
        dlq_url: Optional[str] = None,
        region_name: str = "us-east-1"
    ):
        self.region_name = os.environ.get("AWS_DEFAULT_REGION", region_name)
        self.queue_url = queue_url or os.environ.get("SQS_QUEUE_URL", "")
        self.dlq_url = dlq_url or os.environ.get("SQS_DLQ_URL", "")
        self.sqs_client = boto3.client("sqs", region_name=self.region_name)
        
        # If queue_url is just a queue name (not full https URL), resolve it
        if self.queue_url and not self.queue_url.startswith("https://"):
            try:
                q_resp = self.sqs_client.get_queue_url(QueueName=self.queue_url)
                self.queue_url = q_resp.get("QueueUrl", self.queue_url)
            except Exception as e:
                logger.warning(f"Could not resolve queue URL for {self.queue_url}: {e}")
        
        # Local mock queue buffer for running locally without live AWS SQS credentials
        self._local_buffer: List[Dict[str, Any]] = []

    def send_upload_event(
        self,
        submission_id: str,
        s3_key: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Pushes an upload event to SQS for asynchronous worker processing."""
        payload = {
            "submission_id": submission_id,
            "s3_key": s3_key,
            "metadata": metadata or {},
            "attempt": 1
        }
        
        if not self.queue_url:
            # Fallback to local in-memory buffer if SQS_QUEUE_URL is not set
            self._local_buffer.append(payload)
            logger.info(f"[Local Mock SQS] Buffered submission {submission_id}")
            return {"message_id": f"mock-sqs-{submission_id}", "status": "QUEUED_LOCAL"}

        try:
            resp = self.sqs_client.send_message(
                QueueUrl=self.queue_url,
                MessageBody=json.dumps(payload),
                MessageAttributes={
                    "SubmissionId": {
                        "DataType": "String",
                        "StringValue": submission_id
                    }
                }
            )
            logger.info(f"Queued SQS message for {submission_id} (MessageId={resp['MessageId']})")
            return {"message_id": resp["MessageId"], "status": "QUEUED"}
        except ClientError as e:
            logger.error(f"Failed to push message to SQS: {e}")
            raise

    def receive_messages(self, max_messages: int = 5, wait_time_seconds: int = 10) -> List[Dict[str, Any]]:
        """Pulls messages from SQS for batch processing by the Lambda/Worker."""
        if not self.queue_url:
            # Pull from local buffer
            messages = []
            while self._local_buffer and len(messages) < max_messages:
                item = self._local_buffer.pop(0)
                messages.append({
                    "ReceiptHandle": f"mock-handle-{item['submission_id']}",
                    "Body": json.dumps(item),
                    "Attributes": {"ApproximateReceiveCount": "1"}
                })
            return messages

        try:
            response = self.sqs_client.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait_time_seconds,
                AttributeNames=["ApproximateReceiveCount"],
                MessageAttributeNames=["All"]
            )
            return response.get("Messages", [])
        except ClientError as e:
            logger.error(f"Failed to receive messages from SQS: {e}")
            return []

    def delete_message(self, receipt_handle: str) -> None:
        """Deletes processed message from SQS."""
        if not self.queue_url or receipt_handle.startswith("mock-handle"):
            return

        try:
            self.sqs_client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=receipt_handle)
        except ClientError as e:
            logger.error(f"Failed to delete SQS message: {e}")

    def send_to_dlq(self, payload: Dict[str, Any], failure_reason: str) -> None:
        """Routes permanently unprocessable messages to the Dead Letter Queue."""
        dlq_payload = {
            **payload,
            "failure_reason": failure_reason
        }
        if not self.dlq_url:
            logger.warning(f"[DLQ Alert] Permanent failure for {payload.get('submission_id')}: {failure_reason}")
            return

        try:
            self.sqs_client.send_message(
                QueueUrl=self.dlq_url,
                MessageBody=json.dumps(dlq_payload)
            )
            logger.error(f"Pushed to DLQ: {payload.get('submission_id')} - Reason: {failure_reason}")
        except ClientError as e:
            logger.critical(f"CRITICAL: Failed to push to DLQ: {e}")
