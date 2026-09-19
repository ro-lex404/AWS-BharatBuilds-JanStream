"""
Main REST API - JanStream Resilient Ingestion
FastAPI application providing endpoints for Presigned URLs, upload confirmation, and tracking.
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
import os
import logging

from app.services.s3_service import S3Service
from app.services.sqs_service import SQSService
from app.services.dynamodb_service import DynamoDBService
from app.services.metrics_service import MetricsService
from app.worker import JanStreamWorker

logger = logging.getLogger("janstream.api")

app = FastAPI(
    title="JanStream - Resilient Citizen Ingestion Engine",
    description="High-concurrency, fault-tolerant public service ingestion pipeline powered by AWS.",
    version="1.0.0"
)

# Enable CORS for Amplify frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Services
s3_svc = S3Service()
sqs_svc = SQSService()
db_svc = DynamoDBService()
metrics_svc = MetricsService()
worker = JanStreamWorker(s3_service=s3_svc, sqs_service=sqs_svc, dynamodb_service=db_svc, metrics_service=metrics_svc)


# Schemas
class PresignedUrlRequest(BaseModel):
    filename: str = Field(..., example="flood_complaint_ward12.jpg")
    content_type: str = Field(default="application/octet-stream", example="image/jpeg")
    citizen_phone: Optional[str] = Field(default=None, example="+919876543210")
    description: Optional[str] = Field(default=None, example="Live wire fallen on main road outside colony gate.")


class ConfirmUploadRequest(BaseModel):
    submission_id: str = Field(..., example="paste-submission-id-here")
    s3_key: Optional[str] = Field(default=None, example="uploads/.../file.jpg")
    description: Optional[str] = Field(default=None, example="Live wire fallen on main road.")


@app.get("/health")
def health_check():
    """Liveness probe for AWS App Runner or ALB."""
    return {
        "status": "HEALTHY",
        "service": "JanStream Ingestion Engine",
        "aws_region": os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    }


@app.post("/api/submissions/presigned-url")
def get_presigned_upload_url(req: PresignedUrlRequest):
    """
    Step 1: Client requests a secure S3 Presigned URL.
    Bypasses backend server bandwidth: client uploads directly to S3.
    """
    presigned_data = s3_svc.generate_presigned_upload_url(
        filename=req.filename,
        content_type=req.content_type
    )

    # Initialize DynamoDB record
    db_svc.create_submission_record(
        submission_id=presigned_data["submission_id"],
        filename=req.filename,
        s3_key=presigned_data["s3_key"],
        citizen_phone=req.citizen_phone
    )

    metrics_svc.record_metric("PresignedUrlsIssued", 1.0)
    metrics_svc.log_event("PRESIGNED_URL_ISSUED", presigned_data["submission_id"], details={"filename": req.filename})

    return {
        "submission_id": presigned_data["submission_id"],
        "upload_url": presigned_data["upload_url"],
        "fields": presigned_data["fields"],
        "s3_key": presigned_data["s3_key"],
        "expires_in_seconds": presigned_data["expires_in_seconds"]
    }


@app.post("/api/submissions/confirm-upload")
def confirm_upload_and_enqueue(req: ConfirmUploadRequest, background_tasks: BackgroundTasks):
    """
    Step 2: Client notifies backend that direct S3 upload completed.
    Pushes event to SQS queue to handle backpressure and triggers async processing.
    """
    record = db_svc.get_submission(req.submission_id)
    if not record:
        raise HTTPException(status_code=404, detail="Submission not found.")

    # Push to SQS
    s3_key = req.s3_key or record.get("s3_key", f"uploads/{req.submission_id}/document.bin")
    queue_result = sqs_svc.send_upload_event(
        submission_id=req.submission_id,
        s3_key=s3_key,
        metadata={"description": req.description or ""}
    )

    # Trigger background worker poll for immediate processing in demo mode
    background_tasks.add_task(worker.poll_and_process_batch, 5)

    metrics_svc.record_metric("SubmissionsQueued", 1.0)
    return {
        "submission_id": req.submission_id,
        "status": "QUEUED_FOR_TRIAGE",
        "queue_result": queue_result
    }


@app.get("/api/submissions/{submission_id}")
def get_submission_status(submission_id: str):
    """Step 3: Client polls submission status and Bedrock triage output."""
    record = db_svc.get_submission(submission_id)
    if not record:
        raise HTTPException(status_code=404, detail="Submission not found.")
    return record


@app.get("/api/submissions")
def list_submissions(limit: int = 15):
    """Lists recent submissions for administrative dashboard."""
    return db_svc.list_recent_submissions(limit=limit)


@app.post("/api/worker/process-queue")
def trigger_worker_batch():
    """Manually triggers worker batch processing (useful for demonstrations)."""
    count = worker.poll_and_process_batch(max_messages=10)
    return {"status": "SUCCESS", "messages_processed": count}
