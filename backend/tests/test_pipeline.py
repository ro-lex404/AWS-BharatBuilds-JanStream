"""
Automated Test Suite - JanStream Pipeline
Used by GitHub Actions CI/CD to verify ingestion, queueing, triage, and state transitions.
"""

import pytest
from app.services.s3_service import S3Service
from app.services.sqs_service import SQSService
from app.services.bedrock_service import BedrockService
from app.services.dynamodb_service import DynamoDBService
from app.services.metrics_service import MetricsService
from app.worker import JanStreamWorker


@pytest.fixture
def mock_pipeline():
    """Sets up an isolated, in-memory pipeline for unit testing."""
    s3 = S3Service(bucket_name="test-bucket")
    sqs = SQSService()  # Will use in-memory mock queue buffer
    bedrock = BedrockService()  # Uses rule-based triage heuristic in test
    db = DynamoDBService()  # Uses local in-memory dict
    metrics = MetricsService()
    worker = JanStreamWorker(s3, sqs, bedrock, db, metrics)

    return {
        "s3": s3,
        "sqs": sqs,
        "bedrock": bedrock,
        "db": db,
        "metrics": metrics,
        "worker": worker
    }


def test_s3_presigned_url_generation(mock_pipeline):
    s3 = mock_pipeline["s3"]
    # Verify presigned URL payload structure
    res = s3.generate_presigned_upload_url("complaint.jpg", content_type="image/jpeg")
    assert "submission_id" in res
    assert "upload_url" in res
    assert "fields" in res
    assert res["s3_key"].startswith("uploads/")


def test_triage_routine_grievance(mock_pipeline):
    bedrock = mock_pipeline["bedrock"]
    text = "There is a major pothole on 4th cross road causing traffic slow down."
    res = bedrock.triage_document(text, "pothole.txt")
    
    assert res["category"] == "Road Infrastructure"
    assert res["is_emergency"] is False
    assert res["urgency_score"] <= 8


def test_triage_emergency_hazard_escalation(mock_pipeline):
    bedrock = mock_pipeline["bedrock"]
    text = "ALERT: Heavy storm caused live electric power wire to fall into water puddle outside primary school. Sparking violently!"
    res = bedrock.triage_document(text, "emergency_wire.txt")
    
    assert res["category"] == "Electrical Hazard"
    assert res["is_emergency"] is True
    assert res["urgency_score"] >= 9


def test_end_to_end_worker_pipeline(mock_pipeline):
    sqs = mock_pipeline["sqs"]
    db = mock_pipeline["db"]
    worker = mock_pipeline["worker"]

    sub_id = "test-sub-101"
    filename = "live_wire_hazard.txt"
    s3_key = f"uploads/{sub_id}/{filename}"

    # 1. Initialize record
    db.create_submission_record(sub_id, filename, s3_key, citizen_phone="+919876543210")
    initial_rec = db.get_submission(sub_id)
    assert initial_rec["status"] == "RECEIVED"

    # 2. Queue event
    sqs.send_upload_event(sub_id, s3_key, metadata={"description": "Live electric cable snapped on street."})

    # 3. Process via worker
    processed = worker.poll_and_process_batch(max_messages=1)
    assert processed == 1

    # 4. Verify state updated to ESCALATED_EMERGENCY
    final_rec = db.get_submission(sub_id)
    assert final_rec["status"] == "ESCALATED_EMERGENCY"
    assert final_rec["is_emergency"] is True
    assert final_rec["urgency_score"] >= 9
    assert final_rec["triage_details"] is not None
