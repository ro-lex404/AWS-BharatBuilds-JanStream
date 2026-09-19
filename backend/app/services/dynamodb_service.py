"""
DynamoDB Service - JanStream Resilient Ingestion
Handles submission lifecycle states, audit trails, and fast query lookups.
"""

import os
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger("janstream.dynamodb")


class DynamoDBService:
    def __init__(self, table_name: Optional[str] = None, region_name: str = "us-east-1"):
        self.region_name = os.environ.get("AWS_DEFAULT_REGION", region_name)
        self.table_name = table_name or os.environ.get("DYNAMODB_TABLE_NAME", "JanStreamSubmissions")
        self.dynamodb = boto3.resource("dynamodb", region_name=self.region_name)
        self.table = None
        if os.environ.get("DYNAMODB_TABLE_NAME"):
            try:
                self.table = self.dynamodb.Table(self.table_name)
            except Exception:
                pass
        
        # In-memory store for local testing
        self._local_db: Dict[str, Dict[str, Any]] = {}

    def create_submission_record(
        self,
        submission_id: str,
        filename: str,
        s3_key: str,
        citizen_phone: Optional[str] = None
    ) -> Dict[str, Any]:
        """Creates initial submission record in RECEIVED state."""
        now_iso = datetime.now(timezone.utc).isoformat()
        item = {
            "submission_id": submission_id,
            "filename": filename,
            "s3_key": s3_key,
            "citizen_phone": citizen_phone or "N/A",
            "status": "RECEIVED",
            "created_at": now_iso,
            "updated_at": now_iso,
            "urgency_score": 0,
            "is_emergency": False,
            "triage_details": None
        }

        if self.table:
            try:
                self.table.put_item(Item=item)
                logger.info(f"Created DynamoDB record for {submission_id}")
            except ClientError as e:
                logger.error(f"Failed to put DynamoDB item: {e}")
                self._local_db[submission_id] = item
        else:
            self._local_db[submission_id] = item

        return item

    def update_triage_result(
        self,
        submission_id: str,
        triage_result: Dict[str, Any],
        status: str = "TRIAGED"
    ) -> Dict[str, Any]:
        """Updates record with Bedrock triage output and urgency score."""
        now_iso = datetime.now(timezone.utc).isoformat()
        urgency = triage_result.get("urgency_score", 1)
        is_emergency = triage_result.get("is_emergency", False)

        final_status = "ESCALATED_EMERGENCY" if is_emergency else status

        if self.table:
            try:
                self.table.update_item(
                    Key={"submission_id": submission_id},
                    UpdateExpression="SET #st = :st, triage_details = :td, urgency_score = :us, is_emergency = :ie, updated_at = :ua",
                    ExpressionAttributeNames={"#st": "status"},
                    ExpressionAttributeValues={
                        ":st": final_status,
                        ":td": triage_result,
                        ":us": urgency,
                        ":ie": is_emergency,
                        ":ua": now_iso
                    }
                )
            except ClientError as e:
                logger.error(f"Failed to update DynamoDB record {submission_id}: {e}")

        # Update local store
        if submission_id in self._local_db:
            self._local_db[submission_id].update({
                "status": final_status,
                "triage_details": triage_result,
                "urgency_score": urgency,
                "is_emergency": is_emergency,
                "updated_at": now_iso
            })

        return self.get_submission(submission_id) or {}

    def get_submission(self, submission_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves a single submission by its ID."""
        if self.table:
            try:
                resp = self.table.get_item(Key={"submission_id": submission_id})
                return resp.get("Item")
            except ClientError as e:
                logger.error(f"Failed to get DynamoDB item {submission_id}: {e}")
        
        return self._local_db.get(submission_id)

    def list_recent_submissions(self, limit: int = 15) -> List[Dict[str, Any]]:
        """Lists recent submissions for administrative dashboard."""
        if self.table:
            try:
                resp = self.table.scan(Limit=limit)
                return resp.get("Items", [])
            except ClientError as e:
                logger.error(f"Failed to scan DynamoDB: {e}")
        
        return list(self._local_db.values())[-limit:]
