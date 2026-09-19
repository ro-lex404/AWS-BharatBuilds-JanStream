"""
Metrics & Observability Service - JanStream
Emits AWS CloudWatch custom metrics and structured JSON logs with trace correlation.
"""

import os
import time
import json
import logging
from typing import Dict, Any, Optional
import boto3
from botocore.exceptions import ClientError

# Configure structured JSON logger
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("janstream.observability")


class MetricsService:
    NAMESPACE = "JanStream/IngestionEngine"

    def __init__(self, region_name: str = "us-east-1"):
        self.region_name = os.environ.get("AWS_DEFAULT_REGION", region_name)
        self.cloudwatch_client = None
        if os.environ.get("AWS_ACCESS_KEY_ID"):
            try:
                self.cloudwatch_client = boto3.client("cloudwatch", region_name=self.region_name)
            except Exception as e:
                logger.warning(f"Could not connect to CloudWatch: {e}")

    def log_event(
        self,
        event_name: str,
        submission_id: str,
        duration_ms: Optional[float] = None,
        details: Optional[Dict[str, Any]] = None,
        level: str = "INFO"
    ) -> None:
        """Outputs structured JSON log for CloudWatch Logs Insights."""
        log_payload = {
            "timestamp": time.time(),
            "event": event_name,
            "submission_id": submission_id,
            "duration_ms": duration_ms,
            "details": details or {}
        }
        json_line = json.dumps(log_payload)
        if level == "ERROR":
            logger.error(json_line)
        elif level == "WARNING":
            logger.warning(json_line)
        else:
            logger.info(json_line)

    def record_metric(
        self,
        metric_name: str,
        value: float,
        unit: str = "Count",
        dimensions: Optional[Dict[str, str]] = None
    ) -> None:
        """Pushes custom metric data point to AWS CloudWatch."""
        dims = [{"Name": k, "Value": v} for k, v in (dimensions or {}).items()]

        if self.cloudwatch_client:
            try:
                self.cloudwatch_client.put_metric_data(
                    Namespace=self.NAMESPACE,
                    MetricData=[
                        {
                            "MetricName": metric_name,
                            "Dimensions": dims,
                            "Value": value,
                            "Unit": unit
                        }
                    ]
                )
            except ClientError as e:
                logger.warning(f"Failed to push CloudWatch metric {metric_name}: {e}")
        else:
            # Local output for debugging
            self.log_event("METRIC_EMITTED", submission_id="system", details={
                "metric": metric_name,
                "value": value,
                "unit": unit
            })
