"""
Bedrock Service - JanStream Resilient Ingestion
Invokes Amazon Bedrock (Claude 3.5 Haiku) for zero-shot document extraction,
urgency triage (0 - 10), and vernacular Indian language translation.
"""

import os
import json
import re
import logging
from typing import Dict, Any, Optional
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger("janstream.bedrock")


class BedrockService:
    def __init__(self, region_name: str = "us-east-1"):
        self.region_name = os.environ.get("AWS_DEFAULT_REGION", region_name)
        self.bedrock_client = None
        if os.environ.get("AWS_ACCESS_KEY_ID"):
            try:
                self.bedrock_client = boto3.client("bedrock-runtime", region_name=self.region_name)
            except Exception as e:
                logger.warning(f"Could not initialize Bedrock client: {e}")

    def triage_document(
        self,
        document_text_or_desc: str,
        filename: str,
        content_type: str = "text/plain"
    ) -> Dict[str, Any]:
        """
        Extracts structured grievance/application details and urgency score using Amazon Bedrock.
        """
        if self.bedrock_client:
            try:
                return self._invoke_bedrock_claude(document_text_or_desc, filename)
            except Exception as e:
                logger.warning(f"Bedrock invocation failed, falling back to heuristic triage: {e}")

        # Rule-based / Heuristic triage fallback
        return self._heuristic_triage(document_text_or_desc, filename)

    def _invoke_bedrock_claude(self, text: str, filename: str) -> Dict[str, Any]:
        """Calls Anthropic Claude 3.5 Haiku on Amazon Bedrock."""
        prompt = f"""
You are the JanStream Citizen Intake & Disaster Triage AI for Indian Public Services.
Analyze the following citizen application or grievance submission (Filename: '{filename}').

Content:
```
{text[:3500]}
```

Provide strict JSON output matching this schema:
{{
  "applicant_name": "Extracted name or 'Anonymous'",
  "category": "Disaster Relief" | "Electrical Hazard" | "Road Infrastructure" | "Water Supply" | "Healthcare" | "Scholarship / Education" | "Other",
  "urgency_score": <Integer from 1 (lowest) to 10 (critical life-safety hazard)>,
  "is_emergency": <Boolean true if life/safety at risk (e.g. open manhole, fallen wire, flash flood, medical emergency)>,
  "detected_language": "Hindi" | "Tamil" | "Telugu" | "Kannada" | "Bengali" | "Marathi" | "English" | "Mixed",
  "administrative_summary": "Clean 2-sentence English summary for departmental routing.",
  "recommended_department": "Municipal Corporation" | "State Disaster Management" | "Electricity Board (Discom)" | "Jal Board" | "Social Welfare",
  "action_items": ["Action 1", "Action 2"]
}}
"""
        body = json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 700,
            "temperature": 0.1,
            "messages": [{"role": "user", "content": prompt}]
        })

        response = self.bedrock_client.invoke_model(
            modelId="anthropic.claude-3-5-haiku-20241022-v1:0",
            contentType="application/json",
            accept="application/json",
            body=body
        )

        response_body = json.loads(response["body"].read().decode("utf-8"))
        raw_text = response_body["content"][0]["text"]

        json_match = re.search(r"\{[\s\S]*\}", raw_text)
        if json_match:
            return json.loads(json_match.group(0))
        raise ValueError("Failed to parse JSON response from Bedrock")

    def _heuristic_triage(self, text: str, filename: str) -> Dict[str, Any]:
        """Intelligent offline triage rules based on Indian public service keywords."""
        lower_text = (text + " " + filename).lower()

        # Critical hazard detection
        hazard_keywords = ["wire", "shock", "current", "flood", "drown", "fire", "leak", "collapse", "manhole", "accident"]
        is_emergency = any(kw in lower_text for kw in hazard_keywords)

        category = "General Public Grievance"
        dept = "Municipal Corporation"
        urgency = 4

        if any(w in lower_text for w in ["wire", "electric", "power", "transformer", "pole", "blackout"]):
            category = "Electrical Hazard"
            dept = "Electricity Board (Discom)"
            urgency = 9 if is_emergency else 6
        elif any(w in lower_text for w in ["water", "sewage", "drain", "pipe", "drinking"]):
            category = "Water Supply"
            dept = "Jal Board"
            urgency = 7 if is_emergency else 5
        elif any(w in lower_text for w in ["pothole", "road", "tar", "asphalt", "bridge"]):
            category = "Road Infrastructure"
            dept = "Public Works Department (PWD)"
            urgency = 8 if is_emergency else 4
        elif any(w in lower_text for w in ["scholarship", "college", "school", "fee", "exam"]):
            category = "Scholarship / Education"
            dept = "Social Welfare Department"
            urgency = 3

        if is_emergency:
            urgency = max(urgency, 9)

        return {
            "applicant_name": "Citizen Applicant",
            "category": category,
            "urgency_score": urgency,
            "is_emergency": is_emergency,
            "detected_language": "English / Mixed",
            "administrative_summary": f"Citizen grievance regarding {category.lower()} in local ward. Requires inspection by {dept}.",
            "recommended_department": dept,
            "action_items": [
                f"Dispatch field inspector from {dept}",
                "Send SMS notification with tracking ID to citizen"
            ]
        }
