# ⚡ JanStream — Resilient Public Ingestion & Emergency Triage Engine

[![JanStream CI/CD](https://github.com/ro-lex404/AWS-BharatBuilds-JanStream/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/ro-lex404/AWS-BharatBuilds-JanStream/actions)
[![Live Demo](https://img.shields.io/badge/Demo-Live%20on%20AWS%20Amplify-success?logo=aws-amplify)](https://main.d1gdvhx1kit3yw.amplifyapp.com/)
[![AWS Architecture: Serverless](https://img.shields.io/badge/AWS-Serverless-orange?logo=amazon-aws)](https://aws.amazon.com)
[![Foundation Model: Bedrock](https://img.shields.io/badge/AI-Amazon%20Bedrock-purple)](https://aws.amazon.com/bedrock)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

> 🌐 **Live Production Dashboard:** [https://main.d1gdvhx1kit3yw.amplifyapp.com/](https://main.d1gdvhx1kit3yw.amplifyapp.com/)  
> ⚡ **Serverless API (Lambda URL):** [https://k5ozslqcrpti26q26gqwbgkta40zelma.lambda-url.us-east-1.on.aws/health](https://k5ozslqcrpti26q26gqwbgkta40zelma.lambda-url.us-east-1.on.aws/health)  
> 📊 **Live SRE Cloud Telemetry:** [https://k5ozslqcrpti26q26gqwbgkta40zelma.lambda-url.us-east-1.on.aws/api/sre/stats](https://k5ozslqcrpti26q26gqwbgkta40zelma.lambda-url.us-east-1.on.aws/api/sre/stats)  
> 🏆 **Built for Bharat Builds Tour: First Commit** *(Organized by WeMakeDevs & AWS)*

---

## 📌 The Problem: The "Portal Crash" Epidemic in Digital India

Across India, public portals for **scholarships, emergency disaster relief, university admissions, and municipal complaint redressal** consistently crash whenever heavy deadlines approach.

### Why do traditional portals crash?
* **Synchronous File Streaming:** Thousands of citizens upload 5MB–10MB photos/documents simultaneously. Traditional monolithic servers stream these bytes into server RAM, exhausting memory and socket pools.
* **Lack of Queue Backpressure:** Server CPUs spike to 100%, causing cascading failures and dropping connections (`504 Gateway Timeout`).
* **Manual, Slow Triage:** Critical life-safety hazards (e.g., *snapped 11kV live power line in flood water*) sit in the exact same backlog queue as routine issues (*flickering street light*), taking weeks to be noticed.

---

## 💡 The Solution: JanStream

**JanStream** replaces fragile monolithic servers with a **fault-tolerant, event-driven, scale-to-zero AWS architecture**:

1. **Direct-to-S3 Presigned Ingestion:** Citizens upload evidence photos and documents *directly to Amazon S3* using cryptographic presigned URLs. The backend server never handles raw file bytes and consumes **0 MB of RAM** during transfers.
2. **Asynchronous Backpressure (Amazon SQS + DLQ):** Upload events are pushed to an SQS queue. Even during a 10,000-citizen traffic surge, requests are buffered safely. Unprocessable messages route to a **Dead Letter Queue (DLQ)** without crashing the worker fleet.
3. **Automated AI Triage (Amazon Bedrock — Claude 3.5 Haiku):** Background workers use Bedrock to parse unstructured documents, extract key applicant metadata, translate vernacular Indian text, and score urgency from `1` (routine) to `10` (life-safety emergency).
4. **Instant Emergency Escalation:** High-urgency incidents (`score >= 9`) trigger automated emergency dispatch workflows via Amazon SNS.
5. **Observability & SRE Telemetry (AWS CloudWatch):** Structured JSON logs with correlation IDs, custom metrics (`IngestionCount`, `TriageDurationMs`, `DLQFailures`), and real-time CloudWatch metric alarms.
6. **Automated CI/CD:** GitHub Actions pipeline running linting (`flake8`) and unit tests (`pytest`) before any code is deployed to AWS.

---

## 🏗️ Architecture Diagram

```mermaid
flowchart LR
    subgraph Client["Client Tier"]
        Citizen(["Citizen / User"]) -->|"1. Request Presigned URL"| API["AWS Lambda (API Function URL)"]
        Citizen -->|"2. Direct Upload (Zero Server RAM)"| S3[("Amazon S3 Bucket")]
    end

    subgraph Ingestion["Asynchronous Buffer Tier"]
        API -->|"3. Buffer Ingestion Event"| SQS["Amazon SQS Ingestion Queue"]
        SQS -->|"4. Consume Batch"| Worker["AWS Lambda Worker"]
        Worker -.->|"On 3x Failure"| DLQ["Dead Letter Queue (DLQ)"]
    end

    subgraph Intelligence["AI Triage & State Tier"]
        Worker -->|"5. Vernacular AI Triage"| Bedrock["Amazon Bedrock (Claude 3.5 Haiku)"]
        Worker -->|"6. Commit State"| DynamoDB[("Amazon DynamoDB")]
    end

    subgraph Observability["Observability & SRE Tier"]
        Worker -->|"Structured Logs & Metrics"| CloudWatch["Amazon CloudWatch"]
        CloudWatch -->|"Alarm: DLQ Breach"| SNS["Amazon SNS Alert"]
    end
```

---

## 🚀 AWS Services Breakdown

| AWS Service | Architectural Purpose | Why It Matters |
| :--- | :--- | :--- |
| **Amazon S3** | Object storage for citizen photos & documents | Uses Presigned URLs for direct client-to-bucket uploads. Bypasses backend server memory bottlenecks. |
| **Amazon SQS** | Ingestion buffer & backpressure control | Smooths out sudden traffic surges. Prevents 504 timeouts. |
| **Dead Letter Queue (DLQ)** | Resilient failure isolation | Ensures malformed payloads never block the queue or get lost. |
| **Amazon Bedrock** | Foundation model (Claude 3.5 Haiku) | Fast, zero-shot document extraction, urgency scoring (1–10), and vernacular Indian language translation. |
| **Amazon DynamoDB** | Serverless NoSQL persistence | Single-digit millisecond state updates (`RECEIVED` $\rightarrow$ `QUEUED` $\rightarrow$ `TRIAGED` / `ESCALATED_EMERGENCY`). |
| **AWS CloudWatch** | Production monitoring & metric alarms | Tracks `TriageDurationMs`, `DLQFailureCount`, and structured JSON logs with trace correlation. |
| **AWS Amplify** | Global CDN Frontend Hosting | Single-page reactive SRE Mission Control dashboard with live telemetry. |

---

## 🧪 Testing the Pipeline Locally

### 1. Run the Automated Test Suite (Pytest)
```bash
cd backend
python -m pip install -r requirements.txt
python -m pytest -v tests/
```
All 4 automated unit tests will pass:
* `test_s3_presigned_url_generation` ✅
* `test_triage_routine_grievance` ✅
* `test_triage_emergency_hazard_escalation` ✅
* `test_end_to_end_worker_pipeline` ✅

### 2. Start the Backend API
```bash
cd backend
python -m uvicorn app.main:app --reload --port 8000
```
* **Interactive Swagger UI:** `http://127.0.0.1:8000/docs`
* **Health Endpoint:** `http://127.0.0.1:8000/health`

### 3. Open the SRE Mission Control Dashboard
* **Live Hosted Dashboard:** Access directly at [https://main.d1gdvhx1kit3yw.amplifyapp.com/](https://main.d1gdvhx1kit3yw.amplifyapp.com/)
* **Or Run Locally:** Open `frontend/index.html` in any browser.
* **Test Emergency Scenario:** Click *Critical: Live Wire in Water* $\rightarrow$ Watch the S3, SQS, Bedrock, and DynamoDB nodes pulse as data flows.
* **Test Surge Simulator:** Click *Simulate 15 Concurrent Uploads* $\rightarrow$ Watch the SQS queue absorb the traffic burst with 0 server crashes.
* **Test DLQ Fault Isolation:** Click *Inject Corrupted Payload (DLQ Test)* $\rightarrow$ Demonstrates dead-letter queue routing and CloudWatch alarm triggers.

---

## 🛠️ CI/CD Pipeline (GitHub Actions)

Located in `.github/workflows/ci-cd.yml`:
1. **Linting:** Enforces PEP 8 standards with `flake8`.
2. **Automated Unit Testing:** Executes `pytest tests/` on Python 3.11.
3. **Deployment Gate:** Automatically triggers AWS deployment on pushes to `main` branch.

---

## 👥 Team
* **Rohan Alex Bimal** ([@ro-lex404](https://github.com/ro-lex404))
* Team submission for **Bharat Builds Tour: First Commit** (Sept 2026).
