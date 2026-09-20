# Building JanStream: Resilient Serverless Ingestion & AI Triage for Digital Bharat

**By Rohan Alex Bimal**  
*Built for Bharat Builds Tour: First Commit (WeMakeDevs in association with Amazon Web Services)*  
*Live Project:* [https://main.d1gdvhx1kit3yw.amplifyapp.com/](https://main.d1gdvhx1kit3yw.amplifyapp.com/)  
*GitHub Repository:* [https://github.com/ro-lex404/AWS-BharatBuilds-JanStream](https://github.com/ro-lex404/AWS-BharatBuilds-JanStream)

---

## 📌 The Problem: The "Portal Crash" Epidemic During Civic Crises

Across India, public service portals—from municipal complaint systems during urban monsoons to state disaster helplines—consistently collapse whenever a crisis strikes.

Every monsoon season in cities like Bengaluru, Mumbai, and Delhi, citizens rush to upload photos and videos of flooded underpasses, collapsed trees, and snapped electrical wires. Yet, within minutes of heavy rain, portals freeze or throw `HTTP 502/504 Bad Gateway` errors.

### Why do traditional portals crash?
1. **Synchronous File Ingress Bottleneck:** When 10,000 citizens upload 5MB–10MB evidence photos simultaneously, that represents **50 GB to 100 GB of network traffic** streaming directly through monolithic application servers (Node.js/Django/Spring pods). The servers buffer these bytes into RAM, exhausting memory, socket pools, and triggering catastrophic Out-Of-Memory (OOM) restarts.
2. **Cascading Synchronous Blockages:** Application Load Balancers hold client TCP connections open while downstream databases or AI pipelines process the requests. When the database slows down under load, connection limits saturate.
3. **No Prioritization of Life Hazards:** Critical life-safety hazards (e.g. *an 11kV high-tension power line snapped in a school water puddle*) sit in the exact same chronological FIFO queue as routine civic issues (*a flickering street light*).

We asked ourselves: **How can we build an unbreakable, scale-to-zero shock absorber on AWS that ingests heavy citizen evidence without consuming server RAM, prioritizes life hazards with AI, and costs $0.00 to idle?**

That is how **JanStream** was born.

---

## 🏗️ Architecture: The 100% Serverless Event-Driven Blueprint

JanStream decouples file ingress, buffering, AI triage, and telemetry into a resilient pipeline built on the **AWS Well-Architected Framework**:

```
[Citizen Client]
       │
       ▼ (1. Request S3 Presigned URL)
 [AWS Lambda API] ──► Generates cryptographic signed URL (~10ms)
       │
       ▼ (2. Direct Upload - 0 MB Server RAM)
 [Amazon S3 Bucket: janstream-citizen-uploads-rolex]
       │
       ▼ (3. Enqueue Metadata)
 [Amazon SQS Buffer: janstream-ingest-queue]
       │
       ├──► (On Malformed / Poison Pill) ──► [Amazon SQS DLQ: janstream-dlq]
       │                                            │
       │                                            ▼
       │                                     [AWS CloudWatch Alarm: JanStream-DLQ-Breach]
       ▼ (4. Controlled Worker Pull)
 [AWS Lambda Worker]
       │
       ▼ (5. Zero-Shot Vernacular AI Triage)
 [Amazon Bedrock: Claude / Nova Lite]
       │
       ▼ (6. Sub-10ms State Persistence)
 [Amazon DynamoDB: JanStreamSubmissions]
       │
       ▼ (7. Continuous Delivery)
 [AWS Amplify CDN Hosting]
```

---

## ⚡ The 5 Key Cloud Innovations in JanStream

### 1. Zero-RAM Cryptographic S3 Ingestion
Instead of streaming file bytes through our backend Lambda, the frontend requests a temporary cryptographic **S3 Presigned PUT URL**. The browser streams the 10MB photo directly into Amazon S3 (`janstream-citizen-uploads-rolex`). 
* **The Result:** The API Lambda processes only a ~200-byte JSON metadata request, consuming **0 MB of server RAM for file transfers**.

### 2. Traffic Surge Shock Absorption via Amazon SQS
During sudden civic spikes, the ingestion tier immediately pushes lightweight event notifications to an **Amazon SQS queue** (`janstream-ingest-queue`). Even if 10,000 requests arrive in 10 seconds, SQS absorbs the shock instantaneously. The worker tier drains the queue at an optimal, rate-limited cadence, eliminating cascading timeouts.

### 3. SRE Fault Isolation with Dead Letter Queues (DLQ)
In public systems, users and bots submit corrupted, malformed, or poisoned payloads. Rather than allowing a poisoned payload to crash worker threads and block the queue, JanStream automatically routes failed messages to an **Amazon SQS Dead Letter Queue** (`janstream-dlq`). 
* We connected this DLQ to an **AWS CloudWatch Metric Alarm (`JanStream-DLQ-Breach`)** that arms whenever poisoned message count > 0, alerting SRE on-call engineers while healthy traffic continues processing uninterrupted.

### 4. Multilingual Vernacular AI Triage via Amazon Bedrock
Citizen reports in India are rarely formatted in standard English. Citizens mix vernacular keywords like *"bijli"*, *"taar"*, *"paani"*, *"sadak"*, and *"minsaram"*. 
JanStream uses **Amazon Bedrock (Claude / Nova)** to perform zero-shot evaluation:
* Computes an **Urgency Score from 1 to 10**.
* Classifies the hazard into responsible civic departments (Discom QRT, Fire & Rescue, PWD, Jal Board).
* Sets an emergency SLA (e.g., 2 hours for life hazards vs 48 hours for routine potholes).
* *Resilience Circuit Breaker:* If foundation model APIs face network latency, JanStream gracefully degrades to a deterministic heuristic engine to guarantee sub-100ms response times.

### 5. Scale-to-Zero Cost Optimization ($0.00 vs $250/month)
For public utilities, 95% of days have baseline traffic, while disaster days see 500x surges. 

| Dimension | Traditional Kubernetes (EKS + ALB) | JanStream (Serverless AWS) |
| :--- | :--- | :--- |
| **Idle Monthly Cost** | **$250–$350/mo** (EKS control plane, 2x EC2 nodes, ALB, NAT Gateway) | **$0.00** (True scale-to-zero) |
| **File Transfer RAM** | **High (OOM crashes)** | **0 MB** (Direct S3 offload) |
| **Surge Provisioning Delay** | **2 to 5 minutes** (Auto-scaler EC2 provisioning) | **<50ms** (Instant SQS absorption) |

---

## 🧪 What We Learned & AWS Feedback

### What Worked Incredibly Well:
1. **S3 Presigned URLs:** Generating cryptographic upload signatures in ~10ms eliminated our API memory bottleneck entirely.
2. **SQS + DLQ Fault Isolation:** Building a live chaos testing feature (injecting poisoned payloads into SQS and seeing CloudWatch alarms trigger) was effortless due to AWS's native service bindings.
3. **AWS Amplify Git-Driven Deployments:** Pushing a commit to `main` triggered automatic builds and global CDN cache invalidation in under 90 seconds.

### Constructive Feedback & Areas for Improvement:
1. **Lambda Function URL CORS Configuration:** While Function URLs eliminate API Gateway costs, CORS preflight `OPTIONS` handling requires delicate header isolation to prevent clashes with custom response headers.
2. **Bedrock Model Access Onboarding:** On new AWS accounts, default quotas and console access enablement for models can fail with generic validation errors. Providing CLI hints linking directly to the Model Access page would streamline developer setup.
3. **DynamoDB Decimal Serialization:** Converting standard Python `float` types to `Decimal` for DynamoDB storage requires custom utility boilerplate that could be handled natively by the Boto3 resource tier.

---

## 🏁 Conclusion

JanStream proves that building mission-critical public infrastructure for India doesn't require expensive, always-on server clusters. By combining **S3 direct offloading, SQS backpressure, and Bedrock AI triage**, we can build digital public goods that are resilient, scalable, and cost-effective.

*Check out our live deployment and test the SRE chaos simulator here:* [https://main.d1gdvhx1kit3yw.amplifyapp.com/](https://main.d1gdvhx1kit3yw.amplifyapp.com/)
