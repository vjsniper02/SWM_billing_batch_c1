# SWM C1 Billing Batch Integration

**One-Line Summary:** A serverless AWS Lambda-based billing batch processing system that orchestrates CSV file ingestion from Landmark SFTP, transforms billing data into TechOne SOAP requests, and provides asynchronous queue-based processing with multi-destination file delivery.

## Table of Contents

1. [Overview](#overview)
2. [Architecture at a Glance](#architecture-at-a-glance)
3. [Repository Structure](#repository-structure)
4. [How It Works (Deep Dive)](#how-it-works-deep-dive)
5. [Workflow Diagrams](#workflow-diagrams)
6. [Installation & Setup](#installation--setup)
7. [Usage](#usage)
8. [Testing & Quality](#testing--quality)
9. [Troubleshooting](#troubleshooting)
10. [Extending the Project](#extending-the-project)
11. [Security & Secrets Management](#security--secrets-management)
12. [Contributing Guidelines](#contributing-guidelines)
13. [License](#license)
14. [Appendix](#appendix)

---

## Overview

### What This Project Does

The **SWM C1 Billing Batch Integration** is a complete, production-grade billing data pipeline that:

- **Ingests** CSV billing files from an external Landmark SFTP server on a scheduled basis
- **Validates** the data and enforces scheduling rules (only runs on specific days of the month)
- **Transforms** billing records into TechOne-compatible SOAP XML requests
- **Routes** data through an asynchronous queue (AWS SQS) for resilient processing
- **Delivers** files to multiple destinations (S3 for archival, GoAnywhere SFTP for downstream systems)
- **Integrates** with TechOne's Warehouse Import API to load billing data into enterprise financial systems

### Key Features

- ✅ **Scheduled Execution:** Runs on configurable days of the month via AWS EventBridge (ScheduleV2)
- ✅ **Bulk Processing:** Splits large CSV files into 4,999-row chunks to respect API limits
- ✅ **Multi-Format Output:** Generates TechOne-compliant SOAP XML with proper field transformations
- ✅ **Date Reformatting:** Converts DDMMYYYY dates to DD/MM/YYYY format per TechOne specs
- ✅ **Credential Management:** Stores SFTP and OAuth2 secrets in AWS Secrets Manager
- ✅ **Configuration-Driven:** Uses AWS SSM Parameter Store for environment-specific settings
- ✅ **S3 Lifecycle Management:** Auto-archives and expires billing files per retention policies
- ✅ **Asynchronous Architecture:** Queue-based processing decouples file ingestion from transformation
- ✅ **Error Handling:** Moves failed files to error folders and supports retry mechanisms
- ✅ **Infrastructure as Code:** Fully defined via AWS SAM (Serverless Application Model)
- ✅ **CI/CD Pipeline:** Automated build, test, and multi-environment deployment via Azure Pipelines

### Target Audience

- **DevOps/Cloud Engineers:** Deploy and manage the infrastructure
- **Backend Developers:** Extend transformations, add new field mappings, integrate with other systems
- **Data Engineers:** Monitor data quality, reconcile file counts, troubleshoot pipeline failures
- **New Joiners:** Understand serverless architecture patterns in AWS

### Primary Runtime

**Python 3.13** deployed on **AWS Lambda** (serverless functions)

### Deployment Type

**AWS Serverless** (Lambda + EventBridge + SQS + S3 + Secrets Manager + SSM) with **CI/CD via Azure Pipelines**

---

## Architecture at a Glance

### Component Overview

| Component | Type | Purpose |
|-----------|------|---------|
| **Billing File Processor** | Lambda Function | Scheduled file ingestion from Landmark SFTP |
| **Billing Queue** | SQS Queue | Buffers file chunks for async processing |
| **Billing Queue Consumer** | Lambda Function | Transforms CSV to SOAP and invokes TechOne adapter |
| **TechOne SOAP Adaptor** | Lambda Function | Authenticates to TechOne and sends SOAP requests |
| **Billing Bucket** | S3 Bucket | Stores raw, processed, and archived billing files |
| **Secrets Manager** | Service | Stores SFTP and OAuth2 credentials |
| **Parameter Store** | Service | Stores SFTP paths, schedule ranges, API URLs |

### Architecture Diagram

```mermaid
graph LR
    A["🕐 EventBridge<br/>ScheduleV2"] -->|Trigger on schedule| B["⚡ Billing File<br/>Processor Lambda"]
    
    B -->|Connect via<br/>Paramiko| C["🖥️ Landmark<br/>SFTP Server"]
    B -->|Fetch credentials| D["🔐 Secrets Manager<br/>LANDMARKS/GA Creds"]
    B -->|Get paths & params| E["⚙️ SSM Parameter<br/>Store"]
    
    C -->|Download CSV| B
    B -->|Send chunks to| F["📨 SQS Queue"]
    B -->|Upload to raw/| G["📦 S3 Billing<br/>Bucket"]
    B -->|Push to| H["🖥️ GoAnywhere<br/>SFTP"]
    
    F -->|Event source<br/>mapping| I["⚡ Billing Queue<br/>Consumer Lambda"]
    I -->|Read CSV from| G
    I -->|Fetch TechOne<br/>creds| D
    I -->|Invoke| J["⚡ TechOne SOAP<br/>Adaptor Lambda"]
    
    J -->|OAuth2<br/>Token| K["🌐 TechOne<br/>API"]
    J -->|SOAP<br/>Request| K
```

---

## Repository Structure

```
SWM_billing_batch_c1/
├── R2_int_c1_billing/                    # Main project directory
│
├── README.md                              # Original brief README
├── DETAILED_README.md                    # This file
│
├── pyproject.toml                         # Poetry dependencies and project metadata
├── pytest.ini                             # Pytest configuration (logging, env)
├── bandit.yaml                            # Security linting config
├── GitVersion.yml                         # Semantic versioning config
├── python-code-check.sh                   # CI/CD code quality check script
│
├── template.yaml                          # AWS SAM template (main IaC file)
│                                           # Defines Lambda functions, S3 bucket,
│                                           # SQS queue, EventBridge rules, IAM roles
│
├── pipelines/
│   └── c1_int_pipeline.yaml              # Azure Pipelines CI/CD config
│                                           # Stages: code_check, build, deploy, tag
│
├── functions/                             # Lambda function source code
│
│   ├── billing_file_processor/           # Scheduled ingestion function
│   │   ├── app.py                         # Main Lambda handler (233 lines)
│   │   ├── ga_sftp.py                     # GoAnywhere SFTP helper
│   │   ├── requirements.txt               # paramiko, pytz
│   │   └── __init__.py
│   │
│   ├── billing_queue/                     # Template/reference function
│   │   ├── app.py                         # SOAP request construction example
│   │   ├── requirements.txt               # paramiko
│   │   └── __init__.py
│   │
│   ├── billing_queue_consumer/           # SQS message processor
│   │   ├── c1_billing_queue_consumer.py  # Main handler (420 lines)
│   │   │                                   # Reads CSV, transforms to XML, invokes adaptor
│   │
│   └── techone_adaptor/                  # TechOne API integration
│       ├── c1_techone_soap_adaptor.py    # OAuth2 + SOAP sender (107 lines)
│       ├── requirements.txt               # requests, urllib3
│       └── __init__.py
│
└── tests/                                 # Test suite
    ├── test_billing_queue_consumer.py    # Unit tests with mocking
    ├── mock_boto.py                       # boto3 mock utilities
    └── FINANCE_20240510113633.csv        # Sample test data
```

### Key File Purposes

| File | Role | Key Responsibility |
|------|------|-------------------|
| [template.yaml](template.yaml) | Infrastructure | Defines all AWS resources (Lambdas, SQS, S3, IAM roles) |
| [functions/billing_file_processor/app.py](functions/billing_file_processor/app.py) | Ingestion | Connects to Landmark SFTP, chunks CSV, queues messages |
| [functions/billing_queue_consumer/c1_billing_queue_consumer.py](functions/billing_queue_consumer/c1_billing_queue_consumer.py) | Transformation | Parses CSV, builds XML, handles date/field formatting |
| [functions/techone_adaptor/c1_techone_soap_adaptor.py](functions/techone_adaptor/c1_techone_soap_adaptor.py) | Delivery | OAuth2 auth, SOAP request signing, API call |
| [functions/billing_file_processor/ga_sftp.py](functions/billing_file_processor/ga_sftp.py) | Delivery | Paramiko SFTP session to push files |
| [pipelines/c1_int_pipeline.yaml](pipelines/c1_int_pipeline.yaml) | CI/CD | Azure Pipelines build, test, SAM package, deploy |
| [pyproject.toml](pyproject.toml) | Dependencies | Poetry lock file; python 3.13, boto3, paramiko, etc. |
| [pytest.ini](pytest.ini) | Testing | Logging levels, pytest plugins, test env config |

---

## How It Works (Deep Dive)

### End-to-End Request Flow

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. SCHEDULED TRIGGER                                             │
│    EventBridge ScheduleV2 fires at configured times              │
│    (e.g., "cron(0 9 1-15 * ? *)" = days 1-15 of each month)     │
│    Invokes: Billing File Processor Lambda                        │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 2. VALIDATION & SCHEDULING CHECK                                │
│    • Fetch ALLOWED_SCHEDULE_RANGE from SSM Parameter Store      │
│    • Parse allowed day range (e.g., "1-15")                     │
│    • Check current day in Sydney timezone (Australia/Sydney)    │
│    • Skip execution if outside allowed window                   │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 3. SFTP CONNECTION & CREDENTIAL RETRIEVAL                        │
│    • Fetch LANDMARK_SFTP_SECRET_NAME from Secrets Manager       │
│    • Parse credentials:                                          │
│      - host (ftp_url)                                           │
│      - username (user_id)                                       │
│      - password (password)                                      │
│      - SSH key (key_value)                                      │
│    • Connect to Landmark SFTP server via Paramiko               │
│    • Change to LANDMARK_SFTP_PATH directory                     │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 4. FILE DISCOVERY & DOWNLOAD                                     │
│    • List all files in SFTP directory                           │
│    • Filter for .csv files (case-insensitive)                   │
│    • For each CSV:                                              │
│      - Download file content (binary)                           │
│      - Parse as charmap encoding                                │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 5. MULTI-DESTINATION FILE DELIVERY                               │
│    Step 5a: Upload RAW CSV to S3                                 │
│    • S3 key: raw/{csv_file_name}                                │
│    • Stores original unmodified CSV for audit trail             │
│                                                                  │
│    Step 5b: Push to GoAnywhere SFTP                             │
│    • via ga_sftp.push_file_to_ga()                             │
│    • Get GA_SFTP_SECRET_NAME credentials                        │
│    • Get GA_FTP_PATH from SSM                                   │
│    • Connect, push file, close connection                       │
│    • Sleep 20 seconds (rate limit/sync pause)                   │
│                                                                  │
│    Step 5c: Chunk CSV and Send to SQS                           │
│    • Split CSV into chunks of 4,999 rows (+ header)            │
│    • For each chunk:                                            │
│      - Preserve header row in multi-chunk files                │
│      - Create filename: queue/{original}_{start_end}_{idx}.csv  │
│      - Upload to S3 key (queue/ folder)                        │
│      - Send S3 key as message to SQS_QUEUE_URL                 │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 6. FILE CLEANUP ON LANDMARK SFTP                                 │
│    • After successful processing:                               │
│      - Delete file from Landmark SFTP                           │
│      - Use separate LANDMARK_SFTP_SECRET_NAME_DELETE cred       │
│    • On error: Log error, file remains for manual intervention  │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 7. SQS → ASYNC PROCESSING                                        │
│    • Each chunk's S3 key sits in SQS queue                      │
│    • SQS → Lambda event source mapping (batch size: TBD)        │
│    • Triggers Billing Queue Consumer Lambda                    │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 8. CSV → SOAP TRANSFORMATION (Queue Consumer)                   │
│    • Retrieve CSV chunk from S3                                 │
│    • Parse as CSV (comma-delimited, UTF-8)                     │
│    • Define CSV header (see SOAP field mapping below)           │
│    • For each row:                                              │
│      - Filter: Skip rows with empty CRMID                       │
│      - Date format: Convert DDMMYYYY → DD/MM/YYYY              │
│      - Quote handling: Add double quotes to string fields       │
│      - Build field list (31 fields)                             │
│      - Format as comma-separated values                         │
└────────────────────────────┬────────────────────────────────────┘
│                             │
│    CSV Fields Mapped to SOAP XML:                                │
│    ┌─────────────────────────────────────────────────┐           │
│    │ GENERAL LEDGER CODE → GENERALLEDGERCODE (NO QUOTE) │           │
│    │ BUSINESS AREA → BUSINESSAREACODE (QUOTED)         │           │
│    │ SALES REP → SALESREP (QUOTED)                     │           │
│    │ CAMPAIGN NAME → CAMPAIGNNAME (QUOTED)             │           │
│    │ BILLING ACCOUNT NAME → BILLINGACCOUNTNAME (QUOTED)│           │
│    │ [... 26 more fields ...]                          │           │
│    │ PRODUCT NAME → PRODUCTNAME (QUOTED)               │           │
│    └─────────────────────────────────────────────────┘           │
│                                                                  │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 9. XML CONSTRUCTION (construct_soap_request)                     │
│    • Build TechOne Warehouse_DoImport SOAP envelope            │
│    • Template: TechOne's MixedMode import (insert or update)   │
│    • Populate Auth block: UserId, Password, Config             │
│    • Add Column definitions (31 <ColumnInfo> elements)         │
│    • Append data rows (each row is comma-separated text)       │
│    • Serialize to XML string                                    │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 10. LAMBDA INVOCATION (Async)                                    │
│     • Queue Consumer invokes TechOne Adaptor Lambda             │
│     • Payload: { content: { soap_url, content } }              │
│     • InvocationType: RequestResponse (waits for result)        │
│     • Adaptor Lambda ARN from env: TECHONE_ADAPTOR_FUNCTION   │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 11. OAUTH2 AUTHENTICATION (TechOne Adaptor)                      │
│     • Fetch TECHONE_SECRET_NAME from Secrets Manager           │
│     • Credentials: client_id, client_secret                    │
│     • Get access_token_url from SSM Parameter Store            │
│     • POST to /oauth2/token:                                    │
│       {                                                          │
│         "grant_type": "client_credentials",                     │
│         "client_id": "...",                                     │
│         "client_secret": "..."                                 │
│       }                                                          │
│     • Parse response → access_token                            │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 12. SOAP REQUEST DELIVERY                                        │
│     • Get billing_soap_action_url from SSM                      │
│     • POST to TechOne SOAP endpoint:                            │
│       Headers: {                                                │
│         "Accept": "text/xml",                                   │
│         "Content-Type": "text/xml",                             │
│         "Authorization": access_token,                          │
│         "SOAPAction": billing_soap_action_url                   │
│       }                                                          │
│       Body: SOAP XML (content)                                  │
│     • Receive response from TechOne                             │
│       - Log response status, headers, body                      │
│       - Return to Queue Consumer                                │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 13. COMPLETION & FILE MOVEMENT                                   │
│     • Success: Move file from queue/ to delivered/              │
│     • Failure: Move file from queue/ to error/                  │
│     • Log: Record count, response details, any errors           │
└─────────────────────────────────────────────────────────────────┘
```

### Core Modules & Responsibilities

#### Module 1: Billing File Processor (`functions/billing_file_processor/app.py`)

**Entrypoint:** `lambda_handler(event, context)`

**Key Functions:**

| Function | Lines | Purpose |
|----------|-------|---------|
| `get_secret_credentials(secret_name)` | ~20 | Fetch SFTP credentials from Secrets Manager |
| `parse_csv(csv_data)` | ~10 | Decode CSV from charmap, return list of rows |
| `connect_to_sftp(hostname, username, password, ssh_key)` | ~6 | Establish Paramiko SFTP connection |
| `lambda_handler(event, context)` | ~150 | Main orchestration logic |

**Logic Gates & Conditional Branches:**

```python
# 1. Check if current day is within allowed range
if start_day <= current_day <= end_day:
    # Proceed with processing
else:
    # Skip execution (log and return)

# 2. For each CSV file ending in .csv:
for csv_file_name in files_on_sftp:
    if csv_file_name.lower().endswith(".csv"):
        # Process file

# 3. Split into chunks
for idx, start_index in enumerate(range(0, len(csv_rows), rows_per_chunk)):
    # Create chunked CSV, upload to S3, send SQS message
```

**Flow Summary:**
1. Validate schedule (SSM + timezone)
2. Connect to Landmark SFTP (Secrets Manager)
3. List files & filter for .csv
4. For each CSV:
   - Download
   - Upload to `raw/` on S3
   - Push to GoAnywhere SFTP (10-second sleep)
   - Split rows into 4,999-row chunks
   - Upload each chunk to `queue/` on S3
   - Send S3 key to SQS
   - Delete from Landmark SFTP

#### Module 2: Billing Queue Consumer (`functions/billing_queue_consumer/c1_billing_queue_consumer.py`)

**Entrypoint:** `lambda_handler(event, context)`

**Key Functions:**

| Function | Lines | Purpose |
|----------|-------|---------|
| `get_techone_soap_secret(secret_name)` | ~20 | Fetch TechOne SOAP credentials |
| `change_date_format(row_billing)` | ~50 | Convert DDMMYYYY to DD/MM/YYYY |
| `str_field_handling(row_billing)` | ~80 | Add double quotes to string fields |
| `construct_soap_request(user_id, password, config, csv_rows, first_file_flag)` | ~200 | Build SOAP XML envelope |
| `read_data_from_s3(file_path)` | ~30 | Parse CSV from S3 using DictReader |
| `lambda_handler(event, context)` | ~40 | Main SQS batch processor |

**SOAP Field Mapping (31 Fields):**

```
CSV Column Name              → SOAP XML Element
────────────────────────────────────────────────
General Ledger Code          → GENERALLEDGERCODE
Business Area                → BUSINESSAREACODE
Sales Rep                    → SALESREP
Sales Group                  → SALESGROUP
Sales Office                 → SALESOFFICE
Transaction Type             → TRANSACTIONTYPE
Goods Received               → GOODSRECEIVED
Campaign Type                → CAMPAIGNTYPE
CRMID                        → CRMID
Billing Account Name         → BILLINGACCOUNTNAME
Primary Advertiser           → PRIMARYADVERTISER
Start Date (reformatted)     → STARTDATE
End Date (reformatted)       → ENDDATE
Campaign Reference           → CAMPAIGNREFERENCE
Campaign Name                → CAMPAIGNNAME
Revenue Type                 → REVENUETYPE
Invoice Currency             → INVOICECURRENCY
Invoice Number               → INVOICENUMBER
Line Number                  → LINENUMBER
External PO Number           → EXTERNALPONUMBER
Subtotal – Sales Area Code   → SUBTOTALSALESAREACODE
Subtotal – Sales Area        → SUBTOTALSALESAREA
Tax                          → TAX
Subtotal Line                → SUBTOTALLINE
Agency Commission            → AGENCYCOMMISSION
Campaign Billing Start Date  → CAMPAIGNBILLINGSTARTDATE
Campaign Billing End Date    → CAMPAIGNBILLINGENDDATE
Journal Comments             → JOURNALCOMMENTS
Journal Type                 → JOURNALTYPE
Product Code                 → PRODUCTCODE
Product Name                 → PRODUCTNAME
```

**SOAP XML Structure:**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="..." xmlns:ns1="...">
  <soapenv:Header/>
  <soapenv:Body>
    <ns1:Warehouse_DoImport>
      <ns1:request WarehouseName="SALESFORCE" 
                   WarehouseTableName="BILLINGDATA" 
                   ImportMode="MixedMode">
        <ns1:Auth UserId="SRV_SFPROXY" 
                  Password="..." 
                  Config="..." 
                  FunctionName="$E1.BI.WHT.DOIMP.WS"/>
        <ns1:Columns>
          <ns1:ColumnInfo Name="GENERALLEDGERCODE"/>
          <!-- ... 30 more columns ... -->
        </ns1:Columns>
        <ns1:Rows>
          <ns1:Row>field1,field2,...,field31</ns1:Row>
          <!-- ... one row per CSV record (CRMID != empty) ... -->
        </ns1:Rows>
      </ns1:request>
    </ns1:Warehouse_DoImport>
  </soapenv:Body>
</soapenv:Envelope>
```

**Logic:**
1. Parse SQS event (batch of messages)
2. Extract S3 key from message body
3. Read CSV from S3 (DictReader with comma delimiter)
4. Fetch TechOne credentials from Secrets Manager
5. For each CSV row:
   - Skip if CRMID is empty
   - Reformat dates (DDMMYYYY → DD/MM/YYYY)
   - Add quotes to string fields
   - Build row text
6. Construct complete SOAP XML
7. Invoke TechOne Adaptor Lambda
8. Return SQS batch result (failures logged)

#### Module 3: TechOne SOAP Adaptor (`functions/techone_adaptor/c1_techone_soap_adaptor.py`)

**Entrypoint:** `lambda_handler(event, context)`

**Key Functions:**

| Function | Lines | Purpose |
|----------|-------|---------|
| `get_techone_secret()` | ~15 | Fetch OAuth2 credentials |
| `lambda_handler(event, context)` | ~45 | OAuth2 token exchange + SOAP POST |

**Logic:**
1. Fetch client_id & client_secret from Secrets Manager (TECHONE_SECRET_NAME)
2. Fetch access_token_url & billing_soap_action_url from SSM Parameter Store
3. POST to OAuth2 endpoint for access_token:
   ```json
   {
     "grant_type": "client_credentials",
     "client_id": "...",
     "client_secret": "..."
   }
   ```
4. Parse token response → extract `access_token`
5. POST SOAP content to billing_soap_action_url with headers:
   ```
   Accept: text/xml
   Content-Type: text/xml
   Authorization: {access_token}
   SOAPAction: {billing_soap_action_url}
   ```
6. Log response (status, headers, body)

#### Module 4: GoAnywhere SFTP Helper (`functions/billing_file_processor/ga_sftp.py`)

**Key Functions:**

| Function | Lines | Purpose |
|----------|-------|---------|
| `get_secret(secret_name)` | ~20 | Fetch GA SFTP credentials |
| `connect_to_sftp(hostname, username, ssh_key)` | ~4 | Paramiko SFTP connection (RSA key auth) |
| `push_file_to_ga(bucket_name, file_name, secret_name, ftp_path)` | ~40 | Download from S3, upload to GA SFTP |

**Logic:**
1. Fetch GA SFTP credentials & key from Secrets Manager
2. Parse RSA private key from StringIO
3. Get GA FTP path from SSM Parameter Store
4. Construct remote path: `{sftp_path}/{filename}`
5. Connect via Paramiko using RSA key
6. Download file content from S3
7. Upload (putfo) to GA SFTP
8. Handle IOError and SSHException

---

## Workflow Diagrams

### Diagram 1: High-Level System Flow

```mermaid
sequenceDiagram
    actor EventBridge as EventBridge<br/>(ScheduleV2)
    participant BFP as Billing File<br/>Processor Lambda
    participant LM as Landmark<br/>SFTP Server
    participant SM as Secrets<br/>Manager
    participant SSM as SSM Parameter<br/>Store
    participant S3 as S3 Billing<br/>Bucket
    participant GA as GoAnywhere<br/>SFTP
    participant SQS as SQS<br/>Queue
    participant BQC as Billing Queue<br/>Consumer Lambda
    participant TA as TechOne SOAP<br/>Adaptor Lambda
    participant TechOne as TechOne<br/>API

    EventBridge->>BFP: Trigger (scheduled time)
    BFP->>SSM: Get allowed day range
    alt Day in range?
        BFP->>SM: Get Landmark SFTP creds
        BFP->>LM: Connect & list files
        LM->>BFP: Return .csv file list
        BFP->>LM: Download CSV
        LM->>BFP: CSV data
        BFP->>S3: Upload raw/{filename}
        BFP->>GA: Push file to GA SFTP
        BFP->>S3: Upload queue/{chunk}.csv (batches)
        BFP->>SQS: Send S3 key messages
        BFP->>LM: Delete processed file
    else Skip
        BFP->>BFP: Log: Outside schedule window
    end
    Note over SQS: Files await async processing
    SQS->>BQC: Trigger (event source mapping)
    BQC->>S3: Read CSV chunk
    S3->>BQC: CSV data
    BQC->>SM: Get TechOne SOAP creds
    BQC->>BQC: Parse CSV, build SOAP XML
    BQC->>TA: Invoke Lambda (SOAP payload)
    TA->>SM: Get OAuth2 credentials
    TA->>TechOne: POST /oauth2/token
    TechOne->>TA: access_token
    TA->>TechOne: POST SOAP request (with token)
    TechOne->>TA: SOAP response
    TA->>BQC: Return response
    BQC->>S3: Move to delivered/ or error/
```

### Diagram 2: Data Transformation Pipeline

```mermaid
graph TB
    CSV["📄 Input CSV<br/>(from Landmark SFTP)"]
    CSV -->|Parse with DictReader| Parse["Python DictReader<br/>(comma-delimited)"]
    
    Parse -->|For each row| Process["🔄 Row Processing"]
    
    Process -->|Check CRMID| Filter{{"CRMID<br/>empty?"}}
    Filter -->|Yes| Skip["⊘ Skip row"]
    Filter -->|No| Transform["✅ Transform row"]
    
    Transform -->|Format dates| DateFmt["📅 Date Format<br/>DDMMYYYY → DD/MM/YYYY<br/>(Start, End, Billing dates)"]
    
    DateFmt -->|Add quotes| Quote["💬 String Quoting<br/>String fields: wrapped in quotes<br/>Numeric fields: no quotes"]
    
    Quote -->|Build field list| Build["🏗️ Field List Assembly<br/>(31 fields, comma-separated)"]
    
    Build -->|Collect rows| XML["📦 SOAP XML Generation<br/>Warehouse_DoImport envelope<br/>+ Auth block<br/>+ Column definitions<br/>+ Data rows"]
    
    XML -->|Serialize| Payload["📬 SOAP Payload<br/>(XML string)"]
    
    Payload -->|Invoke Lambda| OAuth["🔐 OAuth2 Authentication<br/>GET access_token"]
    
    OAuth -->|SOAP POST| TechOne["🌐 TechOne API<br/>Warehouse_DoImport<br/>Batch insert/update"]
    
    TechOne -->|Response| Result["✅ Billing data loaded<br/>in TechOne warehouse"]
```

### Diagram 3: Error Handling & File Movement

```mermaid
graph LR
    Raw["raw/<br/>Original CSV"]
    Queue["queue/<br/>CSV Chunks"]
    Delivered["delivered/<br/>Processed"]
    Error["error/<br/>Failed"]
    Delete["Landmark SFTP<br/>Deleted"]
    
    Raw -->|After chunking| Queue
    Queue -->|SQS → Consumer| Processing{{"Processing<br/>Success?"}}
    
    Processing -->|✅ Yes| Delivered
    Processing -->|❌ No| Error
    
    Delivered -->|Post-processing| Delete
    Error -->|Manual review| Delete
    
    style Delivered fill:#90EE90
    style Error fill:#FFB6C6
    style Delete fill:#FFD700
```

---

## Installation & Setup

### Prerequisites

- **Operating System:** macOS, Linux, or Windows (WSL)
- **Python Version:** 3.13 (installed and in PATH)
- **Poetry:** Package manager for Python (`pip install --user poetry`)
- **AWS Account:** Access to AWS services (Lambda, S3, SQS, Secrets Manager, SSM, IAM)
- **AWS CLI:** Configured with appropriate credentials for your environment
- **SAM CLI:** AWS Serverless Application Model (`pip install aws-sam-cli`)
- **Git:** Version control
- **Node.js 16+:** (Optional, for Azure Pipelines if running locally)

### Step 1: Clone & Navigate

```bash
git clone <repository-url>
cd SWM_billing_batch_c1/R2_int_c1_billing
```

### Step 2: Configure Python Environment (Poetry)

Ensure Python 3.13 is available:

```bash
python3.13 --version  # Should output: Python 3.13.x
```

Add Python 3.13 to PATH (if not already):

```bash
# macOS example
export PATH="/usr/local/bin:/Library/Frameworks/Python.framework/Versions/3.13/bin:$PATH"
```

Install Poetry and dependencies:

```bash
# Install Poetry globally (if not already done)
pip3 install --user poetry

# Ensure Poetry uses Python 3.13
poetry env use /usr/local/bin/python3.13

# Install project dependencies (including dev dependencies)
poetry install --no-root

# Verify installation
poetry run python --version
```

### Step 3: Environment Variables & Configuration

#### Create Local `.env` File (for local testing)

```bash
# Create .env in the project root
cat > .env << 'EOF'
# AWS Configuration
AWS_REGION=ap-southeast-2
AWS_PROFILE=default  # or your AWS profile name

# Function Environment Variables (used by Lambda)
LANDMARK_SFTP_PATH=/r2dev/c1/lmk_ftppath
ALLOWED_SCHEDULE_RANGE=/r2dev/c1/allowed_schedule_range
SEIL_S3_BUCKET=seil-r2dev-billing
LANDMARK_SFTP_SECRET_NAME=r2dev/lmk_ftp
LANDMARK_SFTP_SECRET_NAME_DELETE=r2dev/lmk_ftp_delete
GA_SFTP_SECRET_NAME=r2dev/ga_ftp
GA_FTP_PATH=/r2dev/c1/ga_ftp_path
SQS_QUEUE_URL=https://queue.amazonaws.com/123456789012/seil-r2dev-c1-billing-queue
S3_KEY=s3-prefix/filename.ext

# TechOne Integration
TECHONE_SOAP_SECRET_NAME=r2dev/techone_soap
TECHONE_API_ACCESS_TOKEN_URL=/r2dev/techone/access_token_url
TECHONE_BILLING_SOAP_ACTION_URL=/r2dev/techone/billing_soap_action_url
TECHONE_ADAPTOR_FUNCTION=seil-r2dev-c1-techone-soap-adaptor
BILLING_BUCKET=seil-r2dev-billing
EOF
```

#### Configure AWS Secrets Manager

Store the following secrets in AWS Secrets Manager:

**1. Landmark SFTP Credentials** (Secret Name: `{env}/lmk_ftp`)

```json
{
  "ftp_url": "landmark.example.com",
  "user_id": "your_landmark_user",
  "password": "your_landmark_password",
  "key_value": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"
}
```

**2. Landmark Delete Credentials** (Secret Name: `{env}/lmk_ftp_delete`)

```json
{
  "ftp_url": "landmark.example.com",
  "user_id": "your_landmark_delete_user",
  "password": "your_landmark_delete_password",
  "key_value": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"
}
```

**3. GoAnywhere SFTP Credentials** (Secret Name: `{env}/ga_ftp`)

```json
{
  "ftp_url": "goanywhere.example.com",
  "user_id": "your_ga_user",
  "password": "your_ga_password",
  "key_value": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"
}
```

**4. TechOne SOAP Credentials** (Secret Name: `{env}/techone_soap`)

```json
{
  "UserId": "SRV_SFPROXY",
  "Password": "techone_password",
  "WSDL": "https://techone.example.com/soap/warehouse",
  "Config": "SEVEN-TEST-CES direct"
}
```

**5. TechOne OAuth2 Credentials** (Secret Name: `{env}/techone_oauth`)

```json
{
  "client_id": "your_oauth2_client_id",
  "client_secret": "your_oauth2_client_secret"
}
```

#### Configure AWS SSM Parameter Store

Store the following parameters:

| Parameter Name | Value | Type |
|----------------|-------|------|
| `/{env}/c1/lmk_ftppath` | `/ftp.out/FINANCE` | String |
| `/{env}/c1/allowed_schedule_range` | `1-15` | String |
| `/{env}/c1/ga_ftp_path` | `/goanywhere/incoming` | String |
| `/{env}/techone/access_token_url` | `https://techone.example.com/v1/oauth2/token` | String |
| `/{env}/techone/billing_soap_action_url` | `http://TechnologyOneCorp.com/Public/Services/Warehouse_DoImport` | String |

**Command-line examples:**

```bash
# Create SSM parameters
aws ssm put-parameter \
  --name "/r2dev/c1/lmk_ftppath" \
  --value "/ftp.out/FINANCE" \
  --type "String" \
  --overwrite

aws ssm put-parameter \
  --name "/r2dev/c1/allowed_schedule_range" \
  --value "1-15" \
  --type "String" \
  --overwrite
```

### Step 4: Build with SAM

```bash
# Build Lambda functions and dependencies
sam build --template template.yaml

# Output: CloudFormation-compatible artifact in `.aws-sam/build/`
```

### Step 5: Local Testing (Optional)

#### Run Unit Tests

```bash
# Run pytest with coverage
poetry run pytest -s --show-capture=stdout -vv --cov=functions

# Run specific test file
poetry run pytest -s tests/test_billing_queue_consumer.py
```

#### Run SAM Local (Simulate Lambda Locally)

```bash
# Start SAM local API / emulate Lambda
sam local start-api --template template.yaml

# OR for event invocation:
sam local invoke BillingFileProcessorFunction --event event.json
```

### Step 6: Deploy to AWS

#### Option A: Manual Deployment via SAM

```bash
# Deploy interactively (guided)
sam deploy --guided

# Or deploy with parameters:
sam deploy \
  --template-file template.yaml \
  --stack-name seil-r2dev-c1 \
  --region ap-southeast-2 \
  --capabilities CAPABILITY_IAM \
  --s3-bucket swmi-r2dev-sourcecode \
  --s3-prefix r2dev_int_c1_stack
```

#### Option B: CI/CD via Azure Pipelines

Commit and push to repository; Azure Pipelines automatically:
1. Runs code quality checks (bandit, safety)
2. Builds SAM package
3. Deploys to selected environment (r2dev, r2sit, r2uat, r2train, prod)

```bash
git add .
git commit -m "Feature: New billing transformation rules"
git push origin main
# Azure Pipelines pipeline triggers automatically
```

### Step 7: Verify Deployment

```bash
# Check CloudFormation stack
aws cloudformation describe-stacks --stack-name seil-r2dev-c1 --region ap-southeast-2

# Check Lambda function
aws lambda get-function --function-name seil-r2dev-c1-BillingFileProcessor --region ap-southeast-2

# Check SQS queue
aws sqs get-queue-url --queue-name seil-r2dev-c1-billing-queue --region ap-southeast-2
```

---

## Usage

### Running the Pipeline Manually

#### Trigger Billing File Processor Directly

```bash
# Invoke Lambda directly (simulates scheduled trigger)
aws lambda invoke \
  --function-name seil-r2dev-c1-BillingFileProcessor \
  --region ap-southeast-2 \
  --payload '{}' \
  response.json

# Check response
cat response.json
```

#### Simulate CSV File Upload to Landmark SFTP

```bash
# Create a sample CSV
cat > sample_billing.csv << 'EOF'
General Ledger Code,Business Area,Sales Rep,Sales Group,Sales Office,Transaction Type,Goods Received,Campaign Type,CRMID,Billing Account Name,Primary Advertiser,Start Date,End Date,Campaign Reference,Campaign Name,Revenue Type,Invoice Currency,Invoice Number,Line Number,External PO Number,Subtotal – Sales Area Code,Subtotal – Sales Area,Tax,Subtotal Line,Agency Commission,Campaign Billing Start Date,Campaign Billing End Date,Journal Comments,Journal Type,Product Code,Product Name
110-0608-00604,Seven,REP001,GROUP001,OFFICE001,Standard,FALSE,Reach,CRM001234,Seven Seven Australia Pty Ltd,Seven Seven Australia Pty Ltd,01012024,31012024,CAMP001,Campaign A,Reach,AUD,INV001,1,PO001,CODE001,Sales Area 1,Australian GST,10,19999.98,320.00,01012024,31012024,Test Campaign,NetCost,PROD001,Product A
EOF

# Upload to Landmark SFTP (via your SFTP client or script)
# Once uploaded, the scheduled Lambda will pick it up
```

#### Monitor Execution

```bash
# View Billing File Processor logs
aws logs tail /aws/lambda/seil-r2dev-c1-BillingFileProcessor --follow

# View Billing Queue Consumer logs
aws logs tail /aws/lambda/seil-r2dev-c1-BillingQueueConsumer --follow

# View TechOne Adaptor logs
aws logs tail /aws/lambda/seil-r2dev-c1-TechOneSOAPAdaptor --follow
```

#### Check SQS Queue Status

```bash
# Get queue attributes
aws sqs get-queue-attributes \
  --queue-url "$(aws sqs get-queue-url --queue-name seil-r2dev-c1-billing-queue --query 'QueueUrl' --output text)" \
  --attribute-names All

# Receive messages (for testing)
aws sqs receive-message \
  --queue-url "$(aws sqs get-queue-url --queue-name seil-r2dev-c1-billing-queue --query 'QueueUrl' --output text)" \
  --max-number-of-messages 1
```

#### Check S3 Bucket Contents

```bash
# List raw files
aws s3 ls s3://seil-r2dev-billing/raw/ --recursive

# List queue files
aws s3 ls s3://seil-r2dev-billing/queue/ --recursive

# List delivered files
aws s3 ls s3://seil-r2dev-billing/delivered/ --recursive

# List error files
aws s3 ls s3://seil-r2dev-billing/error/ --recursive

# Download a specific file for inspection
aws s3 cp s3://seil-r2dev-billing/raw/FINANCE_20240510113633.csv ./local_copy.csv
```

### API/Integration Examples

#### Example: Manual SOAP Request (for testing TechOne adaptor directly)

```bash
# Invoke TechOne Adaptor Lambda directly
aws lambda invoke \
  --function-name seil-r2dev-c1-TechOneSOAPAdaptor \
  --region ap-southeast-2 \
  --payload '{"content":{"soap_url":"https://techone.example.com/soap/warehouse","content":"<?xml version=\"1.0\"?>..."}}' \
  response.json

cat response.json
```

#### Example: Queue Consumer Response Structure

**Input SQS Message:**
```json
{
  "Records": [
    {
      "messageId": "msg-12345",
      "receiptHandle": "...",
      "body": "queue/FINANCE_20240510113633.csv_0_4998_1.csv"
    }
  ]
}
```

**Output (after processing):**
```json
{
  "batchItemFailures": []
}
```

If processing fails:
```json
{
  "batchItemFailures": [
    {
      "itemIdentifier": "msg-12345"
    }
  ]
}
```

---

## Testing & Quality

### Running Tests

#### Unit Tests with Coverage

```bash
# Run all tests with verbose output and coverage report
poetry run pytest -s --show-capture=stdout -vv --cov=functions

# Run specific test file
poetry run pytest tests/test_billing_queue_consumer.py -v

# Generate HTML coverage report
poetry run pytest --cov=functions --cov-report=html
# Open htmlcov/index.html in browser
```

#### Code Quality Checks

```bash
# Run Bandit (security linter)
poetry run bandit -r functions/ -c bandit.yaml

# Run Black (code formatter - check only)
poetry run black --check functions/

# Format code with Black
poetry run black functions/

# Run Safety (dependency vulnerability check - requires API key)
poetry run safety check --key $SAFETY_API_KEY

# Run all checks (via CI/CD script)
bash python-code-check.sh bandit.yaml $SAFETY_API_KEY
```

### Test Files

**Test Location:** [tests/test_billing_queue_consumer.py](tests/test_billing_queue_consumer.py)

**What's Tested:**
- Queue Consumer Lambda handler invocation
- CSV parsing from mock S3
- Mock boto3 session with TechOne secret credentials
- Mock Lambda invocation for TechOne adaptor

**Sample Data:** [tests/FINANCE_20240510113633.csv](tests/FINANCE_20240510113633.csv)

**Running Tests with Mock Data:**

```bash
poetry run pytest tests/test_billing_queue_consumer.py::TestBillingQueueConsumer::test_lambda_handler -v
```

### pytest Configuration

Configuration in [pytest.ini](pytest.ini):

```ini
[pytest]
log_cli = 1
log_cli_level = INFO
log_format = %(asctime)s %(levelname)s %(message)s
log_date_format = %Y-%m-%d %H:%M:%S
```

---

## Troubleshooting

### Common Errors & Fixes

#### Error 1: Lambda Timeout (> 900 seconds)

**Symptom:** `TaskFailedOnTimeout`

**Root Cause:** 
- Large CSV file (> 500K rows)
- Slow SFTP connection
- GoAnywhere SFTP push taking too long

**Fix:**
```bash
# Increase Lambda timeout in template.yaml
Timeout: 1800  # 30 minutes

# Or reduce chunk size in billing_file_processor/app.py
rows_per_chunk = 2500  # instead of 4999

# Rebuild and redeploy
sam build && sam deploy
```

#### Error 2: SFTP Connection Refused

**Symptom:** `paramiko.ssh_exception.SSHException: Error reading SSH protocol banner`

**Root Cause:**
- SFTP credentials expired in Secrets Manager
- SFTP firewall blocking Lambda security group
- Wrong hostname/port

**Fix:**
```bash
# Verify credentials in Secrets Manager
aws secretsmanager get-secret-value --secret-id r2dev/lmk_ftp

# Test SFTP connection manually
sftp -i private_key.pem username@landmark.example.com

# Check VPC/security group configuration
aws ec2 describe-security-groups --group-ids sg-xxxxx

# Update credentials and test
# In Lambda environment, ensure NAT gateway/bastion access to SFTP server
```

#### Error 3: SQS Message Not Triggering Queue Consumer

**Symptom:** Messages accumulate in SQS; Consumer Lambda not invoked

**Root Cause:**
- No event source mapping configured
- IAM role missing SQS permissions
- Queue URL mismatch

**Fix:**
```bash
# Verify event source mapping exists
aws lambda list-event-source-mappings \
  --function-name seil-r2dev-c1-BillingQueueConsumer

# If missing, add via SAM or AWS CLI
aws lambda create-event-source-mapping \
  --event-source-arn arn:aws:sqs:ap-southeast-2:123456789012:seil-r2dev-c1-billing-queue \
  --function-name seil-r2dev-c1-BillingQueueConsumer \
  --batch-size 1

# Check IAM role
aws iam get-role --role-name seil-r2dev-c1-LambdaExecutionRole
```

#### Error 4: TechOne SOAP Request Fails (401 Unauthorized)

**Symptom:** `HTTP 401` from TechOne API

**Root Cause:**
- OAuth2 token expired
- Client credentials invalid
- Token endpoint URL wrong

**Fix:**
```bash
# Verify OAuth2 credentials
aws secretsmanager get-secret-value --secret-id r2dev/techone_oauth

# Test OAuth2 token endpoint manually
curl -X POST \
  https://techone.example.com/v1/oauth2/token \
  -d "grant_type=client_credentials&client_id=...&client_secret=..."

# Check access_token_url in SSM
aws ssm get-parameter --name /r2dev/techone/access_token_url

# Update credentials and redeploy
```

#### Error 5: CSV Date Parsing Error

**Symptom:** `Error converting in row: {...}. Error: time data '....' does match format '%d%m%Y'`

**Root Cause:**
- CSV has dates in different format (e.g., DD/MM/YYYY instead of DDMMYYYY)
- Date field is empty or null

**Fix:**
```python
# In c1_billing_queue_consumer.py, check date parsing
i_start_date = row_billing["Start Date"]
logger.info(f"Raw date value: {i_start_date}, length: {len(i_start_date)}")

# Adjust format if needed
source_format = "%d-%m-%Y"  # if CSV uses DD-MM-YYYY
target_format = "%d/%m/%Y"

# Handle leading zeros
if len(i_start_date) == 7:
    i_start_date = "0" + i_start_date
```

#### Error 6: Double Quote Handling Issues in SOAP

**Symptom:** XML parsing error or TechOne rejects SOAP

**Root Cause:**
- String fields contain unescaped quotes
- Field handler adding quotes to numeric fields

**Fix:**
```python
# In c1_billing_queue_consumer.py, str_field_handling()
# Verify fields in numeric_indices (no quotes):
numeric_indices = [0, 11, 12, 13, 17, 18, 23, 24, 25, 26, 28, 29]

# Escape double quotes in string fields
field_quote = f'"{field.replace(chr(34), "&quot;")}"'
```

### Debugging Checklist

- [ ] Check CloudWatch logs for Lambda invocations
- [ ] Verify AWS credentials and IAM permissions
- [ ] Validate environment variables in Lambda console
- [ ] Confirm secrets are stored in Secrets Manager with correct format
- [ ] Test SFTP connectivity manually from Lambda VPC
- [ ] Inspect S3 bucket contents (raw/, queue/, delivered/, error/ folders)
- [ ] Monitor SQS queue depth and messages
- [ ] Review TechOne API documentation for SOAP schema changes
- [ ] Check file permissions on SFTP servers
- [ ] Verify network connectivity (security groups, NACLs)

### Logging & Monitoring

#### Enable Debug Logging

```python
# In app.py, set logging level to DEBUG
logger.setLevel(logging.DEBUG)

# Log detailed information at key points
logger.debug(f"SFTP path: {sftp_path}")
logger.debug(f"CSV rows before chunking: {len(csv_rows)}")
logger.debug(f"SOAP XML: {ET.tostring(root)}")
```

#### CloudWatch Insights Queries

```sql
# Find errors in past hour
fields @timestamp, @message
| filter @message like /ERROR/
| stats count() by @message

# Count Lambda invocations by function
fields @message, @duration
| stats avg(@duration) as avg_duration by @logStream
| sort avg_duration desc

# Track SQS message volume
fields @message
| filter @message like /SQS/
| stats count() as message_count
```

---

## Extending the Project

### Adding a New CSV Field Mapping

**Scenario:** TechOne adds a new field `Department Code` to the warehouse table.

**Steps:**

1. **Update CSV Header** in sample/test files:
   ```csv
   General Ledger Code,...,Department Code,Product Name
   ```

2. **Add to SOAP Mapping** in [functions/billing_queue_consumer/c1_billing_queue_consumer.py](functions/billing_queue_consumer/c1_billing_queue_consumer.py):

   ```python
   # In construct_soap_request(), add to columns
   + '               <ns1:ColumnInfo Name="DEPARTMENTCODE"/>'
   ```

   ```python
   # In str_field_handling(), add to field extraction
   department_code = row_billing["Department Code"]
   
   # Add to field_list
   field_list = [
       # ... existing fields ...
       department_code,
       product_name,
   ]
   ```

3. **Update Index Logic** (which fields get quotes):

   ```python
   # In str_field_handling(), numeric fields (no quotes)
   if index in [0, 11, 12, 13, 17, 18, 23, 24, 25, 26, 28, 29, 31]:  # Add new index
       modified_row_billing_str += field + ","
   ```

4. **Test:**
   ```python
   # Update test CSV
   # Run tests: poetry run pytest tests/test_billing_queue_consumer.py
   ```

5. **Deploy:**
   ```bash
   git add functions/
   git commit -m "Feature: Add Department Code to TechOne mapping"
   git push  # Triggers Azure Pipelines
   ```

### Adding a New Transformation Rule

**Scenario:** Campaign names need to be uppercase before sending to TechOne.

**Steps:**

1. **Add Transformation Function** in [functions/billing_queue_consumer/c1_billing_queue_consumer.py](functions/billing_queue_consumer/c1_billing_queue_consumer.py):

   ```python
   def uppercase_campaign_name(row):
       """Convert campaign name to uppercase."""
       row["Campaign Name"] = row["Campaign Name"].upper() if row["Campaign Name"] else ""
       return row
   ```

2. **Apply in Processing Pipeline**:

   ```python
   # In lambda_handler(), after reading CSV
   csv_reader_list = read_data_from_s3(file_path)
   
   # Apply transformations
   csv_reader_list = [uppercase_campaign_name(row) for row in csv_reader_list]
   ```

3. **Test & Deploy:**
   ```bash
   poetry run pytest tests/test_billing_queue_consumer.py -v
   git add functions/ && git commit -m "Feature: Uppercase campaign names" && git push
   ```

### Adding a New File Destination

**Scenario:** Archive processed files to a separate backup S3 bucket.

**Steps:**

1. **Update Infrastructure** in [template.yaml](template.yaml):

   ```yaml
   BackupBucket:
     Type: AWS::S3::Bucket
     Properties:
       BucketName: !Sub "seil-${EnvPrefix}-billing-backup"
       VersioningConfiguration:
         Status: Enabled
   ```

2. **Add IAM Permission** to Lambda role:

   ```yaml
   - Version: "2012-10-17"
     Statement:
     - Effect: Allow
       Action:
       - s3:PutObject
       Resource: !Sub "arn:aws:s3:::seil-${EnvPrefix}-billing-backup/*"
   ```

3. **Add Backup Logic** in [functions/billing_queue_consumer/c1_billing_queue_consumer.py](functions/billing_queue_consumer/c1_billing_queue_consumer.py):

   ```python
   def backup_file_to_s3(s3_client, bucket_name, source_key, target_bucket):
       """Copy processed file to backup bucket."""
       copy_source = {"Bucket": bucket_name, "Key": source_key}
       target_key = f"backup/{source_key.split('/')[-1]}"
       s3_client.copy_object(
           CopySource=copy_source,
           Bucket=target_bucket,
           Key=target_key
       )
   ```

4. **Call in Consumer**:

   ```python
   # In lambda_handler()
   backup_bucket = os.environ["BACKUP_BUCKET"]
   backup_file_to_s3(s3_client, BILLING_BUCKET, csv_key, backup_bucket)
   ```

5. **Add Environment Variable** in template.yaml:

   ```yaml
   Environment:
     Variables:
       BACKUP_BUCKET: !Ref BackupBucket
   ```

6. **Deploy:**
   ```bash
   sam build && sam deploy
   ```

### Adding Error Alerts (SNS Notification)

**Scenario:** Send Slack/Email alerts when files fail to process.

**Steps:**

1. **Add SNS Topic** in [template.yaml](template.yaml):

   ```yaml
   AlertTopic:
     Type: AWS::SNS::Topic
     Properties:
       DisplayName: Billing Pipeline Alerts
       TopicName: !Sub "seil-${EnvPrefix}-c1-alerts"
   ```

2. **Add LaunchEvent Trigger**:

   ```yaml
   - Version: "2012-10-17"
     Statement:
     - Effect: Allow
       Action:
       - sns:Publish
       Resource: !Ref AlertTopic
   ```

3. **Publish Errors** in [functions/billing_queue_consumer/c1_billing_queue_consumer.py](functions/billing_queue_consumer/c1_billing_queue_consumer.py):

   ```python
   sns = boto3.client("sns")
   
   except Exception as e:
       logger.error(f"Error processing message: {e}")
       sns.publish(
           TopicArn=os.environ["ALERT_TOPIC_ARN"],
           Subject="Billing Pipeline Error",
           Message=f"Failed to process: {csv_key}\nError: {str(e)}"
       )
       result["batchItemFailures"].append({"itemIdentifier": msg["messageId"]})
   ```

4. **Add Subscription** (Slack/Email):

   ```bash
   aws sns subscribe \
     --topic-arn arn:aws:sns:ap-southeast-2:123456789012:seil-r2dev-c1-alerts \
     --protocol email \
     --notification-endpoint your-email@example.com
   ```

### Adding Metrics & Dashboards

**Scenario:** Track CSV file counts, record counts, execution duration.

**Steps:**

1. **Add CloudWatch Custom Metrics** in Lambda handlers:

   ```python
   cloudwatch = boto3.client("cloudwatch")
   
   cloudwatch.put_metric_data(
       Namespace="BillingPipeline",
       MetricData=[
           {
               "MetricName": "FilesProcessed",
               "Value": files_count,
               "Unit": "Count"
           },
           {
               "MetricName": "RecordsProcessed",
               "Value": record_count,
               "Unit": "Count"
           }
       ]
   )
   ```

2. **Create Dashboard** (via AWS Console or CloudFormation):

   ```bash
   aws cloudwatch put-dashboard \
     --dashboard-name BillingPipeline \
     --dashboard-body file://dashboard.json
   ```

### Common Modification Points

| Area | Files | Modification Type |
|------|-------|-------------------|
| SFTP Credentials | `template.yaml`, Secrets Manager | Update credentials |
| Allowed Days | SSM Parameter Store | Change schedule range |
| CSV Field Mapping | `c1_billing_queue_consumer.py` | Add/remove fields |
| Date Format | `c1_billing_queue_consumer.py` | Change date transformer |
| Chunk Size | `app.py` (BFP) | Adjust `rows_per_chunk` |
| Lambda Timeout | `template.yaml` | Increase/decrease timeout |
| S3 Lifecycle | `template.yaml` | Adjust retention policies |
| ErrorHandling | `c1_billing_queue_consumer.py` | Add retry logic |
| Integrations | New Lambda functions | Add new adaptor |

---

## Security & Secrets Management

### Secrets Storage

All sensitive data is stored in **AWS Secrets Manager**. Never hardcode credentials.

**Secrets Managed:**
- SFTP credentials (Landmark, GoAnywhere)
- OAuth2 credentials (TechOne)
- TechOne SOAP auth credentials

**Retrieval Pattern:**

```python
def get_secret(secret_name):
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])
    return secret
```

### IAM Permissions

**Principle of Least Privilege:** Each Lambda has minimal required permissions.

**Billing File Processor Permissions:**
- `secretsmanager:GetSecretValue` (SFTP credentials)
- `ssm:GetParameter` (paths, schedule range)
- `s3:*` (read/write to billing bucket)
- `sqs:SendMessage` (queue messages)

**Billing Queue Consumer Permissions:**
- `secretsmanager:GetSecretValue` (TechOne credentials)
- `s3:GetObject` (read from billing bucket)
- `lambda:InvokeFunction` (call TechOne adaptor)

**TechOne SOAP Adaptor Permissions:**
- `secretsmanager:GetSecretValue` (OAuth2 credentials)
- `ssm:GetParameter` (OAuth2/SOAP URLs)
- CloudWatch Logs (write)

### Network Security

**VPC Considerations:**
- If Lambda runs in VPC, ensure NAT gateway for external SFTP access
- Security groups should allow outbound HTTPS (443) for TechOne API
- VPC endpoints for S3, SQS, Secrets Manager (optional, reduces egress costs)

### Compliance & Audit

**CloudTrail:** All API calls logged
**CloudWatch:** Lambda execution logs retained for 30 days
**S3 Versioning:** Enabled on billing bucket for audit trail
**Encryption:** At rest (KMS), in transit (TLS 1.2+)

### Credential Rotation

Rotate secrets every 90 days:

```bash
# Update credential in Secrets Manager
aws secretsmanager update-secret \
  --secret-id r2dev/lmk_ftp \
  --secret-string '{"ftp_url":"...","user_id":"...","password":"...","key_value":"..."}'

# Lambda automatically fetches updated credentials on next invocation
```

---

## Contributing Guidelines

### Branching Strategy

- **Main Branch (`main`):** Production-ready code
- **Develop Branch (`develop`):** Integration branch for features
- **Feature Branch (`feature/...`):** Individual features

### Commit Message Format

```
<type>: <subject>

<body>

<footer>
```

**Types:** `feat`, `fix`, `refactor`, `test`, `docs`, `chore`

**Examples:**
```
feat: Add email alerts for pipeline failures

fix: Handle empty CRMID records in CSV parsing

test: Add test cases for SOAP XML generation
```

### Code Style

- **Python:** Follow PEP 8 (enforced by Black)
- **Line length:** 88 characters (Black default)
- **Imports:** Organized (stdlib, third-party, local)

```python
import json
import os
import logging
from io import StringIO

import boto3
import paramiko

from functions.billing_file_processor.ga_sftp import push_file_to_ga
```

### Pre-Commit Checks

```bash
# Format code
poetry run black functions/

# Check security
poetry run bandit -r functions/ -c bandit.yaml

# Run tests
poetry run pytest -v

# Push only after all checks pass
git push origin feature/my-feature
```

### Pull Request Process

1. Create feature branch from `develop`
2. Make changes following code style guidelines
3. Write/update tests
4. Run `poetry run pytest && poetry run black functions/ && poetry run bandit -r functions/`
5. Create PR with description
6. Request review from team
7. Address feedback
8. Merge to `develop` after approval
9. Create release PR to `main` with version bump

### Version Bumping

Uses **Semantic Versioning** via GitVersion:

```bash
# Tag is created automatically by CI/CD based on commit messages
# Major: breaking changes (feat!)
# Minor: new features (feat)
# Patch: bug fixes (fix)

# Manual tag (if needed)
git tag -a v1.2.3 -m "Release version 1.2.3"
git push origin v1.2.3
```

---

## License

Unknown from repository context. Please consult your organization's standard license or check with the repository maintainer.

---

## Appendix

### A. Glossary

| Term | Definition |
|------|-----------|
| **Landmark** | External SFTP server hosting billing CSV files |
| **GoAnywhere** | File transfer/workflow automation platform (downstream) |
| **TechOne** | Enterprise financial system with Warehouse import API |
| **SFTP** | SSH File Transfer Protocol for secure file transfer |
| **SOAP** | Simple Object Access Protocol (XML-based web service) |
| **EventBridge (ScheduleV2)** | AWS serverless event scheduler (formerly CloudWatch Events) |
| **SQS** | AWS Simple Queue Service for asynchronous messaging |
| **Lambda** | AWS serverless compute service |
| **SAM** | AWS Serverless Application Model (IaC for serverless) |
| **Parameter Store** | AWS Systems Manager Parameter Store (config storage) |
| **Secrets Manager** | AWS service for credentials/secrets storage |
| **MixedMode** | TechOne import mode (insert or update based on key) |

### B. Configuration Matrices

#### Environment Parameter Naming Convention

```
Format: /{environment}/{system}/{parameter}

Examples:
/r2dev/c1/lmk_ftppath              → Landmark SFTP path
/r2dev/c1/allowed_schedule_range   → Allowed days (1-15)
/r2dev/c1/ga_ftp_path              → GoAnywhere SFTP path
/r2dev/techone/access_token_url    → OAuth2 endpoint
/r2dev/techone/billing_soap_action_url → SOAP action URI
```

#### Deployment Environments

| Environment | Stack Name | S3 Bucket | Purpose |
|-------------|-----------|-----------|---------|
| **r2dev** | `seil-r2dev-c1` | `seil-r2dev-billing` | Development |
| **r2sit** | `seil-r2sit-c1` | `seil-r2sit-billing` | System Integration Testing |
| **r2uat** | `seil-r2uat-c1` | `seil-r2uat-billing` | User Acceptance Testing |
| **r2train** | `seil-r2train-c1` | `seil-r2train-billing` | Training |
| **prod** | `seil-prod-c1` | `seil-prod-billing` | Production |

#### CSV File Lifecycle

```
Landmark SFTP
    ↓
    → S3: raw/ (intact copy)
    → GoAnywhere SFTP (push)
    → S3: queue/ (chunked)
    ↓
    SQS
    ↓
    Lambda: Queue Consumer
    ↓
    Either: ✅ delivered/ or ❌ error/
    ↓
    [Archived/Cleaned by S3 Lifecycle]
```

### C. Key Files Reference

**Infrastructure:**
- [template.yaml](template.yaml) – SAM template with Lambda, SQS, S3, IAM

**Application Code:**
- [functions/billing_file_processor/app.py](functions/billing_file_processor/app.py) – File ingestion (233 lines)
- [functions/billing_queue_consumer/c1_billing_queue_consumer.py](functions/billing_queue_consumer/c1_billing_queue_consumer.py) – CSV transformation (420 lines)
- [functions/techone_adaptor/c1_techone_soap_adaptor.py](functions/techone_adaptor/c1_techone_soap_adaptor.py) – SOAP delivery (107 lines)
- [functions/billing_file_processor/ga_sftp.py](functions/billing_file_processor/ga_sftp.py) – SFTP helper

**Configuration:**
- [pyproject.toml](pyproject.toml) – Poetry dependencies
- [pytest.ini](pytest.ini) – Test configuration
- [pipelines/c1_int_pipeline.yaml](pipelines/c1_int_pipeline.yaml) – Azure Pipelines CI/CD
- [bandit.yaml](bandit.yaml) – Security linting

**Tests:**
- [tests/test_billing_queue_consumer.py](tests/test_billing_queue_consumer.py) – Unit tests
- [tests/FINANCE_20240510113633.csv](tests/FINANCE_20240510113633.csv) – Sample test data

### D. Useful Commands Reference

**Poetry:**
```bash
poetry install                              # Install dependencies
poetry run pytest                           # Run tests
poetry run black functions/                 # Format code
poetry update                               # Update dependencies
poetry show                                 # List installed packages
```

**AWS CLI:**
```bash
aws lambda invoke --function-name <fn> --payload '{}' out.json  # Invoke Lambda
aws s3 ls s3://bucket/ --recursive          # List S3 contents
aws logs tail /aws/lambda/<fn> --follow     # Stream logs
aws sqs receive-message --queue-url <url>   # Get SQS message
aws secretsmanager get-secret-value --secret-id <name>         # Get secret
```

**SAM:**
```bash
sam build                                   # Build application
sam deploy                                  # Deploy to AWS
sam local invoke <function>                 # Test locally
```

**Git:**
```bash
git tag -a v1.0.0 -m "Release"             # Create version tag
git push origin v1.0.0                      # Push tag
git log --oneline | head -20                # View recent commits
```

### E. Performance Optimization Tips

1. **Lambda Memory/Timeout:**
   - Current: 4098 MB, 900 sec → Suitable for large CSV files
   - Monitor actual usage in CloudWatch; adjust if needed

2. **CSV Chunking:**
   - 4,999 rows per chunk → Balances processing time vs. queue overhead
   - Reduce if TechOne API rejects large payloads

3. **SQS Batch Processing:**
   - Default batch size: 10 messages
   - Tune based on consumer Lambda memory/duration

4. **S3 Bucket Optimization:**
   - Current lifecycle: INTELLIGENT_TIERING (0 days), GLACIER (30 days), DEEP_ARCHIVE (180 days)
   - Adjust retention based on compliance requirements

5. **SFTP Connection Pooling:**
   - Currently creates new connection per invocation
   - Consider connection reuse if performance bottleneck identified

### F. Open Questions & Unknowns

For new contributors, clarify the following with the team:

1. **TechOne API Specifics:**
   - What error response codes indicate retry vs. permanent failure?
   - Max payload size for SOAP requests?
   - Is batch import transactional (all-or-nothing)?

2. **SFTP & File Handling:**
   - Are there duplicate file handling rules (e.g., re-upload same CSV)?
   - What naming conventions are expected for output files?
   - Should failed deletions on Landmark SFTP trigger alerts?

3. **Data Quality:**
   - Are there CRM ID validation rules (format, ranges, allowlists)?
   - How should rows with missing mandatory fields be handled (skip vs. error)?
   - Date validation: check for future dates, invalid day/month combinations?

4. **Scalability:**
   - Expected file sizes and frequency?
   - Peak throughput (files/hour, records/hour)?
   - Should there be rate limiting to TechOne API?

5. **Disaster Recovery:**
   - Should failed chunks be retried automatically or require manual intervention?
   - How long to retain raw files for audit/recovery?
   - Is there a rollback procedure for incorrectly loaded data in TechOne?

6. **Monitoring & Alerting:**
   - What metrics should trigger PagerDuty/Slack alerts?
   - Executive dashboard: what KPIs are critical?
   - SLA: max acceptable delay from file drop to TechOne load?

---

**Document Version:** 1.0  
**Last Updated:** 2025-02-25  
**Maintained By:** [Your Team/Organization]

For questions, reach out to the engineering team or consult the repository wiki.
