# B3 Aviation & Aerospace Market Data Pipeline

Targeted data engineering pipeline dedicated to Brazilian aviation and aerospace stocks listed on B3. Automates ingestion, normalization, and transformation of market data to support sector-specific financial analysis and investment research.

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-009688?style=flat-square&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![AWS](https://img.shields.io/badge/AWS-S3%20%7C%20Lambda%20%7C%20Glue-FF9900?style=flat-square&logo=amazonaws&logoColor=white)](https://aws.amazon.com)
[![Parquet](https://img.shields.io/badge/Format-Parquet-50ABF1?style=flat-square&logo=apacheparquet&logoColor=white)](https://parquet.apache.org)
[![Athena](https://img.shields.io/badge/Query-Athena-8C4FFF?style=flat-square&logo=amazonaws&logoColor=white)](https://aws.amazon.com/athena/)
[![License](https://img.shields.io/badge/License-MIT-22C55E?style=flat-square)](LICENSE)

## Overview

System for collecting and processing B3 quotes with emphasis on reliability, partition idempotency, and low operational cost.

- Daily ingestion via `yfinance`, partitioned by date (`dt`)
- Raw Parquet storage in S3
- Event-driven orchestration (`S3 -> Lambda -> Glue`)
- Refined layer queryable via Amazon Athena
- REST API for manual trigger and reprocessing

## Architecture

```mermaid
flowchart LR
    subgraph Runtime["Python Runtime"]
        CLI["CLI\nsrc/main.py"]
        API["FastAPI\nsrc/ingestion/api.py"]
        Runner["Runner\nrun_ingestion.py"]
        Scraper["B3Scraper\nWebScrapping.py"]
    end

    subgraph AWS["AWS"]
        S3["S3\nraw/dt=YYYY-MM-DD"]
        Lambda["Lambda\nstart_glue_job.py"]
        Glue["Glue Job\nrefined_job.py"]
        Athena["Athena\nb3_refined"]
    end

    CLI --> Scraper
    API --> Runner --> Scraper
    Scraper -->|parquet| S3
    S3 -->|ObjectCreated| Lambda
    Lambda -->|start_job_run| Glue
    Glue -->|write| Athena
```

## Main Flows

### Daily ingestion (D-1)

```mermaid
sequenceDiagram
    participant SCH as Scheduler
    participant ING as run_daily_ingestion
    participant SCR as B3Scraper
    participant YF as yfinance
    participant S3 as S3

    SCH->>ING: trigger D-1
    ING->>ING: weekend validation
    ING->>SCR: create scraper (tickers, period, interval)
    SCR->>YF: download data
    YF-->>SCR: raw dataframe
    SCR->>SCR: normalize schema
    SCR->>S3: PUT raw/dt=YYYY-MM-DD/data.parquet
    S3-->>ING: written URI
```

### Manual trigger via API

```mermaid
sequenceDiagram
    participant Client
    participant API as FastAPI
    participant ING as run_daily_ingestion
    participant S3 as S3

    Client->>API: POST /ingestion/run?dt=YYYY-MM-DD
    API->>API: validate CRON_SECRET
    API->>ING: execute ingestion
    ING->>S3: write raw partition
    S3-->>ING: URI
    ING-->>API: { dt, skipped, uris }
    API-->>Client: 200 OK JSON
```

### S3 event to Glue

```mermaid
sequenceDiagram
    participant S3 as S3 Raw
    participant L as Lambda
    participant G as Glue Job

    S3->>L: ObjectCreated (raw/dt=YYYY-MM-DD/...)
    L->>L: extract dt with regex
    L->>G: start_job_run --dt=<dt>
    G->>G: transform raw to refined
    G-->>L: job started
```

## Repository Structure

```text
.
|-- src/
|   |-- ingestion/
|   |   |-- api.py                 # FastAPI endpoints
|   |   |-- run_ingestion.py       # Daily ingestion orchestration
|   |   |-- WebScrapping.py        # B3Scraper (yfinance)
|   |   `-- lambda_function.py     # Ingestion Lambda
|   |-- lambda/
|   |   `-- start_glue_job.py      # Lambda that triggers Glue
|   |-- glue/
|   |   `-- refined_job.py         # Pending implementation
|   `-- main.py                    # CLI entrypoint
|-- sql/
|   `-- athena_request.sql         # Reference refined query
`-- README.md
```

## Data Schema

### Raw layer: `s3://<bucket>/<prefix>/dt=YYYY-MM-DD/data.parquet`

| Column | Type | Description |
|--------|------|-------------|
| `date` | `timestamp` | Quote date/time |
| `ticker` | `string` | Asset code |
| `open` | `float64` | Open price |
| `high` | `float64` | High price |
| `low` | `float64` | Low price |
| `close` | `float64` | Close price |
| `adj_close` | `float64` | Adjusted close |
| `volume` | `int64` | Traded volume |

### Refined layer: `Athena table b3_refined` (partition key `dt`)

| Column | Type | Description |
|--------|------|-------------|
| `dt` | `date` | Partition key |
| `ticker` | `string` | Asset code |
| `volume_total_dia` | `int64` | Daily aggregated volume |
| `mm7_preco` | `float64` | 7-day moving average price |

Note: the refined layer still depends on implementation of `src/glue/refined_job.py`.

## How To Run

### Prerequisites

- Python 3.11+
- AWS credentials configured (`~/.aws/credentials` or environment variables)
- S3 bucket created and accessible

### Installation

```bash
pip install -r requirements.txt
```

### CLI ingestion

```bash
python src/main.py --tickers GOLL4,AZUL4 --period 1mo --s3-bucket your-bucket --s3-prefix raw --dt 2026-02-20
```

### API ingestion

```bash
# start server
uvicorn src.ingestion.api:app --reload

# trigger ingestion
curl -X POST "http://localhost:8000/ingestion/run?dt=2026-02-20" \
  -H "X-Cron-Secret: your_secret"
```

### Backfill Jan/Feb to raw and trigger refined

Using API:

```bash
curl -X POST "http://localhost:8000/ingestion/backfill?start=2026-01-01&end=2026-02-28&trigger_refined=true" \
  -H "X-Cron-Secret: your_secret"
```

Using Lambda event payload:

```bash
aws lambda invoke \
  --function-name your-ingestion-lambda \
  --payload fileb://config/events/backfill_jan_feb_2026.json \
  response.json
```

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `AWS_BUCKET` | S3 bucket for CLI mode (`src/main.py`) | Conditional |
| `S3_BUCKET` | S3 bucket for runner/API/Lambda ingestion | Conditional |
| `RAW_PREFIX` | Base prefix in S3 (default `raw`) | No |
| `TICKERS` | Comma-separated tickers | No |
| `CRON_SECRET` | Secret used by `POST /ingestion/run` | No |
| `GLUE_JOB_NAME` | Glue job name used to start refined processing | Yes (for refined trigger) |
| `RAW_BUCKET` | Legacy Lambda bucket variable | Legacy only |

## Non-Functional Requirements

| Category | Requirement |
|----------|-------------|
| Reliability | Skip weekends, fail explicitly on missing data, controlled overwrite by `dt` |
| Idempotency | Re-running for the same `dt` overwrites the same partition |
| Observability | Structured logs with `dt`, `ticker_count`, `rows_ingested`, `latency_ms` |
| Security | `CRON_SECRET` via env, no hardcoded credentials, least-privilege IAM |
| Cost | Parquet plus date partitioning, one file/day, short default period (`5d`) |

## Known Risks

| Risk | Mitigation |
|------|------------|
| `yfinance` may return empty or unstable data | Retries with backoff and alerts on failure |
| Quote schema drift | Validate schema before writing to S3 |
| Invalid S3 key parsing may block Glue trigger | Strict key validation and event logging |
| Duplicate Lambda implementations in repo | Define official flow and deprecate legacy files |

## Roadmap

- [x] Implement `src/glue/refined_job.py` with business transformations and `b3_refined` writes
- [x] Add unit tests for normalization and date selection logic
- [x] Add CI/CD with lint, tests, and dependency security checks
- [x] Publish operation runbook (backfill, rollback, troubleshooting)

## Architectural Decisions

| ID | Decision | Pros | Cons |
|----|----------|------|------|
| ADR-01 | Use `yfinance` as source | Fast implementation | No enterprise SLA |
| ADR-02 | Store raw data in Parquet on S3 | Low cost, efficient reads | Schema governance discipline required |
| ADR-03 | Use `S3 -> Lambda -> Glue` flow | On-demand processing | Higher operational complexity |
| ADR-04 | Expose API for manual triggers | Operational flexibility | Requires auth and rate limiting |
