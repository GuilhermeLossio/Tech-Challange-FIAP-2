# Engineering Best Practices

This guide defines the minimum quality baseline for evolving the project safely.

## 1. Configuration and Secrets

- Never commit `.env` with real values.
- Commit only `.env.example` with placeholders.
- Load secrets from environment variables or AWS Secrets Manager.
- Avoid hardcoded production defaults in code.

## 2. Structure and Responsibilities

- `WebScrapping.py`: data collection and normalization only.
- `run_ingestion.py`: daily orchestration only.
- `api.py`: HTTP layer and access validation only.
- Lambdas: thin handlers that delegate to domain functions.

Rule: separate external I/O (S3, API, yfinance) from business logic to make testing easier.

## 3. Logging and Observability

- Include in logs: `dt`, `tickers`, `bucket`, `prefix`.
- Use standard log levels:
  - `INFO`: normal flow and counters
  - `WARNING`: recoverable degradation
  - `ERROR`: failures that stop execution
- Emit minimum metrics per execution:
  - `rows_ingested`
  - `partitions_written`
  - `duration_ms`

## 4. Error Handling

- Convert expected errors to clear messages (`ValueError` -> HTTP 400 in API).
- Do not swallow exceptions silently.
- In Lambda, return contextual error payload and keep stack trace in logs.

## 5. Data Quality

Before writing raw data:

- validate required columns (`date`, `ticker`, `open`, `high`, `low`, `close`, `volume`)
- reject empty dataframe when `dt` is specified
- enforce correct types for `date` and `ticker`

## 6. Automated Testing

Recommended minimum coverage:

- Unit:
  - `normalize_tickers`
  - `normalize_download`
  - `_coerce_target_date`
  - `_resolve_s3_bucket`
- Integration:
  - in-memory Parquet write
  - S3 upload simulation with `moto` or stubs
- API:
  - `/health` returns 200
  - `/ingestion/run` returns 401 when secret is invalid

## 7. Dependencies and Security

- Pin dependency versions in `requirements.txt`.
- Review library CVEs periodically.
- Remove unused dependencies to reduce attack surface.

## 8. Recommended CI/CD

Minimum PR pipeline:

1. `ruff` (lint)
2. `black --check` (format)
3. `pytest` (tests)
4. `pip-audit` (security)

Deployment:

- promote versioned artifacts
- keep fast rollback to previous version
- block deploys when tests fail

## 9. PR Conventions

Quick checklist:

- [ ] Change documented in `README.md` or technical docs
- [ ] No secrets in diff
- [ ] Error logs include enough context
- [ ] Any schema/contract impact is described
- [ ] Rollback plan defined when applicable
