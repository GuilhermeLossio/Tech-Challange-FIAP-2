"""
AWS Lambda entrypoint for ingestion.

Modes:
- daily (default): ingest D-1 into raw
- backfill: ingest a date range into raw and optionally trigger refined jobs
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict

from ingestion.run_ingestion import (
    run_backfill_ingestion,
    run_daily_ingestion,
    trigger_refined_glue_jobs,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _is_backfill_event(event: Dict[str, Any]) -> bool:
    return event.get("mode") == "backfill" or (
        event.get("start") is not None and event.get("end") is not None
    )


def lambda_handler(event, context):
    event = event or {}

    try:
        if _is_backfill_event(event):
            result = run_backfill_ingestion(
                start_date=event.get("start"),
                end_date=event.get("end"),
                tickers=event.get("tickers"),
                s3_bucket=event.get("s3_bucket"),
                raw_prefix=event.get("raw_prefix"),
                interval=event.get("interval"),
                trigger_refined=bool(event.get("trigger_refined", True)),
                glue_job_name=event.get("glue_job_name"),
            )

            logger.info(
                "Backfill completed. partitions=%s raw_files=%s glue_runs=%s",
                len(result["partition_dates"]),
                len(result["uris"]),
                len(result["glue_runs"]),
            )

            return {
                "statusCode": 200,
                "body": json.dumps(
                    {
                        "mode": "backfill",
                        "start_date": result["start_date"],
                        "end_date": result["end_date"],
                        "partition_dates": result["partition_dates"],
                        "raw_uris": result["uris"],
                        "glue_runs": result["glue_runs"],
                    }
                ),
            }

        # Daily mode (default)
        result = run_daily_ingestion(
            target_date=event.get("dt"),
            tickers=event.get("tickers"),
            s3_bucket=event.get("s3_bucket"),
            raw_prefix=event.get("raw_prefix"),
            period=event.get("period"),
            interval=event.get("interval"),
        )

        if result["skipped"]:
            message = f"Ingestion skipped: {result['dt']} is a weekend."
            logger.info(message)
            return {
                "statusCode": 200,
                "body": json.dumps({"mode": "daily", "message": message}),
            }

        glue_runs = []
        if bool(event.get("trigger_refined", False)):
            glue_runs = trigger_refined_glue_jobs(
                partition_dates=[result["dt"]],
                glue_job_name=event.get("glue_job_name"),
            )

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "mode": "daily",
                    "date": result["dt"],
                    "raw_uris": result["uris"],
                    "glue_runs": glue_runs,
                }
            ),
        }

    except Exception as exc:
        logger.error("Ingestion failed: %s", exc, exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(exc)}),
        }
