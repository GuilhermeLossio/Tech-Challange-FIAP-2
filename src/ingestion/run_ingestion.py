"""
Daily ingestion entrypoint.

Responsibilities:
- Define the ingestion date (D-1)
- Define the tickers to be ingested
- Trigger raw data ingestion into S3 (Parquet, partitioned by dt)

This script intentionally contains no business logic or transformations.
"""

from __future__ import annotations

from datetime import date, timedelta
import os
from typing import Optional, Union, Dict, List

from ingestion.WebScrapping import B3Scraper

# ---------------------------------------------------------
# Defaults
# ---------------------------------------------------------

DEFAULT_TICKERS = "GOLL4,AZUL4,EMBR3,EVEB31"
DEFAULT_S3_BUCKET = "aeronaticalverifier-s3"
DEFAULT_RAW_PREFIX = "raw"
DEFAULT_PERIOD = "5d"
DEFAULT_INTERVAL = "1d"

# ---------------------------------------------------------
# Ingestion
# ---------------------------------------------------------

def _coerce_target_date(target_date: Optional[Union[str, date]]) -> date:
    if target_date is None:
        return date.today() - timedelta(days=1)
    if isinstance(target_date, date):
        return target_date
    return date.fromisoformat(target_date)


def _resolve_s3_bucket(explicit: Optional[str]) -> str:
    if explicit:
        return explicit
    return os.getenv("S3_BUCKET") or os.getenv("AWS_BUCKET") or DEFAULT_S3_BUCKET


def run_daily_ingestion(
    target_date: Optional[Union[str, date]] = None,
    tickers: Optional[str] = None,
    s3_bucket: Optional[str] = None,
    raw_prefix: Optional[str] = None,
    period: Optional[str] = None,
    interval: Optional[str] = None,
) -> Dict[str, object]:
    target = _coerce_target_date(target_date)
    dt = target.strftime("%Y-%m-%d")

    if target.weekday() > 4:  # 5=Sat, 6=Sun
        return {"dt": dt, "skipped": True, "uris": []}

    tickers_value = tickers or os.getenv("TICKERS") or DEFAULT_TICKERS
    bucket_value = _resolve_s3_bucket(s3_bucket)
    prefix_value = raw_prefix or os.getenv("RAW_PREFIX") or DEFAULT_RAW_PREFIX
    period_value = period or DEFAULT_PERIOD
    interval_value = interval or DEFAULT_INTERVAL

    scraper = B3Scraper(
        tickers=tickers_value,
        period=period_value,     # small window to guarantee D-1 availability
        interval=interval_value,
    )

    uris: List[str] = scraper.save_to_s3_partitioned(
        bucket=bucket_value,
        prefix=prefix_value,
        dt=dt,
    )

    return {
        "dt": dt,
        "skipped": False,
        "uris": uris,
    }


def main() -> None:
    result = run_daily_ingestion()
    if result["skipped"]:
        print(f"Skipping ingestion: {result['dt']} is a weekend.")
        return

    for uri in result["uris"]:
        print(f"Ingested: {uri}")


if __name__ == "__main__":
    main()
