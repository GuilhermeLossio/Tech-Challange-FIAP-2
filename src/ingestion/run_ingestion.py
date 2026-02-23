"""
Ingestion orchestration module.

This module provides:
- Daily ingestion (D-1)
- Backfill ingestion for date ranges
- Optional refined triggering in Glue per partition date
"""

from __future__ import annotations

from calendar import monthrange
from datetime import date, timedelta
import os
from typing import Dict, Iterable, List, Optional, Set, Tuple, Union
import unicodedata

import boto3

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
# Helpers
# ---------------------------------------------------------

def _coerce_target_date(target_date: Optional[Union[str, date]]) -> date:
    if target_date is None:
        return date.today() - timedelta(days=1)
    if isinstance(target_date, date):
        return target_date
    try:
        return date.fromisoformat(target_date)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid target_date: {target_date}. Use YYYY-MM-DD.") from exc


def _coerce_required_date(value: Union[str, date], field_name: str) -> date:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid {field_name}: {value}. Use YYYY-MM-DD.") from exc


def _resolve_s3_bucket(explicit: Optional[str]) -> str:
    if explicit:
        return explicit
    return os.getenv("S3_BUCKET") or os.getenv("AWS_BUCKET") or DEFAULT_S3_BUCKET


def _resolve_raw_prefix(explicit: Optional[str]) -> str:
    raw = (explicit if explicit is not None else os.getenv("RAW_PREFIX")) or DEFAULT_RAW_PREFIX
    prefix = str(raw).strip().strip("/")
    if not prefix:
        return DEFAULT_RAW_PREFIX

    # Guardrail: never allow saving under "unsaved" prefix.
    if prefix.lower().startswith("unsaved"):
        return DEFAULT_RAW_PREFIX
    return prefix


def _resolve_glue_job_name(explicit: Optional[str]) -> str:
    if explicit:
        return explicit
    job_name = os.getenv("GLUE_JOB_NAME")
    if not job_name:
        raise ValueError("GLUE_JOB_NAME is required to trigger refined processing.")
    return job_name


def _normalize_month_token(value: str) -> str:
    # Handles inputs like "janeiro", "marco", "January", "Feb".
    norm = unicodedata.normalize("NFKD", value)
    ascii_only = norm.encode("ascii", "ignore").decode("ascii")
    return ascii_only.strip().lower()


def _coerce_month(month: Union[int, str]) -> int:
    if isinstance(month, int):
        if 1 <= month <= 12:
            return month
        raise ValueError(f"Invalid month number: {month}. Use 1..12.")

    token = _normalize_month_token(str(month))
    if not token:
        raise ValueError("Invalid month: empty value.")

    if token.isdigit():
        value = int(token)
        if 1 <= value <= 12:
            return value
        raise ValueError(f"Invalid month number: {month}. Use 1..12.")

    aliases = {
        1: {"jan", "january", "janeiro"},
        2: {"feb", "february", "fev", "fevereiro"},
        3: {"mar", "march", "marco"},
        4: {"apr", "april", "abr", "abril"},
        5: {"may", "mai", "maio"},
        6: {"jun", "june", "junho"},
        7: {"jul", "july", "julho"},
        8: {"aug", "august", "ago", "agosto"},
        9: {"sep", "september", "set", "setembro"},
        10: {"oct", "october", "out", "outubro"},
        11: {"nov", "november", "novembro"},
        12: {"dec", "december", "dez", "dezembro"},
    }

    for month_number, names in aliases.items():
        if token in names:
            return month_number

    raise ValueError(
        f"Invalid month: {month}. Use month number (1..12) or names like january/janeiro."
    )


def _month_start_end(year: int, month: int) -> Tuple[date, date]:
    if year < 1900 or year > 2100:
        raise ValueError("year must be between 1900 and 2100.")
    last_day = monthrange(year, month)[1]
    return date(year, month, 1), date(year, month, last_day)


def _extract_partition_date_from_uri(uri: str) -> str:
    # Expected format: s3://bucket/prefix/dt=YYYY-MM-DD/data.parquet
    try:
        path = uri.split("://", 1)[1].split("/", 1)[1]
    except IndexError as exc:
        raise ValueError(f"Invalid S3 URI format: {uri}") from exc

    for part in path.split("/"):
        if part.startswith("dt="):
            dt = part[3:]
            try:
                date.fromisoformat(dt)
            except ValueError as exc:
                raise ValueError(f"Invalid dt partition in URI: {uri}") from exc
            return dt

    raise ValueError(f"Could not find dt partition in URI: {uri}")


def trigger_refined_glue_jobs(
    partition_dates: Iterable[str],
    glue_job_name: Optional[str] = None,
) -> List[Dict[str, str]]:
    dates = sorted({str(d) for d in partition_dates if d})
    if not dates:
        return []

    job_name = _resolve_glue_job_name(glue_job_name)
    glue = boto3.client("glue")

    runs: List[Dict[str, str]] = []
    for dt in dates:
        # Validate before dispatch.
        date.fromisoformat(dt)
        response = glue.start_job_run(JobName=job_name, Arguments={"--dt": dt})
        runs.append({"dt": dt, "job_run_id": response.get("JobRunId", "")})
    return runs


# ---------------------------------------------------------
# Daily ingestion
# ---------------------------------------------------------

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
    prefix_value = _resolve_raw_prefix(raw_prefix)
    period_value = period or DEFAULT_PERIOD
    interval_value = interval or DEFAULT_INTERVAL

    scraper = B3Scraper(
        tickers=tickers_value,
        period=period_value,  # small window to guarantee D-1 availability
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


# ---------------------------------------------------------
# Backfill ingestion
# ---------------------------------------------------------

def run_backfill_ingestion(
    start_date: Union[str, date],
    end_date: Union[str, date],
    tickers: Optional[str] = None,
    s3_bucket: Optional[str] = None,
    raw_prefix: Optional[str] = None,
    interval: Optional[str] = None,
    trigger_refined: bool = False,
    glue_job_name: Optional[str] = None,
) -> Dict[str, object]:
    start = _coerce_required_date(start_date, "start_date")
    end = _coerce_required_date(end_date, "end_date")

    if end < start:
        raise ValueError("end_date must be greater than or equal to start_date.")

    tickers_value = tickers or os.getenv("TICKERS") or DEFAULT_TICKERS
    bucket_value = _resolve_s3_bucket(s3_bucket)
    prefix_value = _resolve_raw_prefix(raw_prefix)
    interval_value = interval or DEFAULT_INTERVAL

    # yfinance end is exclusive; add one day to make end_date inclusive.
    end_exclusive = end + timedelta(days=1)

    scraper = B3Scraper(
        tickers=tickers_value,
        start=start.isoformat(),
        end=end_exclusive.isoformat(),
        interval=interval_value,
    )

    uris: List[str] = scraper.save_to_s3_partitioned(
        bucket=bucket_value,
        prefix=prefix_value,
        dt=None,  # write one partition per available market day in the range
    )

    partition_dates = sorted({_extract_partition_date_from_uri(uri) for uri in uris})

    glue_runs: List[Dict[str, str]] = []
    if trigger_refined:
        glue_runs = trigger_refined_glue_jobs(
            partition_dates=partition_dates,
            glue_job_name=glue_job_name,
        )

    return {
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "partition_dates": partition_dates,
        "uris": uris,
        "glue_runs": glue_runs,
    }


def run_month_ingestion(
    year: int,
    month: Union[int, str],
    tickers: Optional[str] = None,
    s3_bucket: Optional[str] = None,
    raw_prefix: Optional[str] = None,
    interval: Optional[str] = None,
    trigger_refined: bool = False,
    glue_job_name: Optional[str] = None,
) -> Dict[str, object]:
    month_number = _coerce_month(month)
    start, end = _month_start_end(year, month_number)

    result = run_backfill_ingestion(
        start_date=start,
        end_date=end,
        tickers=tickers,
        s3_bucket=s3_bucket,
        raw_prefix=raw_prefix,
        interval=interval,
        trigger_refined=trigger_refined,
        glue_job_name=glue_job_name,
    )

    return {
        "year": year,
        "month": month_number,
        "month_name": start.strftime("%B"),
        "start_date": result["start_date"],
        "end_date": result["end_date"],
        "partition_dates": result["partition_dates"],
        "uris": result["uris"],
        "glue_runs": result["glue_runs"],
    }


def run_months_ingestion(
    year: int,
    months: Iterable[Union[int, str]],
    tickers: Optional[str] = None,
    s3_bucket: Optional[str] = None,
    raw_prefix: Optional[str] = None,
    interval: Optional[str] = None,
    trigger_refined: bool = False,
    glue_job_name: Optional[str] = None,
) -> Dict[str, object]:
    month_numbers: List[int] = []
    seen: Set[int] = set()

    for month in months:
        month_number = _coerce_month(month)
        if month_number not in seen:
            seen.add(month_number)
            month_numbers.append(month_number)

    if not month_numbers:
        raise ValueError("months cannot be empty.")

    month_results: List[Dict[str, object]] = []
    all_partition_dates: Set[str] = set()
    all_uris: List[str] = []
    all_glue_runs: List[Dict[str, str]] = []

    for month_number in month_numbers:
        month_result = run_month_ingestion(
            year=year,
            month=month_number,
            tickers=tickers,
            s3_bucket=s3_bucket,
            raw_prefix=raw_prefix,
            interval=interval,
            trigger_refined=trigger_refined,
            glue_job_name=glue_job_name,
        )
        month_results.append(month_result)
        all_partition_dates.update(month_result["partition_dates"])
        all_uris.extend(month_result["uris"])
        all_glue_runs.extend(month_result["glue_runs"])

    return {
        "year": year,
        "months": month_numbers,
        "month_results": month_results,
        "partition_dates": sorted(all_partition_dates),
        "uris": all_uris,
        "glue_runs": all_glue_runs,
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

