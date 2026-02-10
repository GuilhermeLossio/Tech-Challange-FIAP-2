from __future__ import annotations

import os
from typing import Optional

from fastapi import FastAPI, Header, HTTPException

from ingestion.run_ingestion import run_daily_ingestion


app = FastAPI(title="Ingestion API", version="1.0.0")


def _check_secret(authorization: Optional[str], x_cron_secret: Optional[str]) -> None:
    secret = os.getenv("CRON_SECRET")
    if not secret:
        return
    if authorization == f"Bearer {secret}" or x_cron_secret == secret:
        return
    raise HTTPException(status_code=401, detail="Unauthorized")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/ingestion/run")
def run_ingestion(
    dt: Optional[str] = None,
    tickers: Optional[str] = None,
    s3_bucket: Optional[str] = None,
    raw_prefix: Optional[str] = None,
    period: Optional[str] = None,
    interval: Optional[str] = None,
    authorization: Optional[str] = Header(None),
    x_cron_secret: Optional[str] = Header(None),
) -> dict:
    _check_secret(authorization, x_cron_secret)

    try:
        result = run_daily_ingestion(
            target_date=dt,
            tickers=tickers,
            s3_bucket=s3_bucket,
            raw_prefix=raw_prefix,
            period=period,
            interval=interval,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {
        "date": result["dt"],
        "skipped": result["skipped"],
        "uris": result["uris"],
    }
