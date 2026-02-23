from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date as dt_date
from io import BytesIO
from typing import Iterable, List, Optional, Union

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yfinance as yf


def normalize_tickers(raw: str) -> List[str]:
    """
    Normalize a comma-separated string of tickers.
    Ensures B3 tickers end with '.SA' unless they are indices (e.g. ^BVSP).
    """
    tickers: List[str] = []
    for part in raw.split(","):
        ticker = part.strip().upper()
        if not ticker:
            continue
        if "." not in ticker and not ticker.startswith("^"):
            ticker = f"{ticker}.SA"
        tickers.append(ticker)
    return tickers


def normalize_tickers_input(raw: Union[str, Iterable[str]]) -> List[str]:
    """
    Accepts either a string or an iterable of tickers and normalizes them.
    """
    if isinstance(raw, str):
        return normalize_tickers(raw)

    tickers: List[str] = []
    for part in raw:
        ticker = str(part).strip().upper()
        if not ticker:
            continue
        if "." not in ticker and not ticker.startswith("^"):
            ticker = f"{ticker}.SA"
        tickers.append(ticker)
    return tickers


def download_data(
    tickers: List[str],
    start: Optional[str],
    end: Optional[str],
    period: str,
    interval: str,
) -> pd.DataFrame:
    """
    Download historical market data from yfinance.
    """
    if not tickers:
        raise ValueError("No tickers provided.")

    kwargs = {
        "tickers": " ".join(tickers),
        "interval": interval,
        "group_by": "ticker",
        "auto_adjust": False,
        "threads": True,
        "progress": False,
    }
    if start or end:
        if start:
            kwargs["start"] = start
        if end:
            kwargs["end"] = end
    else:
        kwargs["period"] = period

    data = yf.download(**kwargs)
    if data.empty:
        raise ValueError("No data returned. Check tickers and date range.")
    return data


def normalize_download(data: pd.DataFrame, tickers: List[str]) -> pd.DataFrame:
    """
    Normalize yfinance output into a flat tabular format.
    Output schema:
      date, ticker, open, high, low, close, adj_close, volume
    """
    # yfinance returns MultiIndex columns when multiple tickers are requested
    if isinstance(data.columns, pd.MultiIndex):
        try:
            stacked = data.stack(level=0, future_stack=True)
        except TypeError:
            # backward compatibility for older pandas
            stacked = data.stack(level=0)
        df = stacked.rename_axis(index=["date", "ticker"]).reset_index()
    else:
        df = data.reset_index()
        df.insert(1, "ticker", tickers[0] if tickers else "")

    # Normalize column names
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    if "datetime" in df.columns and "date" not in df.columns:
        df = df.rename(columns={"datetime": "date"})

    # Enforce column order when possible
    ordered = ["date", "ticker", "open", "high", "low", "close", "adj_close", "volume"]
    ordered = [c for c in ordered if c in df.columns]
    if ordered:
        df = df[ordered]

    # Minimal type normalization
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["ticker"] = df["ticker"].astype(str)

    return df


def _write_parquet_bytes(df: pd.DataFrame) -> bytes:
    """
    Serialize a DataFrame into Parquet bytes using PyArrow.
    """
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    return buf.getvalue()


def _dt_str(d: dt_date) -> str:
    """
    Format a date object as YYYY-MM-DD.
    """
    return d.strftime("%Y-%m-%d")


def _normalize_s3_prefix(prefix: Optional[str]) -> str:
    value = str(prefix or "raw").strip().strip("/")
    if not value:
        return "raw"
    if value.lower().startswith("unsaved"):
        return "raw"
    return value


@dataclass
class B3Scraper:
    def __init__(
        self,
        tickers: Union[str, Iterable[str]],
        start: Optional[str] = None,
        end: Optional[str] = None,
        period: str = "1y",
        interval: str = "1d",
        output: str = "b3_data.parquet",
    ) -> None:
        self.tickers = normalize_tickers_input(tickers)
        if not self.tickers:
            raise ValueError("No tickers provided.")
        self.start = start
        self.end = end
        self.period = period
        self.interval = interval
        self.output = output
        self.row_count = 0

    def fetch(self) -> pd.DataFrame:
        """
        Fetch and normalize B3 data using yfinance.
        """
        data = download_data(
            self.tickers, self.start, self.end, self.period, self.interval
        )
        return normalize_download(data, self.tickers)

    def save(self) -> str:
        """
        Original behavior: save the dataset locally as a Parquet file.
        """
        df = self.fetch()
        self.row_count = len(df)
        df.to_parquet(self.output, index=False)
        return self.output

    def save_to_s3_partitioned(
        self,
        bucket: str,
        prefix: str = "raw",
        dt: Optional[str] = None,
        one_file_per_day: bool = True,
    ) -> List[str]:
        """
        Save data to S3 in Parquet format partitioned by day.

        Target layout:
          s3://{bucket}/{prefix}/dt=YYYY-MM-DD/data.parquet

        - If dt is provided, only that partition is written.
        - If dt is omitted, one partition is written per day in the dataset
          (useful for backfills).
        """
        df = self.fetch()
        self.row_count = len(df)
        safe_prefix = _normalize_s3_prefix(prefix)

        s3 = boto3.client("s3")
        keys_written: List[str] = []

        if dt:
            target = pd.to_datetime(dt).date()
            df_day = df[df["date"] == target].copy()
            if df_day.empty:
                raise ValueError(f"No data found for dt={dt}.")
            body = _write_parquet_bytes(df_day)
            fname = "data.parquet" if one_file_per_day else f"data_{_dt_str(target)}.parquet"
            key = f"{safe_prefix}/dt={_dt_str(target)}/{fname}"
            s3.put_object(Bucket=bucket, Key=key, Body=body)
            keys_written.append(f"s3://{bucket}/{key}")
            return keys_written

        # Write one partition per distinct date (backfill mode)
        for d in sorted(df["date"].dropna().unique()):
            df_day = df[df["date"] == d].copy()
            if df_day.empty:
                continue
            body = _write_parquet_bytes(df_day)
            fname = "data.parquet" if one_file_per_day else f"data_{_dt_str(d)}.parquet"
            key = f"{safe_prefix}/dt={_dt_str(d)}/{fname}"
            s3.put_object(Bucket=bucket, Key=key, Body=body)
            keys_written.append(f"s3://{bucket}/{key}")

        if not keys_written:
            raise ValueError("No partitions were written to S3.")

        return keys_written


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download B3 data from yfinance and save to Parquet (local) or S3 (partitioned)."
    )
    parser.add_argument(
        "--tickers",
        required=True,
        help="Comma-separated tickers (e.g., GOLL4,AZUL4,EMBR3 or GOLL4.SA,AZUL4.SA)",
    )
    parser.add_argument("--start", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", help="End date YYYY-MM-DD")
    parser.add_argument("--period", default="1y")
    parser.add_argument("--interval", default="1d")

    # Local output
    parser.add_argument("--output", default="b3_data.parquet", help="Output parquet path (local)")

    # S3 output (raw zone)
    parser.add_argument("--s3-bucket", help="If provided, also writes to S3 partitioned by dt")
    parser.add_argument("--s3-prefix", default="raw", help="S3 prefix base (default: raw)")
    parser.add_argument("--dt", help="Partition date YYYY-MM-DD (optional)")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    scraper = B3Scraper(
        tickers=args.tickers,
        start=args.start,
        end=args.end,
        period=args.period,
        interval=args.interval,
        output=args.output,
    )

    # Preserve original behavior
    path = scraper.save()
    print(f"Saved {scraper.row_count} rows to {path}")

    # Optional S3 ingestion (raw layer)
    if args.s3_bucket:
        uris = scraper.save_to_s3_partitioned(
            bucket=args.s3_bucket,
            prefix=args.s3_prefix,
            dt=args.dt,
        )
        print("Written to S3:")
        for u in uris:
            print(" -", u)


if __name__ == "__main__":
    main()
