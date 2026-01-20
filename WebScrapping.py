import argparse
from typing import Iterable, List, Optional, Union
import yfinance as yf
from __future__ import annotations
from dataclasses import dataclass
import pandas as pd
import boto3
from io import BytesIO
import pyarrow as pa
import pyarrow.parquet as pq

def normalize_tickers(raw: str) -> List[str]:
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
        data = download_data(
            self.tickers, self.start, self.end, self.period, self.interval
        )
        return normalize_download(data, self.tickers)

    def save(self) -> str:
        df = self.fetch()
        self.row_count = len(df)
        df.to_parquet(self.output, index=False)
        return self.output


def download_data(
    tickers: List[str],
    start: Optional[str],
    end: Optional[str],
    period: str,
    interval: str,
) -> pd.DataFrame:
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
    if isinstance(data.columns, pd.MultiIndex):
        try:
            stacked = data.stack(level=0, future_stack=True)
        except TypeError:
            stacked = data.stack(level=0)
        df = stacked.rename_axis(index=["date", "ticker"]).reset_index()
    else:
        df = data.reset_index()
        df.insert(1, "ticker", tickers[0] if tickers else "")

    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    if "datetime" in df.columns and "date" not in df.columns:
        df = df.rename(columns={"datetime": "date"})

    ordered = ["date", "ticker", "open", "high", "low", "close", "adj_close", "volume"]
    ordered = [c for c in ordered if c in df.columns]
    if ordered:
        df = df[ordered]

    return df


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download B3 data from yfinance and save to Parquet."
    )
    parser.add_argument(
        "--tickers",
        required=True,
        help="Comma-separated tickers (e.g., PETR4,VALE3 or PETR4.SA,VALE3.SA)",
    )
    parser.add_argument("--start", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", help="End date YYYY-MM-DD")
    parser.add_argument(
        "--period",
        default="1y",
        help="Used when start/end are not provided (e.g., 1mo, 1y, 5y, max)",
    )
    parser.add_argument(
        "--interval",
        default="1d",
        help="Data interval (e.g., 1d, 1h, 5m)",
    )
    parser.add_argument("--output", default="b3_data.parquet", help="Output parquet path")
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
    path = scraper.save()
    print(f"Saved {scraper.row_count} rows to {path}")


if __name__ == "__main__":
    main()
