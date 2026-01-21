"""
Daily ingestion entrypoint.

Responsibilities:
- Define the ingestion date (D-1)
- Define the tickers to be ingested
- Trigger raw data ingestion into S3 (Parquet, partitioned by dt)

This script intentionally contains no business logic or transformations.
"""

from datetime import date, timedelta

from WebScrapping import B3Scraper

# ---------------------------------------------------------
# Execution context
# ---------------------------------------------------------

# Industry standard: ingest previous business day (D-1)
DT = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

# Airline sector tickers (B3)
TICKERS = "GOLL4,AZUL4,EMBR3,EVEB31"

# Raw data lake location
S3_BUCKET = "aeronaticalverifier-s3"
RAW_PREFIX = "raw"

# ---------------------------------------------------------
# Ingestion
# ---------------------------------------------------------

def main() -> None:
    scraper = B3Scraper(
        tickers=TICKERS,
        period="5d",     # small window to guarantee D-1 availability
        interval="1d",
    )

    uris = scraper.save_to_s3_partitioned(
        bucket=S3_BUCKET,
        prefix=RAW_PREFIX,
        dt=DT,
    )

    for uri in uris:
        print(f"Ingested: {uri}")


if __name__ == "__main__":
    main()
