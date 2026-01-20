import argparse
from datetime import date

from WebScrapping import B3Scraper


DEFAULT_TICKERS = "GOLL4,AZUL4,EMBR3,EVEB31"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download B3 data with default airline sector tickers and save to Parquet."
    )
    parser.add_argument(
        "--tickers",
        default=DEFAULT_TICKERS,
        help=(
            "Comma-separated tickers "
            f"(default: {DEFAULT_TICKERS}; accepts PETR4,VALE3 or PETR4.SA,VALE3.SA)"
        ),
    )
    parser.add_argument("--start", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", help="End date YYYY-MM-DD")
    parser.add_argument(
        "--period",
        default="1y",
        help="Used when start/end are not provided (e.g., 1mo, 1y, 5y, max)",
    )
    
    parser.add_argument("--s3-bucket", required=True, help="S3 bucket for the data lake")
    parser.add_argument("--s3-prefix", default="raw", help="Base prefix (default raw)")

    
    
    parser.add_argument(
        "--interval",
        default="1d",
        help="Data interval (e.g., 1d, 1h, 5m)",
    )
    parser.add_argument("--output", default="b3_data.parquet", help="Output parquet path")
    
    parser.add_argument("--dt", default=str(date.today()), help="Partition date YYYY-MM-DD (default today)")

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
    
    # This part will be removed on future. It only applies on local test.
    # path = scraper.save()
    # print(path)
    
        
    
    df = scraper.to_dataframe()
    out_uri = scraper.save_partitioned_to_s3(
        df=df,
        bucket=args.s3_bucket,
        prefix=args.s3_prefix,
        dt=args.dt,
    )
    print(out_uri)


if __name__ == "__main__":
    main()
