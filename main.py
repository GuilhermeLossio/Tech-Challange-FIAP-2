import argparse

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
    print(path)


if __name__ == "__main__":
    main()
