"""Entry point for Pharma Sales ETL pipeline."""
import argparse
import logging
import sys

from src.pipeline import run_pipeline


def main() -> int:
    parser = argparse.ArgumentParser(description="Pharma Sales ETL Pipeline")
    parser.add_argument(
        "--mode",
        choices=["sample", "full"],
        default="sample",
        help="Use data/sample (sample) or data/raw (full)",
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Path to YAML config (optional)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Verbose logging",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )

    run_pipeline(mode=args.mode, config_path=args.config)
    return 0


if __name__ == "__main__":
    sys.exit(main())
