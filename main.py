#!/usr/bin/env python3
"""Command-line interface for Spark Deadlock Detector."""

import argparse
import sys

from deadlock_detector.config import DEFAULT_TOP_K, MAX_CYCLE_LENGTH, create_spark_context
from deadlock_detector.pipeline import format_report, run_analysis


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="spark-deadlock-detector",
        description=(
            "Distributed Resource Allocation Graph (RAG) analyzer and "
            "deadlock cycle detector powered by Apache Spark (PySpark)."
        ),
    )
    parser.add_argument(
        "log_file",
        help="Path to the resource allocation log file (e.g., data/sample_hpc_rag.log).",
    )
    parser.add_argument(
        "num_cores",
        type=int,
        help="Number of local Spark worker cores (positive integer).",
    )
    parser.add_argument(
        "--max-cycle-length",
        type=int,
        default=MAX_CYCLE_LENGTH,
        help=f"Maximum directed cycle length in edges (default: {MAX_CYCLE_LENGTH}).",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=DEFAULT_TOP_K,
        help=f"Number of top contention hotspots to report (default: {DEFAULT_TOP_K}).",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Render a human-readable section-by-section summary instead of compact output.",
    )
    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.num_cores < 1:
        parser.error("num_cores must be a positive integer.")
    if args.max_cycle_length < 2:
        parser.error("--max-cycle-length must be at least 2.")
    if args.top_k < 1:
        parser.error("--top-k must be at least 1.")

    sc = create_spark_context(args.num_cores)
    try:
        result = run_analysis(
            sc=sc,
            log_file=args.log_file,
            num_cores=args.num_cores,
            max_cycle_length=args.max_cycle_length,
            top_k=args.top_k,
        )
        print(format_report(result, pretty=args.pretty))
    finally:
        sc.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main())
