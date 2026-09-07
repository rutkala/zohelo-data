#!/usr/bin/env python3
"""Query released NBP values through the supported daily MetricFlow interface."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from semantic_query import METRICS, query_metric


def main():
    parser = argparse.ArgumentParser(description=(
        "Query one NBP daily source value with native MetricFlow. Required daily currency/source "
        "or commodity dimensions are supplied automatically. No sums, period averages, conversion "
        "or filling are supported. Arbitrary raw mf queries do not enforce this contract. "
        "Use a local DuckDB and the semantic_manifest.json restored from the same release. "
        "Linux/libseccomp and the pinned Python environment are required; execution stays offline."
    ))
    parser.add_argument("--list", action="store_true", help="List supported metric names")
    parser.add_argument("--database", type=Path)
    parser.add_argument("--semantic-manifest", type=Path)
    parser.add_argument("--metric", choices=tuple(METRICS))
    parser.add_argument("--start-date", help="Inclusive publication date, YYYY-MM-DD")
    parser.add_argument("--end-date", help="Inclusive publication date, YYYY-MM-DD")
    parser.add_argument("--output", type=Path, help="Destination CSV")
    args = parser.parse_args()
    if args.list:
        print("\n".join(METRICS))
        return 0
    for field in ("database", "semantic_manifest", "metric", "start_date", "end_date", "output"):
        if getattr(args, field) is None:
            parser.error(f"--{field.replace('_', '-')} is required unless --list is used")
    try:
        rows = query_metric(args.database, args.semantic_manifest, args.metric,
                            args.start_date, args.end_date, args.output)
    except (ValueError, RuntimeError, OSError) as exc:
        parser.exit(1, f"Metric query failed: {exc}\n")
    print(json.dumps({"status": "queried", "metric": args.metric,
                      "row_count": len(rows), "output": str(args.output)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
