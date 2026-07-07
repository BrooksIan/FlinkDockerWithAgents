#!/usr/bin/env python3
"""Fetch NASA NEO feed and publish near-earth objects to the nasa.neo Kafka topic."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any

TOPIC = "nasa.neo"
DEFAULT_START_DATE = "2015-09-07"
DEFAULT_END_DATE = "2015-09-08"
NEO_FEED_URL = "https://api.nasa.gov/neo/rest/v1/feed"


def fetch_neo_feed(
    *,
    api_key: str,
    start_date: str,
    end_date: str,
) -> dict[str, Any]:
    from ratatoskr.httpio.fetch import append_query, http_fetch_json

    url = append_query(
        NEO_FEED_URL,
        {"start_date": start_date, "end_date": end_date, "api_key": api_key},
    )
    result = http_fetch_json(url)
    if not result.get("ok"):
        raise RuntimeError(result.get("error") or f"HTTP {result.get('status_code')}")
    data = result["data"]
    if not isinstance(data, dict):
        raise RuntimeError("Unexpected NASA NEO response shape")
    return data


def neo_records(data: dict[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for date, objects in (data.get("near_earth_objects") or {}).items():
        if not isinstance(objects, list):
            continue
        for neo in objects:
            if not isinstance(neo, dict):
                continue
            records.append(
                {
                    "key": str(neo.get("id") or neo.get("neo_reference_id") or ""),
                    "value": neo,
                }
            )
    return records


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--start-date",
        default=os.environ.get("NASA_NEO_START_DATE", DEFAULT_START_DATE),
        help=f"Feed start date (default: {DEFAULT_START_DATE})",
    )
    parser.add_argument(
        "--end-date",
        default=os.environ.get("NASA_NEO_END_DATE", DEFAULT_END_DATE),
        help=f"Feed end date (default: {DEFAULT_END_DATE})",
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("NASA_API_KEY", "DEMO_KEY"),
        help="NASA API key (default: DEMO_KEY or NASA_API_KEY env)",
    )
    parser.add_argument(
        "--bootstrap",
        default=None,
        help="Kafka bootstrap servers (default: auto-detect)",
    )
    args = parser.parse_args()

    repo = Path(__file__).resolve().parents[1]
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))

    from ratatoskr.kafka_sources import kafka_bootstrap_servers, publish_topic_records

    print(f"Fetching NASA NEO feed {args.start_date} → {args.end_date} …")
    data = fetch_neo_feed(
        api_key=args.api_key,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    records = neo_records(data)
    element_count = data.get("element_count", len(records))
    print(f"API returned {element_count} elements ({len(records)} records)")

    bootstrap = args.bootstrap or kafka_bootstrap_servers()
    count = publish_topic_records(TOPIC, records, bootstrap=bootstrap)
    print(f"Published {count} messages to {TOPIC!r} (bootstrap={bootstrap})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
