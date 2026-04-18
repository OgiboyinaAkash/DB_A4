"""
Generate shard-routing evidence for Assignment 4 report/video.

This script demonstrates:
1) Single-key lookup routing (customer lookup)
2) Insert routing (optional demo insert)
3) Range-query routing across shards (sales by start_date)
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from sql_project_store import SQLProjectStore


def _load_env_file() -> None:
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def run_demo(customer_id: int, start_date: str, do_insert_demo: bool) -> Dict[str, Any]:
    _load_env_file()
    store = SQLProjectStore()

    if not store.sharding_enabled:
        return {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "sharding_enabled": False,
            "message": "MYSQL_ENABLE_SHARDING is false. Enable sharding to run routing demo.",
            "sharding_info": store.sharding_info(),
        }

    lookup_expected_shard = store._shard_for_customer_id(customer_id)
    lookup_routed_shard = store._find_record_shard("customers", customer_id)
    lookup_record = store.get_record("customers", customer_id)

    range_filters = {"SaleDate": (">=", start_date)}
    range_target_shards = store._target_shards_for_list("sales", range_filters)
    range_results = store.list_records("sales", filters=range_filters, order_by="SaleDate ASC")

    insert_demo = {
        "executed": False,
        "inserted_customer_id": None,
        "expected_shard": None,
        "routed_shard": None,
        "cleanup_deleted": None,
        "error": None,
    }

    if do_insert_demo:
        try:
            next_id = store._next_global_id("customers")
            payload = {
                "customer_id": next_id,
                "name": f"ShardDemoCustomer{next_id}",
                "email": f"shard.demo.{next_id}@shopstop.local",
                "contact_number": f"90000{next_id % 100000:05d}",
                "loyalty_points": 0,
                "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            }

            inserted_id = store.create_record("customers", payload)
            routed_shard = store._find_record_shard("customers", inserted_id)
            expected_shard = store._shard_for_customer_id(inserted_id)
            deleted = store.delete_record("customers", inserted_id)

            insert_demo.update(
                {
                    "executed": True,
                    "inserted_customer_id": inserted_id,
                    "expected_shard": expected_shard,
                    "routed_shard": routed_shard,
                    "cleanup_deleted": deleted,
                }
            )
        except Exception as exc:
            insert_demo.update(
                {
                    "executed": True,
                    "error": str(exc),
                }
            )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sharding_enabled": True,
        "sharding_info": store.sharding_info(),
        "lookup_demo": {
            "table": "customers",
            "record_id": customer_id,
            "expected_shard": lookup_expected_shard,
            "routed_shard": lookup_routed_shard,
            "found": lookup_record is not None,
        },
        "range_query_demo": {
            "table": "sales",
            "filters": range_filters,
            "target_shards": range_target_shards,
            "result_count": len(range_results),
            "sample_ids": [row.get("sale_id") for row in range_results[:10]],
        },
        "insert_demo": insert_demo,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate sharding routing demo evidence")
    parser.add_argument("--customer-id", type=int, default=1, help="Customer ID for lookup routing demo")
    parser.add_argument(
        "--start-date",
        default="2025-04-01 00:00:00",
        help="Start date for sales range query demo",
    )
    parser.add_argument(
        "--insert-demo",
        action="store_true",
        help="Run an insert+cleanup demo for routing proof",
    )
    parser.add_argument(
        "--output",
        default=str(Path(__file__).resolve().parents[1] / "sharding_routing_demo_report.json"),
        help="Output JSON report path",
    )
    args = parser.parse_args()

    report = run_demo(
        customer_id=args.customer_id,
        start_date=args.start_date,
        do_insert_demo=args.insert_demo,
    )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"Saved routing demo report: {output_path}")
    print(f"Sharding enabled: {report.get('sharding_enabled')}")
    if report.get("sharding_enabled"):
        lookup = report.get("lookup_demo", {})
        print(
            f"Lookup demo -> expected shard: {lookup.get('expected_shard')}, "
            f"routed shard: {lookup.get('routed_shard')}, found: {lookup.get('found')}"
        )
        range_demo = report.get("range_query_demo", {})
        print(
            f"Range demo -> target shards: {range_demo.get('target_shards')}, "
            f"result count: {range_demo.get('result_count')}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
