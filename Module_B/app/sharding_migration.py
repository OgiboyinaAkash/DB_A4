"""
Assignment 4 sharding migration utility for ShopStop.

What this script does:
1) Reads source tables (Customer, Sale, SaleItem, Payment)
2) Partitions rows by hash(CustomerID) -> shard_id = CustomerID % shard_count
3) Creates shard tables using template (default: shard_{shard_id}_{table})
4) Upserts data into each shard table
5) Verifies no-loss and no-duplicate-ID guarantees

Usage examples:
  python sharding_migration.py
  python sharding_migration.py --clean-target
  python sharding_migration.py --report ..\\sharding_migration_report.json
"""

from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pymysql
from pymysql.cursors import DictCursor

from sql_project_store import SQLProjectStore


SHARDED_TABLES: List[Tuple[str, str, str]] = [
    ("customers", "Customer", "CustomerID"),
    ("sales", "Sale", "SaleID"),
    ("sale_items", "SaleItem", "SaleItemID"),
    ("payments", "Payment", "PaymentID"),
]


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


def _connect_with_config(config: Dict[str, Any]):
    return pymysql.connect(
        host=config["host"],
        port=int(config["port"]),
        user=config["user"],
        password=config["password"],
        database=config["database"],
        cursorclass=DictCursor,
        autocommit=True,
    )


def _quote(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def _fetch_rows(conn, query: str, args: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
    with conn.cursor() as cursor:
        cursor.execute(query, args)
        return list(cursor.fetchall())


def _fetch_create_sql(source_conn, base_table: str) -> str:
    with source_conn.cursor() as cursor:
        cursor.execute(f"SHOW CREATE TABLE {_quote(base_table)}")
        row = cursor.fetchone()
    if not row:
        raise RuntimeError(f"Unable to fetch CREATE TABLE for {base_table}")

    # DictCursor keys are typically: Table, Create Table
    create_sql = row.get("Create Table")
    if create_sql:
        return create_sql

    # Fallback for unexpected key naming
    values = list(row.values())
    if len(values) >= 2:
        return str(values[1])
    raise RuntimeError(f"Unexpected SHOW CREATE TABLE shape for {base_table}: {row}")


def _build_shard_create_sql(create_sql: str, target_table: str) -> str:
    """
    Generate a shard table DDL from a base table DDL.
    Foreign key constraints are removed to keep shard tables independent.
    """
    lines = create_sql.splitlines()
    body_lines: List[str] = []
    for line in lines[1:]:
        stripped = line.strip()
        if stripped.startswith(")"):
            continue
        upper = stripped.upper()
        if "FOREIGN KEY" in upper or upper.startswith("CONSTRAINT"):
            continue
        body_lines.append(stripped.rstrip(","))

    if not body_lines:
        raise RuntimeError(f"Failed to derive table body for {target_table}")

    formatted_body = ",\n  ".join(body_lines)
    return f"CREATE TABLE IF NOT EXISTS {_quote(target_table)} (\n  {formatted_body}\n) ENGINE=InnoDB"


def _ensure_table(
    target_conn,
    target_table: str,
    base_table: str,
    base_create_sql: str,
    clean_target: bool,
) -> None:
    with target_conn.cursor() as cursor:
        try:
            cursor.execute(
                f"CREATE TABLE IF NOT EXISTS {_quote(target_table)} LIKE {_quote(base_table)}"
            )
        except Exception:
            create_sql = _build_shard_create_sql(base_create_sql, target_table)
            cursor.execute(create_sql)

        if clean_target:
            cursor.execute(f"TRUNCATE TABLE {_quote(target_table)}")


def _format_shard_table_name(store: SQLProjectStore, logical_name: str, shard_id: int) -> str:
    cfg = store._cfg(logical_name)
    table_template = store.shard_table_template or "shard_{shard_id}_{table}"
    try:
        return table_template.format(
            shard_id=shard_id,
            table=cfg["table"].lower(),
            table_name=cfg["table"].lower(),
        )
    except Exception:
        return f"shard_{shard_id}_{cfg['table'].lower()}"


def _pick_shard(shard_count: int, customer_id: Optional[Any], fallback_id: Any) -> int:
    if customer_id is not None:
        return int(customer_id) % shard_count
    return int(fallback_id) % shard_count


def _group_customers(store: SQLProjectStore, rows: Iterable[Dict[str, Any]]) -> Dict[int, List[Dict[str, Any]]]:
    grouped: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        shard_id = _pick_shard(store.num_shards, row.get("CustomerID"), row.get("CustomerID"))
        grouped[shard_id].append(row)
    return grouped


def _group_sales(store: SQLProjectStore, rows: Iterable[Dict[str, Any]]) -> Dict[int, List[Dict[str, Any]]]:
    grouped: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        shard_id = _pick_shard(store.num_shards, row.get("CustomerID"), row.get("SaleID"))
        grouped[shard_id].append(row)
    return grouped


def _group_child_rows(
    store: SQLProjectStore,
    rows: Iterable[Dict[str, Any]],
    fallback_id_field: str,
) -> Dict[int, List[Dict[str, Any]]]:
    grouped: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        shard_customer_id = row.get("_ShardCustomerID")
        shard_id = _pick_shard(store.num_shards, shard_customer_id, row.get(fallback_id_field))
        cleaned = {k: v for k, v in row.items() if k != "_ShardCustomerID"}
        grouped[shard_id].append(cleaned)
    return grouped


def _columns_for_table(store: SQLProjectStore, logical_name: str) -> List[str]:
    cfg = store._cfg(logical_name)
    return [cfg["pk"]] + list(cfg["cols"])


def _upsert_rows(
    conn,
    table_name: str,
    columns: List[str],
    pk_column: str,
    rows: List[Dict[str, Any]],
) -> int:
    if not rows:
        return 0

    col_sql = ", ".join(_quote(col) for col in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    update_columns = [col for col in columns if col != pk_column]

    if update_columns:
        update_sql = ", ".join(f"{_quote(col)} = VALUES({_quote(col)})" for col in update_columns)
        query = (
            f"INSERT INTO {_quote(table_name)} ({col_sql}) VALUES ({placeholders}) "
            f"ON DUPLICATE KEY UPDATE {update_sql}"
        )
    else:
        query = f"INSERT IGNORE INTO {_quote(table_name)} ({col_sql}) VALUES ({placeholders})"

    payload = [tuple(row.get(col) for col in columns) for row in rows]
    with conn.cursor() as cursor:
        cursor.executemany(query, payload)
    return len(rows)


def _count_rows(conn, table_name: str) -> int:
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) AS cnt FROM {_quote(table_name)}")
        row = cursor.fetchone() or {}
    return int(row.get("cnt", 0))


def _fetch_id_set(conn, table_name: str, pk_column: str) -> set:
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT {_quote(pk_column)} AS pk FROM {_quote(table_name)}")
        rows = cursor.fetchall() or []
    return {int(row["pk"]) for row in rows if row.get("pk") is not None}


def run_migration(clean_target: bool, report_path: Path) -> Dict[str, Any]:
    _load_env_file()
    store = SQLProjectStore()

    source_config = {
        "host": os.getenv("MYSQL_SOURCE_HOST", store.host),
        "port": SQLProjectStore._env_int("MYSQL_SOURCE_PORT", store.port),
        "user": os.getenv("MYSQL_SOURCE_USER", store.user),
        "password": os.getenv("MYSQL_SOURCE_PASSWORD", store.password),
        "database": os.getenv("MYSQL_SOURCE_DATABASE", store.database),
    }

    source_conn = _connect_with_config(source_config)
    shard_conns = {
        shard_id: _connect_with_config(store.shard_nodes[shard_id])
        for shard_id in range(store.num_shards)
    }

    try:
        base_create_sql = {
            base_table: _fetch_create_sql(source_conn, base_table)
            for _, base_table, _ in SHARDED_TABLES
        }

        for shard_id, conn in shard_conns.items():
            for logical_name, base_table, _ in SHARDED_TABLES:
                target_table = _format_shard_table_name(store, logical_name, shard_id)
                if target_table.lower() == base_table.lower():
                    raise RuntimeError(
                        f"Refusing migration: target table {target_table} equals source table {base_table}"
                    )
                _ensure_table(
                    target_conn=conn,
                    target_table=target_table,
                    base_table=base_table,
                    base_create_sql=base_create_sql[base_table],
                    clean_target=clean_target,
                )

        customers_rows = _fetch_rows(source_conn, "SELECT * FROM `Customer`")
        sales_rows = _fetch_rows(source_conn, "SELECT * FROM `Sale`")
        sale_item_rows = _fetch_rows(
            source_conn,
            """
            SELECT
              si.`SaleItemID`,
              si.`SaleID`,
              si.`ProductID`,
              si.`Quantity`,
              si.`UnitPrice`,
              s.`CustomerID` AS _ShardCustomerID
            FROM `SaleItem` si
            LEFT JOIN `Sale` s ON s.`SaleID` = si.`SaleID`
            """,
        )
        payment_rows = _fetch_rows(
            source_conn,
            """
            SELECT
              p.`PaymentID`,
              p.`SaleID`,
              p.`PaymentMethod`,
              p.`Amount`,
              p.`PaymentDate`,
              s.`CustomerID` AS _ShardCustomerID
            FROM `Payment` p
            LEFT JOIN `Sale` s ON s.`SaleID` = p.`SaleID`
            """,
        )

        grouped_payloads = {
            "customers": _group_customers(store, customers_rows),
            "sales": _group_sales(store, sales_rows),
            "sale_items": _group_child_rows(store, sale_item_rows, fallback_id_field="SaleID"),
            "payments": _group_child_rows(store, payment_rows, fallback_id_field="SaleID"),
        }

        inserted_counts: Dict[str, Dict[str, int]] = {
            logical: {str(shard_id): 0 for shard_id in range(store.num_shards)}
            for logical, _, _ in SHARDED_TABLES
        }

        for logical_name, _, pk_column in SHARDED_TABLES:
            columns = _columns_for_table(store, logical_name)
            for shard_id in range(store.num_shards):
                rows = grouped_payloads.get(logical_name, {}).get(shard_id, [])
                target_table = _format_shard_table_name(store, logical_name, shard_id)
                migrated = _upsert_rows(
                    conn=shard_conns[shard_id],
                    table_name=target_table,
                    columns=columns,
                    pk_column=pk_column,
                    rows=rows,
                )
                inserted_counts[logical_name][str(shard_id)] = migrated

        source_counts = {
            "customers": len(customers_rows),
            "sales": len(sales_rows),
            "sale_items": len(sale_item_rows),
            "payments": len(payment_rows),
        }

        target_counts: Dict[str, Dict[str, int]] = {}
        overlap_report: Dict[str, List[int]] = {}
        integrity: Dict[str, Dict[str, Any]] = {}

        for logical_name, _, pk_column in SHARDED_TABLES:
            table_counts: Dict[str, int] = {}
            seen_ids: set = set()
            overlap_ids: set = set()

            for shard_id in range(store.num_shards):
                target_table = _format_shard_table_name(store, logical_name, shard_id)
                count = _count_rows(shard_conns[shard_id], target_table)
                table_counts[str(shard_id)] = count

                current_ids = _fetch_id_set(shard_conns[shard_id], target_table, pk_column)
                overlap_ids |= seen_ids.intersection(current_ids)
                seen_ids |= current_ids

            table_counts["total"] = sum(table_counts[str(shard_id)] for shard_id in range(store.num_shards))
            target_counts[logical_name] = table_counts
            overlap_report[logical_name] = sorted(overlap_ids)

            integrity[logical_name] = {
                "source_count": source_counts[logical_name],
                "target_count": table_counts["total"],
                "no_data_loss": source_counts[logical_name] == table_counts["total"],
                "no_duplicate_ids": len(overlap_ids) == 0,
            }

        overall_ok = all(
            check["no_data_loss"] and check["no_duplicate_ids"]
            for check in integrity.values()
        )

        report = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "clean_target": clean_target,
            "source": source_config,
            "shards": {
                str(shard_id): {
                    "host": store.shard_nodes[shard_id]["host"],
                    "port": store.shard_nodes[shard_id]["port"],
                    "database": store.shard_nodes[shard_id]["database"],
                }
                for shard_id in range(store.num_shards)
            },
            "inserted_counts": inserted_counts,
            "source_counts": source_counts,
            "target_counts": target_counts,
            "overlap_ids": overlap_report,
            "integrity": integrity,
            "overall_pass": overall_ok,
        }

        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        return report
    finally:
        source_conn.close()
        for conn in shard_conns.values():
            conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Migrate and validate Assignment 4 sharded tables")
    parser.add_argument(
        "--clean-target",
        action="store_true",
        help="Truncate shard tables before migration",
    )
    parser.add_argument(
        "--report",
        default=str(Path(__file__).resolve().parents[1] / "sharding_migration_report.json"),
        help="Output JSON report path",
    )
    args = parser.parse_args()

    report_path = Path(args.report)
    report = run_migration(clean_target=args.clean_target, report_path=report_path)

    print("Sharding migration completed")
    print(f"Report: {report_path}")
    print(f"Overall pass: {report.get('overall_pass')}")
    for table_name, check in report.get("integrity", {}).items():
        print(
            f"- {table_name}: source={check['source_count']} target={check['target_count']} "
            f"no_loss={check['no_data_loss']} no_dupe={check['no_duplicate_ids']}"
        )

    return 0 if report.get("overall_pass") else 2


if __name__ == "__main__":
    raise SystemExit(main())
