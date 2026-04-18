"""
MySQL-backed store for project-specific tables.
This layer powers API CRUD so SQL indexes and EXPLAIN plans are meaningful.
"""

import hashlib
import json
import os
from typing import Any, Dict, List, Optional, Tuple

import pymysql
from pymysql.cursors import DictCursor


class SQLProjectStore:
    DEFAULT_SHARDED_TABLES = {"customers", "sales", "sale_items", "payments"}

    TABLE_CONFIG = {
        "members": {
            "table": "Member",
            "pk": "MemberID",
            "cols": ["Name", "Image", "Age", "Email", "ContactNumber", "Role", "CreatedAt"],
            "api_to_db": {
                "member_id": "MemberID",
                "name": "Name",
                "image": "Image",
                "age": "Age",
                "email": "Email",
                "contact_number": "ContactNumber",
                "role": "Role",
                "created_at": "CreatedAt",
            },
        },
        "products": {
            "table": "Product",
            "pk": "ProductID",
            "cols": ["Name", "Price", "StockQuantity", "ReorderLevel", "CategoryID"],
            "api_to_db": {
                "product_id": "ProductID",
                "name": "Name",
                "price": "Price",
                "stock_quantity": "StockQuantity",
                "reorder_level": "ReorderLevel",
                "category_id": "CategoryID",
            },
        },
        "categories": {
            "table": "Category",
            "pk": "CategoryID",
            "cols": ["CategoryName", "Description", "CreatedAt"],
            "api_to_db": {
                "category_id": "CategoryID",
                "category_name": "CategoryName",
                "description": "Description",
                "created_at": "CreatedAt",
            },
        },
        "customers": {
            "table": "Customer",
            "pk": "CustomerID",
            "cols": ["Name", "Email", "ContactNumber", "LoyaltyPoints", "CreatedAt"],
            "api_to_db": {
                "customer_id": "CustomerID",
                "name": "Name",
                "email": "Email",
                "contact_number": "ContactNumber",
                "loyalty_points": "LoyaltyPoints",
                "created_at": "CreatedAt",
            },
        },
        "staff": {
            "table": "Staff",
            "pk": "StaffID",
            "cols": ["Name", "Role", "Salary", "ContactNumber", "JoinDate", "MemberID"],
            "api_to_db": {
                "staff_id": "StaffID",
                "name": "Name",
                "role": "Role",
                "salary": "Salary",
                "contact_number": "ContactNumber",
                "join_date": "JoinDate",
                "member_id": "MemberID",
            },
        },
        "suppliers": {
            "table": "Supplier",
            "pk": "SupplierID",
            "cols": ["Name", "ContactNumber", "Email", "Address"],
            "api_to_db": {
                "supplier_id": "SupplierID",
                "name": "Name",
                "contact_number": "ContactNumber",
                "email": "Email",
                "address": "Address",
            },
        },
        "purchase_orders": {
            "table": "PurchaseOrder",
            "pk": "POID",
            "cols": ["SupplierID", "OrderDate", "TotalAmount", "Status"],
            "api_to_db": {
                "poid": "POID",
                "supplier_id": "SupplierID",
                "order_date": "OrderDate",
                "total_amount": "TotalAmount",
                "status": "Status",
            },
        },
        "purchase_order_items": {
            "table": "PurchaseOrderItem",
            "pk": "POItemID",
            "cols": ["POID", "ProductID", "Quantity", "CostPrice"],
            "api_to_db": {
                "po_item_id": "POItemID",
                "poid": "POID",
                "product_id": "ProductID",
                "quantity": "Quantity",
                "cost_price": "CostPrice",
            },
        },
        "sales": {
            "table": "Sale",
            "pk": "SaleID",
            "cols": ["CustomerID", "StaffID", "SaleDate", "TotalAmount"],
            "api_to_db": {
                "sale_id": "SaleID",
                "customer_id": "CustomerID",
                "staff_id": "StaffID",
                "sale_date": "SaleDate",
                "total_amount": "TotalAmount",
            },
        },
        "sale_items": {
            "table": "SaleItem",
            "pk": "SaleItemID",
            "cols": ["SaleID", "ProductID", "Quantity", "UnitPrice"],
            "api_to_db": {
                "sale_item_id": "SaleItemID",
                "sale_id": "SaleID",
                "product_id": "ProductID",
                "quantity": "Quantity",
                "unit_price": "UnitPrice",
            },
        },
        "payments": {
            "table": "Payment",
            "pk": "PaymentID",
            "cols": ["SaleID", "PaymentMethod", "Amount", "PaymentDate"],
            "api_to_db": {
                "payment_id": "PaymentID",
                "sale_id": "SaleID",
                "payment_method": "PaymentMethod",
                "amount": "Amount",
                "payment_date": "PaymentDate",
            },
        },
        "attendance": {
            "table": "Attendance",
            "pk": "AttendanceID",
            "cols": ["StaffID", "EntryTime", "ExitTime", "WorkDate"],
            "api_to_db": {
                "attendance_id": "AttendanceID",
                "staff_id": "StaffID",
                "entry_time": "EntryTime",
                "exit_time": "ExitTime",
                "work_date": "WorkDate",
            },
        },
    }

    @staticmethod
    def _env_int(name: str, default: int) -> int:
        value = os.getenv(name)
        if value is None or str(value).strip() == "":
            return default
        try:
            return int(value)
        except ValueError:
            return default

    @staticmethod
    def _env_bool(name: str, default: bool = False) -> bool:
        value = os.getenv(name)
        if value is None:
            return default
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    def __init__(self):
        self.host = os.getenv("MYSQL_HOST", "127.0.0.1")
        self.port = self._env_int("MYSQL_PORT", 3306)
        self.user = os.getenv("MYSQL_USER", "root")
        self.password = os.getenv("MYSQL_PASSWORD", "")
        self.database = os.getenv("MYSQL_DATABASE", "outlet_management")
        self.sharding_enabled = self._env_bool("MYSQL_ENABLE_SHARDING", False)
        self.num_shards = max(1, self._env_int("MYSQL_SHARD_COUNT", 3))
        self.primary_shard_id = self._normalize_shard_id(self._env_int("MYSQL_PRIMARY_SHARD", 0))
        self.shard_table_template = os.getenv("MYSQL_SHARD_TABLE_TEMPLATE", "shard_{shard_id}_{table}")

        sharded_tables_raw = os.getenv("MYSQL_SHARDED_TABLES", "")
        if sharded_tables_raw.strip():
            self.sharded_tables = {
                table_name.strip()
                for table_name in sharded_tables_raw.split(",")
                if table_name.strip()
            }
        else:
            self.sharded_tables = set(self.DEFAULT_SHARDED_TABLES)

        self.shard_nodes: List[Dict[str, Any]] = []
        for shard_id in range(self.num_shards):
            self.shard_nodes.append(
                {
                    "host": os.getenv(f"MYSQL_SHARD_{shard_id}_HOST", self.host),
                    "port": self._env_int(f"MYSQL_SHARD_{shard_id}_PORT", self.port),
                    "user": os.getenv(f"MYSQL_SHARD_{shard_id}_USER", self.user),
                    "password": os.getenv(f"MYSQL_SHARD_{shard_id}_PASSWORD", self.password),
                    "database": os.getenv(f"MYSQL_SHARD_{shard_id}_DATABASE", self.database),
                }
            )

    def _normalize_shard_id(self, shard_id: int) -> int:
        if self.num_shards <= 0:
            return 0
        return int(shard_id) % self.num_shards

    def _connect_source(self):
        """Connect to source database (non-sharded tables)."""
        return pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            cursorclass=DictCursor,
            autocommit=True,
        )

    def _connect(self, shard_id: Optional[int] = None, use_source: bool = False):
        """Connect to database. If use_source=True, always use source DB (for non-sharded tables)."""
        if use_source or not self.sharding_enabled:
            return self._connect_source()
        
        target_shard = self.primary_shard_id if shard_id is None else self._normalize_shard_id(shard_id)
        node = self.shard_nodes[target_shard]
        return pymysql.connect(
            host=node["host"],
            port=node["port"],
            user=node["user"],
            password=node["password"],
            database=node["database"],
            cursorclass=DictCursor,
            autocommit=True,
        )

    def ping(self) -> Tuple[bool, str]:
        try:
            with self._connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1 AS ok")
                    cursor.fetchone()
            if self.sharding_enabled:
                return True, f"connected (primary shard={self.primary_shard_id})"
            return True, "connected"
        except Exception as exc:
            return False, str(exc)

    def sharding_info(self) -> Dict[str, Any]:
        return {
            "enabled": self.sharding_enabled,
            "shard_count": self.num_shards,
            "primary_shard": self.primary_shard_id,
            "sharded_tables": sorted(self.sharded_tables),
            "table_template": self.shard_table_template,
            "shards": [
                {
                    "shard_id": shard_id,
                    "host": node["host"],
                    "port": node["port"],
                    "database": node["database"],
                    "user": node["user"],
                }
                for shard_id, node in enumerate(self.shard_nodes)
            ],
        }

    def _cfg(self, table_name: str) -> Dict[str, Any]:
        if table_name not in self.TABLE_CONFIG:
            raise ValueError(f"Unsupported table '{table_name}'")
        return self.TABLE_CONFIG[table_name]

    def _db_to_api(self, table_name: str, row: Dict[str, Any]) -> Dict[str, Any]:
        cfg = self._cfg(table_name)
        reverse = {v: k for k, v in cfg["api_to_db"].items()}
        return {reverse.get(key, key): value for key, value in row.items()}

    def _to_db_payload(self, table_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        cfg = self._cfg(table_name)
        mapped = {}
        for key, value in payload.items():
            db_col = cfg["api_to_db"].get(key)
            if db_col:
                mapped[db_col] = value
        return mapped

    def _is_sharded_table(self, table_name: str) -> bool:
        return self.sharding_enabled and table_name in self.sharded_tables

    def _table_for_query(self, table_name: str, cfg: Dict[str, Any], shard_id: Optional[int]) -> str:
        if not self._is_sharded_table(table_name):
            return cfg["table"]

        target_shard = self.primary_shard_id if shard_id is None else self._normalize_shard_id(shard_id)
        try:
            return self.shard_table_template.format(
                shard_id=target_shard,
                table=cfg["table"].lower(),
                table_name=cfg["table"].lower(),
            )
        except Exception:
            return f"shard_{target_shard}_{cfg['table'].lower()}"

    def _shard_for_customer_id(self, customer_id: Any) -> int:
        try:
            return self._normalize_shard_id(int(customer_id))
        except (TypeError, ValueError) as exc:
            raise ValueError("CustomerID/customer_id must be an integer for shard routing") from exc

    def _build_select_query(
        self,
        table_ref: str,
        filters: Optional[Dict[str, Any]] = None,
        order_by: Optional[str] = None,
    ) -> Tuple[str, List[Any]]:
        sql = [f"SELECT * FROM {table_ref}"]
        args: List[Any] = []

        if filters:
            clauses = []
            for column, value in filters.items():
                if value is None:
                    continue
                if isinstance(value, tuple) and len(value) == 3 and value[0] == "COL_OP":
                    _, op, rhs_col = value
                    clauses.append(f"{column} {op} {rhs_col}")
                elif isinstance(value, tuple) and len(value) == 2:
                    op, val = value
                    clauses.append(f"{column} {op} %s")
                    args.append(val)
                else:
                    clauses.append(f"{column} = %s")
                    args.append(value)
            if clauses:
                sql.append("WHERE " + " AND ".join(clauses))

        if order_by:
            sql.append(f"ORDER BY {order_by}")

        return " ".join(sql), args

    @staticmethod
    def _extract_exact_filter_value(filters: Optional[Dict[str, Any]], column: str) -> Optional[Any]:
        if not filters or column not in filters:
            return None

        value = filters[column]
        if isinstance(value, tuple):
            if len(value) == 2:
                op, rhs = value
                if str(op).strip() in {"=", "=="}:
                    return rhs
            return None
        return value

    def _find_sales_shard_by_sale_id(self, sale_id: Any) -> Optional[int]:
        cfg = self._cfg("sales")
        for shard_id in range(self.num_shards):
            table_ref = self._table_for_query("sales", cfg, shard_id)
            query = f"SELECT 1 AS found FROM {table_ref} WHERE {cfg['pk']} = %s LIMIT 1"
            try:
                with self._connect(shard_id) as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(query, (sale_id,))
                        if cursor.fetchone() is not None:
                            return shard_id
            except Exception:
                continue
        return None

    def _find_record_shard(self, table_name: str, record_id: Any) -> Optional[int]:
        if not self._is_sharded_table(table_name):
            return None

        if table_name == "customers":
            return self._shard_for_customer_id(record_id)

        cfg = self._cfg(table_name)
        for shard_id in range(self.num_shards):
            table_ref = self._table_for_query(table_name, cfg, shard_id)
            query = f"SELECT 1 AS found FROM {table_ref} WHERE {cfg['pk']} = %s LIMIT 1"
            try:
                with self._connect(shard_id) as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(query, (record_id,))
                        if cursor.fetchone() is not None:
                            return shard_id
            except Exception:
                continue

        return None

    def _target_shards_for_list(self, table_name: str, filters: Optional[Dict[str, Any]]) -> List[Optional[int]]:
        if not self._is_sharded_table(table_name):
            return [None]

        all_shards = list(range(self.num_shards))
        if table_name == "customers":
            customer_id = self._extract_exact_filter_value(filters, "CustomerID")
            if customer_id is not None:
                return [self._shard_for_customer_id(customer_id)]
            return all_shards

        if table_name == "sales":
            customer_id = self._extract_exact_filter_value(filters, "CustomerID")
            if customer_id is not None:
                return [self._shard_for_customer_id(customer_id)]
            return all_shards

        if table_name in {"sale_items", "payments"}:
            sale_id = self._extract_exact_filter_value(filters, "SaleID")
            if sale_id is None:
                return all_shards
            shard_id = self._find_sales_shard_by_sale_id(sale_id)
            if shard_id is None:
                return []
            return [shard_id]

        return all_shards

    @staticmethod
    def _sort_rows(rows: List[Dict[str, Any]], order_by: Optional[str]) -> List[Dict[str, Any]]:
        if not order_by:
            return rows

        parts = order_by.strip().split()
        if not parts:
            return rows

        column = parts[0]
        reverse = len(parts) > 1 and parts[1].upper() == "DESC"
        try:
            return sorted(rows, key=lambda row: (row.get(column) is None, row.get(column)), reverse=reverse)
        except TypeError:
            return rows

    def _next_global_id(self, table_name: str) -> int:
        cfg = self._cfg(table_name)
        max_seen = 0
        queried_any = False
        last_error: Optional[Exception] = None

        shard_targets: List[Optional[int]]
        if self._is_sharded_table(table_name):
            shard_targets = list(range(self.num_shards))
        else:
            shard_targets = [None]

        for shard_id in shard_targets:
            table_ref = self._table_for_query(table_name, cfg, shard_id)
            query = f"SELECT COALESCE(MAX({cfg['pk']}), 0) AS max_id FROM {table_ref}"
            try:
                with self._connect(shard_id, use_source=not self._is_sharded_table(table_name)) as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(query)
                        row = cursor.fetchone() or {}
                        current_max = int(row.get("max_id") or 0)
                        max_seen = max(max_seen, current_max)
                        queried_any = True
            except Exception as exc:
                last_error = exc
                if not self._is_sharded_table(table_name):
                    raise

        if not queried_any and last_error is not None:
            raise last_error

        return max_seen + 1

    def _infer_insert_shard(self, table_name: str, payload: Dict[str, Any], mapped: Dict[str, Any]) -> Optional[int]:
        if not self._is_sharded_table(table_name):
            return None

        if table_name == "customers":
            customer_id = mapped.get("CustomerID")
            if customer_id is None:
                customer_id = payload.get("customer_id", payload.get("CustomerID"))
            if customer_id is None:
                raise ValueError("customer_id is required to route customer inserts in sharded mode")
            mapped["CustomerID"] = int(customer_id)
            return self._shard_for_customer_id(customer_id)

        if table_name == "sales":
            customer_id = mapped.get("CustomerID")
            if customer_id is None:
                customer_id = payload.get("customer_id", payload.get("CustomerID"))
            if customer_id is None:
                raise ValueError("customer_id is required to route sale inserts in sharded mode")
            return self._shard_for_customer_id(customer_id)

        if table_name in {"sale_items", "payments"}:
            sale_id = mapped.get("SaleID")
            if sale_id is None:
                sale_id = payload.get("sale_id", payload.get("SaleID"))
            if sale_id is None:
                raise ValueError("sale_id is required to route this insert in sharded mode")
            shard_id = self._find_sales_shard_by_sale_id(sale_id)
            if shard_id is None:
                raise ValueError(f"Unable to route insert for sale_id={sale_id}: sale not found on any shard")
            return shard_id

        return None

    def list_records(self, table_name: str, filters: Optional[Dict[str, Any]] = None, order_by: Optional[str] = None) -> List[Dict[str, Any]]:
        cfg = self._cfg(table_name)
        if not self._is_sharded_table(table_name):
            query, args = self._build_select_query(cfg["table"], filters=filters, order_by=order_by)
            with self._connect(use_source=True) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, args)
                    rows = cursor.fetchall()
            return [self._db_to_api(table_name, row) for row in rows]

        target_shards = self._target_shards_for_list(table_name, filters)
        if not target_shards:
            return []

        merged_rows: List[Dict[str, Any]] = []
        for shard_id in target_shards:
            table_ref = self._table_for_query(table_name, cfg, shard_id)
            query, args = self._build_select_query(table_ref, filters=filters, order_by=None)
            with self._connect(shard_id) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, args)
                    merged_rows.extend(cursor.fetchall())

        sorted_rows = self._sort_rows(merged_rows, order_by)
        return [self._db_to_api(table_name, row) for row in sorted_rows]

    def get_record(self, table_name: str, record_id: Any) -> Optional[Dict[str, Any]]:
        cfg = self._cfg(table_name)
        shard_id: Optional[int] = None
        if self._is_sharded_table(table_name):
            shard_id = self._find_record_shard(table_name, record_id)
            if shard_id is None:
                return None

        table_ref = self._table_for_query(table_name, cfg, shard_id)
        query = f"SELECT * FROM {table_ref} WHERE {cfg['pk']} = %s"
        with self._connect(shard_id, use_source=not self._is_sharded_table(table_name)) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (record_id,))
                row = cursor.fetchone()
        if row is None:
            return None
        return self._db_to_api(table_name, row)

    def create_record(self, table_name: str, payload: Dict[str, Any]) -> int:
        cfg = self._cfg(table_name)
        mapped = self._to_db_payload(table_name, payload)

        if self._is_sharded_table(table_name) and cfg["pk"] not in mapped:
            mapped[cfg["pk"]] = self._next_global_id(table_name)

        shard_id = self._infer_insert_shard(table_name, payload, mapped)
        table_ref = self._table_for_query(table_name, cfg, shard_id)

        cols = []
        values = []
        args: List[Any] = []

        if cfg["pk"] in mapped:
            cols.append(cfg["pk"])
            values.append("%s")
            args.append(mapped[cfg["pk"]])

        for col in cfg["cols"]:
            if col in mapped:
                cols.append(col)
                values.append("%s")
                args.append(mapped[col])

        if not cols:
            raise ValueError("No valid insert columns were provided")

        query = f"INSERT INTO {table_ref} ({', '.join(cols)}) VALUES ({', '.join(values)})"

        try:
            with self._connect(shard_id, use_source=not self._is_sharded_table(table_name)) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, args)
                    if cfg["pk"] in mapped:
                        return int(mapped[cfg["pk"]])
                    return int(cursor.lastrowid)
        except pymysql.IntegrityError as e:
            if "Duplicate entry" in str(e):
                record_id = mapped.get(cfg["pk"], "unknown")
                pk_name = cfg["pk"]
                raise ValueError(
                    f"Record {pk_name} '{record_id}' already exists. You can try updating the existing record or create a new record with a different {pk_name}."
                )
            raise

    def update_record(self, table_name: str, record_id: Any, payload: Dict[str, Any]) -> bool:
        cfg = self._cfg(table_name)
        mapped = self._to_db_payload(table_name, payload)
        mapped.pop(cfg["pk"], None)

        shard_id: Optional[int] = None
        if self._is_sharded_table(table_name):
            shard_id = self._find_record_shard(table_name, record_id)
            if shard_id is None:
                return False

        if not mapped:
            raise ValueError("No updatable fields provided")

        set_parts = [f"{col} = %s" for col in mapped.keys()]
        args = list(mapped.values()) + [record_id]
        table_ref = self._table_for_query(table_name, cfg, shard_id)
        query = f"UPDATE {table_ref} SET {', '.join(set_parts)} WHERE {cfg['pk']} = %s"

        with self._connect(shard_id, use_source=not self._is_sharded_table(table_name)) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, args)
                return cursor.rowcount > 0

    def delete_record(self, table_name: str, record_id: Any) -> bool:
        cfg = self._cfg(table_name)

        shard_id: Optional[int] = None
        if self._is_sharded_table(table_name):
            shard_id = self._find_record_shard(table_name, record_id)
            if shard_id is None:
                return False

        table_ref = self._table_for_query(table_name, cfg, shard_id)
        query = f"DELETE FROM {table_ref} WHERE {cfg['pk']} = %s"
        with self._connect(shard_id, use_source=not self._is_sharded_table(table_name)) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (record_id,))
                return cursor.rowcount > 0

    def table_state(self, table_name: str) -> Tuple[int, str]:
        cfg = self._cfg(table_name)
        pk = cfg["pk"]

        rows: List[Dict[str, Any]] = []
        if self._is_sharded_table(table_name):
            for shard_id in range(self.num_shards):
                table_ref = self._table_for_query(table_name, cfg, shard_id)
                query = f"SELECT * FROM {table_ref} ORDER BY {pk} ASC"
                with self._connect(shard_id) as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(query)
                        rows.extend(cursor.fetchall())
            try:
                rows = sorted(rows, key=lambda row: row.get(pk))
            except TypeError:
                pass
        else:
            query = f"SELECT * FROM {cfg['table']} ORDER BY {pk} ASC"
            with self._connect(use_source=True) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    rows = cursor.fetchall()

        digest = hashlib.sha256()
        for row in rows:
            digest.update(str(row.get(pk)).encode("utf-8"))
            digest.update(b"|")
            digest.update(json.dumps(row, sort_keys=True, default=str).encode("utf-8"))
            digest.update(b";")

        return len(rows), digest.hexdigest()
