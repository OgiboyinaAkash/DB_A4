import json
import os
import tempfile
from dataclasses import dataclass, field
from datetime import datetime

try:
    from .table import Table
except ImportError:
    from table import Table


_TYPE_BY_NAME = {
    "int": int,
    "float": float,
    "str": str,
    "bool": bool,
}


def _type_name(py_type):
    if py_type in (int, float, str, bool):
        return py_type.__name__
    raise ValueError(f"Unsupported schema type for persistence: {py_type}")


@dataclass
class Transaction:
    tx_id: int
    redo_log: list = field(default_factory=list)
    undo_log: list = field(default_factory=list)


class DatabaseManager:
    def __init__(self, persistence_dir=None):
        self.databases = {}  # Dictionary to store databases as {db_name: {table_name: Table instance}}
        self._transactions = {}
        self._next_tx_id = 1

        default_dir = os.path.join(os.path.dirname(__file__), ".acid_store")
        self.persistence_dir = persistence_dir or default_dir
        os.makedirs(self.persistence_dir, exist_ok=True)
        self._snapshot_file = os.path.join(self.persistence_dir, "snapshot.json")
        self._wal_file = os.path.join(self.persistence_dir, "wal.log")
        self._crash_wal_file = os.path.join(self.persistence_dir, "crash_wal.log")
        self._recover_from_disk()

    def create_database(self, db_name):
        """
        Create a new database with the given name.
        Initializes an empty dictionary for tables within this database.
        """
        if not db_name:
            return False, "Database name is required"
        if db_name in self.databases:
            return False, f"Database '{db_name}' already exists"
        self.databases[db_name] = {}
        self._persist_snapshot()
        return True, f"Database '{db_name}' created successfully"

    def delete_database(self, db_name):
        """
        Delete an existing database and all its tables.
        """
        if db_name not in self.databases:
            return False, f"Database '{db_name}' not found"
        del self.databases[db_name]
        self._persist_snapshot()
        return True, f"Database '{db_name}' deleted successfully"

    def list_databases(self):
        """
        Return a list of all database names currently managed.
        """
        return list(self.databases.keys())

    def create_table(self, db_name, table_name, schema, order=8, search_key=None):
        """
        Create a new table within a specified database.
        - schema: dictionary of column names and data types
        - order: B+ tree order for indexing
        - search_key: field name to use as the key in the B+ Tree
        """
        if db_name not in self.databases:
            return False, f"Database '{db_name}' not found"
        if not table_name:
            return False, "Table name is required"
        if not isinstance(schema, dict) or not schema:
            return False, "Schema must be a non-empty dictionary"
        if table_name in self.databases[db_name]:
            return False, f"Table '{table_name}' already exists in database '{db_name}'"
        if search_key is None:
            search_key = next(iter(schema.keys()))
        if search_key not in schema:
            return False, f"search_key '{search_key}' must be a field in schema"

        table = Table(table_name, schema, order=order, search_key=search_key)
        self.databases[db_name][table_name] = table
        self._persist_snapshot()
        return True, f"Table '{table_name}' created successfully"

    def delete_table(self, db_name, table_name):
        """
        Delete a table from the specified database.
        """
        if db_name not in self.databases:
            return False, f"Database '{db_name}' not found"
        if table_name not in self.databases[db_name]:
            return False, f"Table '{table_name}' not found in database '{db_name}'"
        del self.databases[db_name][table_name]
        self._persist_snapshot()
        return True, f"Table '{table_name}' deleted successfully"

    def list_tables(self, db_name):
        """
        List all tables within a given database.
        """
        if db_name not in self.databases:
            return None, f"Database '{db_name}' not found"
        return list(self.databases[db_name].keys()), "OK"

    def get_table(self, db_name, table_name):
        """
        Retrieve a Table instance from a given database.
        Useful for performing operations like insert, update, delete on that table.
        """
        if db_name not in self.databases:
            return None, f"Database '{db_name}' not found"
        table = self.databases[db_name].get(table_name)
        if table is None:
            return None, f"Table '{table_name}' not found in database '{db_name}'"
        return table, "OK"

    def begin_transaction(self):
        tx_id = self._next_tx_id
        self._next_tx_id += 1
        self._transactions[tx_id] = Transaction(tx_id=tx_id)
        return tx_id

    def commit_transaction(self, tx_id, simulate_crash_after_wal=False):
        tx = self._transactions.get(tx_id)
        if tx is None:
            return False, f"Transaction '{tx_id}' not found"

        self._append_wal_entry({
            "tx_id": tx.tx_id,
            "changes": tx.redo_log,
        })

        if simulate_crash_after_wal:
            # Preserve WAL entries to crash log before raising error
            self._preserve_wal_to_crash_log()
            raise RuntimeError("Simulated crash after WAL flush")

        self._persist_snapshot()
        self._clear_wal()
        del self._transactions[tx_id]
        return True, f"Transaction '{tx_id}' committed"

    def rollback_transaction(self, tx_id):
        tx = self._transactions.get(tx_id)
        if tx is None:
            return False, f"Transaction '{tx_id}' not found"

        for change in reversed(tx.undo_log):
            self._apply_change(change, direction="undo")

        del self._transactions[tx_id]
        return True, f"Transaction '{tx_id}' rolled back"

    def insert_record(self, db_name, table_name, record, tx_id=None):
        if tx_id is None:
            tx_id = self.begin_transaction()
            try:
                key = self.insert_record(db_name, table_name, record, tx_id=tx_id)
                committed, message = self.commit_transaction(tx_id)
                if not committed:
                    raise RuntimeError(message)
                return key
            except Exception:
                self.rollback_transaction(tx_id)
                raise

        tx = self._require_transaction(tx_id)
        change = self._insert_impl(db_name, table_name, record)
        tx.redo_log.append(change)
        tx.undo_log.append(change)
        return change["key"]

    def update_record(self, db_name, table_name, record_id, new_record, tx_id=None):
        if tx_id is None:
            tx_id = self.begin_transaction()
            try:
                self.update_record(db_name, table_name, record_id, new_record, tx_id=tx_id)
                committed, message = self.commit_transaction(tx_id)
                if not committed:
                    raise RuntimeError(message)
                return True
            except Exception:
                self.rollback_transaction(tx_id)
                raise

        tx = self._require_transaction(tx_id)
        change = self._update_impl(db_name, table_name, record_id, new_record)
        tx.redo_log.append(change)
        tx.undo_log.append(change)
        return True

    def delete_record(self, db_name, table_name, record_id, tx_id=None):
        if tx_id is None:
            tx_id = self.begin_transaction()
            try:
                self.delete_record(db_name, table_name, record_id, tx_id=tx_id)
                committed, message = self.commit_transaction(tx_id)
                if not committed:
                    raise RuntimeError(message)
                return True
            except Exception:
                self.rollback_transaction(tx_id)
                raise

        tx = self._require_transaction(tx_id)
        change = self._delete_impl(db_name, table_name, record_id)
        tx.redo_log.append(change)
        tx.undo_log.append(change)
        return True

    def run_transaction(self, operations, fail_after_ops=None):
        """
        Execute multiple operations as one atomic transaction.

        Operation format examples:
        {"action": "insert", "db_name": "core", "table_name": "users", "record": {...}}
        {"action": "update", "db_name": "core", "table_name": "users", "record_id": 1, "new_record": {...}}
        {"action": "delete", "db_name": "core", "table_name": "users", "record_id": 1}
        """
        tx_id = self.begin_transaction()
        results = []

        try:
            for index, operation in enumerate(operations):
                action = operation.get("action")

                if action == "insert":
                    key = self.insert_record(
                        operation["db_name"],
                        operation["table_name"],
                        operation["record"],
                        tx_id=tx_id,
                    )
                    results.append(key)
                elif action == "update":
                    self.update_record(
                        operation["db_name"],
                        operation["table_name"],
                        operation["record_id"],
                        operation["new_record"],
                        tx_id=tx_id,
                    )
                    results.append(True)
                elif action == "delete":
                    self.delete_record(
                        operation["db_name"],
                        operation["table_name"],
                        operation["record_id"],
                        tx_id=tx_id,
                    )
                    results.append(True)
                else:
                    raise ValueError(f"Unsupported transaction action: {action}")

                if fail_after_ops is not None and (index + 1) >= fail_after_ops:
                    raise RuntimeError("Simulated failure during transaction")

            committed, message = self.commit_transaction(tx_id)
            if not committed:
                return False, message
            return True, results
        except Exception as exc:
            self.rollback_transaction(tx_id)
            return False, str(exc)

    def validate_global_consistency(self):
        for tables in self.databases.values():
            for table in tables.values():
                table.assert_consistent()
        return True

    def _require_transaction(self, tx_id):
        tx = self._transactions.get(tx_id)
        if tx is None:
            raise ValueError(f"Transaction '{tx_id}' not found")
        return tx

    def _insert_impl(self, db_name, table_name, record):
        table = self._get_table_or_raise(db_name, table_name)
        key = record.get(table.search_key)
        if key is None:
            raise ValueError(f"search_key '{table.search_key}' must be present in record")

        before = table.get(key)
        if before is not None:
            raise ValueError(f"Record with id '{key}' already exists")

        inserted, message = table.insert(record)
        if not inserted:
            raise ValueError(message)

        after = table.get(key)
        table.assert_consistent()
        return {
            "db_name": db_name,
            "table_name": table_name,
            "key": key,
            "before": before,
            "after": after,
        }

    def _update_impl(self, db_name, table_name, record_id, new_record):
        table = self._get_table_or_raise(db_name, table_name)
        before = table.get(record_id)
        if before is None:
            raise ValueError(f"Record with id '{record_id}' not found")

        updated, message = table.update(record_id, new_record)
        if not updated:
            raise ValueError(message)

        after = table.get(record_id)
        table.assert_consistent()
        return {
            "db_name": db_name,
            "table_name": table_name,
            "key": record_id,
            "before": before,
            "after": after,
        }

    def _delete_impl(self, db_name, table_name, record_id):
        table = self._get_table_or_raise(db_name, table_name)
        before = table.get(record_id)
        if before is None:
            raise ValueError(f"Record with id '{record_id}' not found")

        deleted, message = table.delete(record_id)
        if not deleted:
            raise ValueError(message)

        after = table.get(record_id)
        table.assert_consistent()
        return {
            "db_name": db_name,
            "table_name": table_name,
            "key": record_id,
            "before": before,
            "after": after,
        }

    def _apply_change(self, change, direction):
        table = self._get_table_or_raise(change["db_name"], change["table_name"])
        target = change["after"] if direction == "redo" else change["before"]
        key = change["key"]

        if target is None:
            table.force_delete_record(key)
        else:
            table.force_set_record(key, target)

    def _get_table_or_raise(self, db_name, table_name):
        table, message = self.get_table(db_name, table_name)
        if table is None:
            raise ValueError(message)
        return table

    def _recover_from_disk(self):
        self.databases = {}

        if os.path.exists(self._snapshot_file):
            with open(self._snapshot_file, "r", encoding="utf-8") as handle:
                snapshot_data = json.load(handle)
            self._load_snapshot(snapshot_data)

        if os.path.exists(self._wal_file):
            with open(self._wal_file, "r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    entry = json.loads(line)
                    for change in entry.get("changes", []):
                        self._apply_change(change, direction="redo")

            self._persist_snapshot()
            self._clear_wal()

    def _persist_snapshot(self):
        payload = self._build_snapshot_payload()
        fd, temp_path = tempfile.mkstemp(prefix="snapshot_", suffix=".json", dir=self.persistence_dir)

        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())

            os.replace(temp_path, self._snapshot_file)
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def _build_snapshot_payload(self):
        payload = {"databases": {}}

        for db_name, tables in self.databases.items():
            payload["databases"][db_name] = {}
            for table_name, table in tables.items():
                payload["databases"][db_name][table_name] = {
                    "schema": {column: _type_name(py_type) for column, py_type in table.schema.items()},
                    "order": table.order,
                    "search_key": table.search_key,
                    "records": table.export_records(),
                }

        return payload

    def _load_snapshot(self, snapshot_data):
        databases = snapshot_data.get("databases", {})

        for db_name, tables in databases.items():
            self.databases[db_name] = {}

            for table_name, table_data in tables.items():
                schema_names = table_data.get("schema", {})
                schema = {
                    column: _TYPE_BY_NAME[type_name]
                    for column, type_name in schema_names.items()
                }

                table = Table(
                    table_name,
                    schema,
                    order=table_data.get("order", 8),
                    search_key=table_data.get("search_key"),
                )

                for record in table_data.get("records", []):
                    key = record[table.search_key]
                    table.force_set_record(key, record)

                self.databases[db_name][table_name] = table

    def _append_wal_entry(self, payload):
        with open(self._wal_file, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())

    def _clear_wal(self):
        with open(self._wal_file, "w", encoding="utf-8"):
            pass

    def _preserve_wal_to_crash_log(self):
        """Preserve WAL entries to crash log when crash is simulated."""
        if not os.path.exists(self._wal_file):
            return
        
        try:
            with open(self._wal_file, "r", encoding="utf-8") as wal_handle:
                wal_content = wal_handle.read()
            
            if wal_content.strip():  # Only preserve if WAL has content
                with open(self._crash_wal_file, "a", encoding="utf-8") as crash_handle:
                    crash_handle.write(f"=== CRASH WAL ENTRIES (Timestamp: {datetime.now().isoformat()}) ===\n")
                    crash_handle.write(wal_content)
                    crash_handle.write("\n")
                    crash_handle.flush()
                    os.fsync(crash_handle.fileno())
        except Exception as e:
            print(f"Warning: Failed to preserve WAL to crash log: {e}")

    def get_crash_wal_entries(self):
        """Retrieve all WAL entries preserved during crash scenarios."""
        if not os.path.exists(self._crash_wal_file):
            return []
        
        try:
            with open(self._crash_wal_file, "r", encoding="utf-8") as handle:
                content = handle.read()
            return content
        except Exception as e:
            return f"Error reading crash WAL: {e}"

    def clear_crash_wal(self):
        """Clear all crash WAL entries."""
        try:
            if os.path.exists(self._crash_wal_file):
                with open(self._crash_wal_file, "w", encoding="utf-8"):
                    pass
                return True, "Crash WAL log cleared"
        except Exception as e:
            return False, f"Error clearing crash WAL: {e}"
