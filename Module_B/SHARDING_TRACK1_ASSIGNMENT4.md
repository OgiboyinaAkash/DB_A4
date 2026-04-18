# ShopStop - Track 1 Assignment 4 (Sharding)

Group Name: SELECT___SQUAD

## 1) Shard Key Selection and Justification

Chosen shard key: `CustomerID`

Why this key:
- High cardinality: Customer IDs naturally grow with customer base, allowing spread across shards.
- Query aligned: Existing APIs frequently filter by customer_id in sales and customer endpoints.
- Stable: CustomerID is immutable after insert.

Partitioning strategy: Hash based

Shard formula:
- `shard_id = CustomerID % 3`

Expected distribution:
- For sequential IDs, modulo hashing gives near-even split among 3 shards.
- Small temporary skew may happen for low row counts, but long-run balance improves as data grows.

## 2) What Was Implemented

### A) Data Partitioning and Shard Routing

Implemented in:
- `Module_B/app/sql_project_store.py`

Sharded logical tables:
- `customers`
- `sales`
- `sale_items`
- `payments`

Physical shard table naming (default):
- `shard_0_customer`, `shard_1_customer`, `shard_2_customer`
- `shard_0_sale`, `shard_1_sale`, `shard_2_sale`
- `shard_0_saleitem`, `shard_1_saleitem`, `shard_2_saleitem`
- `shard_0_payment`, `shard_1_payment`, `shard_2_payment`

Routing behavior:
- Lookup queries:
  - `customers`: direct route via `CustomerID % 3`
  - `sales/sale_items/payments`: shard discovery using key lookup across shards if required
- Insert operations:
  - `customers`: route by `customer_id`
  - `sales`: route by payload `customer_id`
  - `sale_items/payments`: route by the shard holding parent `SaleID`
- Range queries:
  - Query relevant shards (or all when key range is broad), merge rows in application, then sort.

### B) API Debug Support for Demo

Implemented in:
- `Module_B/app/api/routes.py`

Optional routing metadata is returned when either is provided:
- Query param: `include_shard_debug=1`
- Header: `X-Shard-Debug: 1`

Supported endpoints:
- `GET /api/project/<table>`
- `GET /api/project/<table>/<id>`
- `POST /api/project/<table>` (single insert)

### C) Shard Migration and Validation Tooling

Implemented in:
- `Module_B/app/sharding_migration.py`
- `Module_B/app/sharding_routing_demo.py`

`sharding_migration.py` capabilities:
- Creates shard tables if missing
- Partitions source rows using `CustomerID % shard_count`
- Upserts rows into shard tables
- Validates:
  - source count == total shard count (no loss)
  - no duplicate IDs across shards
- Generates JSON report for evidence

`sharding_routing_demo.py` capabilities:
- Demonstrates lookup routing for a customer ID
- Demonstrates range query shard targeting (sales)
- Optional insert routing demo with cleanup
- Generates JSON report for video/report screenshots

## 3) Configuration

Configured in:
- `Module_B/.env`

Important variables:
- `MYSQL_ENABLE_SHARDING`
- `MYSQL_SHARD_COUNT`
- `MYSQL_SHARD_TABLE_TEMPLATE`
- `MYSQL_SHARD_0_*`, `MYSQL_SHARD_1_*`, `MYSQL_SHARD_2_*`

### Provided shard details used

DB shard ports (MySQL protocol used by app/scripts):
- Shard 1 -> 3307
- Shard 2 -> 3308
- Shard 3 -> 3309

Corrected phpMyAdmin/UI ports (as per instructor correction):
- Shard 1 -> 8081
- Shard 2 -> 8082
- Shard 3 -> 8083

Mapping in code config:
- `shard_id=0` maps to Shard 1
- `shard_id=1` maps to Shard 2
- `shard_id=2` maps to Shard 3

## 4) How To Run

From `Module_B/app`:

1. Enable sharding in `.env`:
- `MYSQL_ENABLE_SHARDING=true`

2. Run migration:
```bash
python sharding_migration.py --clean-target
```
**Output**: `../sharding_migration_report.json` (in Module_B directory)

3. Generate routing demo evidence:
```bash
python sharding_routing_demo.py --customer-id 1 --start-date "2025-04-01 00:00:00" --insert-demo
```
**Output**: `../sharding_routing_demo_report.json` (in Module_B directory)

4. Start API:
```bash
python app.py
```

5. Example debug requests:
```text
GET /api/project/customers/1?include_shard_debug=1
GET /api/project/sales?start_date=2025-04-01%2000:00:00&include_shard_debug=1
POST /api/project/customers?include_shard_debug=1
```

## 5) phpMyAdmin UI Access for Verification

You can access each shard's data directly via phpMyAdmin web interface for visual verification:

**Shard 1 (shard_id=0):**
- URL: `http://10.0.116.184:8081`
- Username: `SELECT___Squad`
- Password: `password@123`
- Database: `SELECT___Squad`

**Shard 2 (shard_id=1):**
- URL: `http://10.0.116.184:8082`
- Username: `SELECT___Squad`
- Password: `password@123`
- Database: `SELECT___Squad`

**Shard 3 (shard_id=2):**
- URL: `http://10.0.116.184:8083`
- Username: `SELECT___Squad`
- Password: `password@123`
- Database: `SELECT___Squad`

After login, you can inspect the sharded tables to verify:
- Data partitioning across `shard_0_customer`, `shard_1_customer`, `shard_2_customer` (and similar for sales, sale_items, payments)
- Row counts per shard (approximately balanced for sequential customer IDs)
- Take screenshots as evidence for your report/video

## 6) Scalability and Trade-Off Analysis

Horizontal vs vertical scaling:
- Vertical scaling upgrades one server (CPU/RAM/storage limits eventually).
- Sharding scales horizontally by splitting data and load across nodes.

Consistency:
- Within one shard, consistency follows underlying DB guarantees.
- Cross-shard operations can observe temporary inconsistency if writes fail on subset of shards or retry logic is incomplete.

Availability:
- If one shard goes down, data on healthy shards remains available.
- Queries requiring unavailable shard return partial failure or not-found depending on access path.

Partition tolerance:
- Network issues can isolate a shard; application continues for reachable shards.
- Trade-off: system favors continued operation on reachable partitions over global completeness.

## 7) Verification Checklist

**Generated JSON Reports** (saved in `Module_B/` directory after running scripts):

**Migration Report** (`sharding_migration_report.json`):
- Location: `Module_B/sharding_migration_report.json`
- Generated by: `python sharding_migration.py --clean-target`
- Use to verify:
  - Correct partitioning with no overlap
  - No data loss after migration (source count == target count)
  - Per-shard row distribution
  - No duplicate IDs across shards

**Routing Demo Report** (`sharding_routing_demo_report.json`):
- Location: `Module_B/sharding_routing_demo_report.json`
- Generated by: `python sharding_routing_demo.py ...`
- Use to verify:
  - Lookup routes to expected shard
  - Insert routes to expected shard
  - Range queries target multiple shards when required
  - Sharding enabled/disabled status

**API Debug Fields** (via query params/headers):
Use API requests with `include_shard_debug=1` to verify:

## 8) Limitations

- Cross-shard joins are application-level and therefore more expensive.
- If `CustomerID` is null for parent-linked sales data, fallback routing uses primary key modulo.
- Full distributed transactions across shards are not implemented in this assignment scope.
