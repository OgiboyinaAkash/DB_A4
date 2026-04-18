import tempfile
import unittest
import sys
from io import StringIO
from datetime import datetime

try:
    from .db_manager import DatabaseManager
except ImportError:
    from db_manager import DatabaseManager


class StructuredTestResult(unittest.TestResult):
    """Custom test result class to capture detailed test information"""
    
    def __init__(self, stream=None, descriptions=None, verbosity=None):
        # Handle compatibility with unittest framework
        if stream is None:
            stream = StringIO()
        if descriptions is None:
            descriptions = True
        if verbosity is None:
            verbosity = 1
        
        super().__init__(stream, descriptions, verbosity)
        self.test_details = {
            "atomicity": {"passed": False, "details": "", "error": ""},
            "consistency": {"passed": False, "details": "", "error": ""},
            "durability": {"passed": False, "details": "", "error": ""}
        }
        self.timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
    def startTest(self, test):
        super().startTest(test)
        
    def addSuccess(self, test):
        super().addSuccess(test)
        test_name = test._testMethodName
        
        if "atomicity" in test_name:
            self.test_details["atomicity"]["passed"] = True
            self.test_details["atomicity"]["details"] = (
                "Failure simulation executed successfully. "
                "Verified that all changes were rolled back (no partial data). "
                "Table consistency verified after rollback."
            )
        elif "consistency" in test_name:
            self.test_details["consistency"]["passed"] = True
            self.test_details["consistency"]["details"] = (
                "Invalid data type detected during transaction. "
                "Transaction rolled back automatically. "
                "Original data preserved and consistency maintained."
            )
        elif "durability" in test_name:
            self.test_details["durability"]["passed"] = True
            self.test_details["durability"]["details"] = (
                "Simulated crash during commit with Write-Ahead Log (WAL). "
                "Database restarted and recovery replayed committed WAL. "
                "All committed data persisted correctly after recovery."
            )
    
    def addError(self, test, err):
        super().addError(test, err)
        test_name = test._testMethodName
        error_msg = self._exc_info_to_string(err, test)
        
        if "atomicity" in test_name:
            self.test_details["atomicity"]["error"] = error_msg
        elif "consistency" in test_name:
            self.test_details["consistency"]["error"] = error_msg
        elif "durability" in test_name:
            self.test_details["durability"]["error"] = error_msg
    
    def addFailure(self, test, err):
        super().addFailure(test, err)
        test_name = test._testMethodName
        error_msg = self._exc_info_to_string(err, test)
        
        if "atomicity" in test_name:
            self.test_details["atomicity"]["error"] = error_msg
        elif "consistency" in test_name:
            self.test_details["consistency"]["error"] = error_msg
        elif "durability" in test_name:
            self.test_details["durability"]["error"] = error_msg
    
    def generate_report(self):
        """Generate structured test report"""
        report = []
        report.append("=" * 80)
        report.append("ACID VALIDATION TEST REPORT")
        report.append("=" * 80)
        report.append("")
        report.append(f"Total Tests Run: {self.testsRun}")
        report.append(f"Failures: {len(self.failures)}")
        report.append(f"Errors: {len(self.errors)}")
        report.append("\n" + "=" * 80)
        
        # Atomicity Test Report
        report.append("\n[1] ATOMICITY TEST")
        report.append("-" * 80)
        atomicity = self.test_details["atomicity"]
        status = "✓ PASSED" if atomicity["passed"] else "✗ FAILED"
        report.append(f"Status: {status}")
        report.append("\nTest Description:")
        report.append("  - Simulates failure during multi-operation transaction")
        report.append("  - Verifies all changes are rolled back (all-or-nothing principle)")
        report.append("  - Validates no partial data remains after failure")
        report.append("  - Confirms table consistency after rollback")
        report.append("\nTechnical Details:")
        report.append("  Database: acid_test_db (B+ Tree Test Database)")
        report.append("  Table: product_index (schema: {product_id: int, name: str, price: float, stock_quantity: int})")
        report.append("  Transaction Operations:")
        report.append("    1. INSERT: {product_id: 101, name: 'Test Product A', price: 1500.00, stock_quantity: 50}")
        report.append("    2. INSERT: {product_id: 102, name: 'Test Product B', price: 2500.00, stock_quantity: 30}")
        report.append("  Failure Injection: After operation 1 (fail_after_ops=1)")
        report.append("  Actions Verified After Failure:")
        report.append("    - table.get(101) returns None (Product A not inserted)")
        report.append("    - table.get(102) returns None (Product B not inserted)")
        report.append("    - table.verify_consistency() returns True (no data corruption)")
        if atomicity["details"]:
            report.append("\nTest Results:")
            report.append(f"  {atomicity['details']}")
        if atomicity["error"]:
            report.append("\nError Details:")
            report.append(f"  {atomicity['error']}")
        
        # Consistency Test Report
        report.append("\n" + "=" * 80)
        report.append("\n[2] CONSISTENCY TEST")
        report.append("-" * 80)
        consistency = self.test_details["consistency"]
        status = "✓ PASSED" if consistency["passed"] else "✗ FAILED"
        report.append(f"Status: {status}")
        report.append("\nTest Description:")
        report.append("  - Tests data validation during transaction execution")
        report.append("  - Inserts invalid data type (string into integer field)")
        report.append("  - Verifies transaction automatically reverts on invalid data")
        report.append("  - Ensures original data is preserved")
        report.append("  - Confirms data consistency after validation failure")
        report.append("\nTechnical Details:")
        report.append("  Database: acid_test_db (B+ Tree Test Database)")
        report.append("  Table: product_index (schema: {product_id: int, name: str, price: float, stock_quantity: int})")
        report.append("  Setup Phase:")
        report.append("    - BEGIN TRANSACTION")
        report.append("    - INSERT: {product_id: 201, name: 'Laptop', price: 50000.00, stock_quantity: 10}")
        report.append("    - COMMIT (transaction committed and persisted)")
        report.append("  Validation Test Phase:")
        report.append("    1. UPDATE: {product_id: 201, price: 45000.00} [VALID]")
        report.append("    2. UPDATE: {product_id: 201, price: 'invalid_price'} [INVALID - type error]")
        report.append("  Schema Validation:")
        report.append("    - Field 'price' expects type: float")
        report.append("    - Attempted invalid value: 'invalid_price' (str)")
        report.append("    - Type mismatch detected and transaction rolled back")
        report.append("  Actions Verified After Rollback:")
        report.append("    - table.get(201)['price'] == 50000.00 (original value preserved)")
        report.append("    - Price not updated to 45000.00 (no partial updates)")
        report.append("    - table.verify_consistency() returns True (no data corruption)")
        if consistency["details"]:
            report.append("\nTest Results:")
            report.append(f"  {consistency['details']}")
        if consistency["error"]:
            report.append("\nError Details:")
            report.append(f"  {consistency['error']}")
        
        # Durability Test Report
        report.append("\n" + "=" * 80)
        report.append("\n[3] DURABILITY TEST")
        report.append("-" * 80)
        durability = self.test_details["durability"]
        status = "✓ PASSED" if durability["passed"] else "✗ FAILED"
        report.append(f"Status: {status}")
        report.append("\nTest Description:")
        report.append("  - Simulates crash during transaction commit")
        report.append("  - Uses Write-Ahead Logging (WAL) for recovery")
        report.append("  - Restarts database and triggers recovery mechanism")
        report.append("  - Verifies committed data persists after crash recovery")
        report.append("  - Confirms data integrity after system restart")
        report.append("\nTechnical Details:")
        report.append("  Database: acid_test_db (B+ Tree Test Database)")
        report.append("  Table: product_index (schema: {product_id: int, name: str, price: float, stock_quantity: int})")
        report.append("  Crash Scenario:")
        report.append("    - BEGIN TRANSACTION")
        report.append("    - INSERT: {product_id: 301, name: 'Smartphone', price: 15000.00, stock_quantity: 75}")
        report.append("    - COMMIT with crash simulation: simulate_crash_after_wal=True")
        report.append("  Write-Ahead Log (WAL) Process:")
        report.append("    1. Transaction operations prepared")
        report.append("    2. WAL entry written to persistent log")
        report.append("    3. System CRASH occurs (RuntimeError simulated)")
        report.append("    4. In-memory state lost (transaction not yet committed in memory)")
        report.append("  Recovery Process:")
        report.append("    1. New DatabaseManager instance created (restart)")
        report.append("    2. System loads persistence directory")
        report.append("    3. Recovery mechanism detects incomplete commit in WAL")
        report.append("    4. WAL entries are replayed to restore state")
        report.append("    5. Committed data is restored from log")
        report.append("  Actions Verified After Recovery:")
        report.append("    - table.get(301) returns committed record")
        report.append("    - table.get(301)['name'] == 'Smartphone' (data persisted)")
        report.append("    - table.get(301)['price'] == 15000.00 (data persisted)")
        report.append("    - table.verify_consistency() returns True (recovery successful)")
        if durability["details"]:
            report.append("\nTest Results:")
            report.append(f"  {durability['details']}")
        if durability["error"]:
            report.append("\nError Details:")
            report.append(f"  {durability['error']}")
        
        # Summary
        report.append("\n" + "=" * 80)
        report.append("\nSUMMARY")
        report.append("-" * 80)
        all_passed = (atomicity["passed"] and 
                     consistency["passed"] and 
                     durability["passed"])
        
        if all_passed:
            report.append("✓ ALL ACID TESTS PASSED")
            report.append("\nConclusion:")
            report.append("  The database system successfully demonstrates:")
            report.append("  • Atomicity: Transactions are all-or-nothing (no partial updates)")
            report.append("  • Consistency: Data validity is maintained (invalid data rejected)")
            report.append("  • Durability: Committed data survives crashes (persistence verified)")
        else:
            report.append("✗ SOME ACID TESTS FAILED")
            report.append("\nFailed Tests:")
            if not atomicity["passed"]:
                report.append("  - Atomicity")
            if not consistency["passed"]:
                report.append("  - Consistency")
            if not durability["passed"]:
                report.append("  - Durability")
        
        report.append("\n" + "=" * 80)
        
        return "\n".join(report)


class TestAcidValidation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test database once for all tests"""
        # Create a dedicated B+ tree test database
        cls.manager = DatabaseManager(persistence_dir=None)
        
        # Create dedicated acid_test_db (B+ tree backed)
        created, msg = cls.manager.create_database("acid_test_db")
        if not created and "already exists" not in msg.lower():
            raise RuntimeError(f"Failed to create test database: {msg}")
    
    def setUp(self):
        """Set up each test with fresh table"""
        # Create product_index table for each test
        # Schema: {product_id: int, name: str, price: float, stock_quantity: int}
        table_created, _ = self.manager.create_table(
            "acid_test_db",
            "product_index",
            schema={"product_id": int, "name": str, "price": float, "stock_quantity": int},
            order=6,
            search_key="product_id",
        )
        
        # If table already exists from previous test, clear it
        if not table_created:
            table, _ = self.manager.get_table("acid_test_db", "product_index")
            # Clean up previous test data (product_ids 101-301)
            for pid in [101, 102, 201, 301]:
                try:
                    if table.get(pid) is not None:
                        table.delete(pid)
                except:
                    pass

    def tearDown(self):
        """Clean up test data after each test"""
        try:
            table, _ = self.manager.get_table("acid_test_db", "product_index")
            for pid in [101, 102, 201, 301]:
                try:
                    if table.get(pid) is not None:
                        table.delete(pid)
                except:
                    pass
        except:
            pass

    def _product_table(self):
        """Get product_index table from B+ tree test database"""
        table, message = self.manager.get_table("acid_test_db", "product_index")
        self.assertIsNotNone(table, msg=message)
        return table

    def test_atomicity_failure_rolls_back_all_changes(self):
        success, message = self.manager.run_transaction(
            operations=[
                {
                    "action": "insert",
                    "db_name": "acid_test_db",
                    "table_name": "product_index",
                    "record": {"product_id": 101, "name": "Test Product A", "price": 1500.00, "stock_quantity": 50},
                },
                {
                    "action": "insert",
                    "db_name": "acid_test_db",
                    "table_name": "product_index",
                    "record": {"product_id": 102, "name": "Test Product B", "price": 2500.00, "stock_quantity": 30},
                },
            ],
            fail_after_ops=1,
        )

        self.assertFalse(success)
        self.assertIn("Simulated failure", message)

        table = self._product_table()
        self.assertEqual(table.get(101), None)
        self.assertEqual(table.get(102), None)
        is_consistent, _ = table.verify_consistency()
        self.assertTrue(is_consistent)

    def test_consistency_invalid_update_reverts_transaction(self):
        tx_id = self.manager.begin_transaction()
        self.manager.insert_record(
            "acid_test_db",
            "product_index",
            {"product_id": 201, "name": "Laptop", "price": 50000.00, "stock_quantity": 10},
            tx_id=tx_id,
        )
        committed, msg = self.manager.commit_transaction(tx_id)
        self.assertTrue(committed, msg=msg)

        success, _ = self.manager.run_transaction(
            operations=[
                {
                    "action": "update",
                    "db_name": "acid_test_db",
                    "table_name": "product_index",
                    "record_id": 201,
                    "new_record": {"price": 45000.00},
                },
                {
                    "action": "update",
                    "db_name": "acid_test_db",
                    "table_name": "product_index",
                    "record_id": 201,
                    "new_record": {"price": "invalid_price"},  # Invalid type - should be float
                },
            ],
        )

        self.assertFalse(success)

        table = self._product_table()
        row = table.get(201)
        self.assertEqual(row["price"], 50000.00)

        is_consistent, _ = table.verify_consistency()
        self.assertTrue(is_consistent)

    def test_durability_recovery_replays_committed_wal(self):
        tx_id = self.manager.begin_transaction()
        self.manager.insert_record(
            "acid_test_db",
            "product_index",
            {"product_id": 301, "name": "Smartphone", "price": 15000.00, "stock_quantity": 75},
            tx_id=tx_id,
        )

        with self.assertRaises(RuntimeError):
            self.manager.commit_transaction(tx_id, simulate_crash_after_wal=True)

        # Restart database manager (simulating system restart)
        restarted = DatabaseManager(persistence_dir=None)
        table, message = restarted.get_table("acid_test_db", "product_index")
        self.assertIsNotNone(table, msg=message)

        row = table.get(301)
        self.assertIsNotNone(row)
        self.assertEqual(row["name"], "Smartphone")
        self.assertEqual(row["price"], 15000.00)

        is_consistent, _ = table.verify_consistency()
        self.assertTrue(is_consistent)


if __name__ == "__main__":
    # Create custom test suite
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestAcidValidation)
    
    # Run tests with custom result class (directly)
    result = StructuredTestResult()
    suite.run(result)
    
    # Generate report and save to file
    report_text = result.generate_report()
    
    # Save to file with UTF-8 encoding
    import os
    report_path = os.path.join(os.path.dirname(__file__), "acid_test_report.txt")
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report_text)
    
    # Print summary to console with basic characters
    print("\n" + "="*80)
    print("RUNNING ACID VALIDATION TESTS")
    print("="*80)
    print(f"Tests Run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print("\n[TEST SUMMARY]")
    
    all_passed = (result.test_details["atomicity"]["passed"] and 
                 result.test_details["consistency"]["passed"] and 
                 result.test_details["durability"]["passed"])
    
    print(f"Atomicity Test: {'PASSED' if result.test_details['atomicity']['passed'] else 'FAILED'}")
    print(f"Consistency Test: {'PASSED' if result.test_details['consistency']['passed'] else 'FAILED'}")
    print(f"Durability Test: {'PASSED' if result.test_details['durability']['passed'] else 'FAILED'}")
    
    if all_passed:
        print("\n>>> ALL ACID TESTS PASSED <<<")
    else:
        print("\n>>> SOME TESTS FAILED <<<")
    
    print(f"\nFull report saved to: {report_path}")
    print("="*80)
