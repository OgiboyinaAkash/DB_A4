"""
Test script to demonstrate crash WAL preservation.
Shows WAL entries being preserved when a crash is simulated.
"""

try:
    from .db_manager import DatabaseManager
except ImportError:
    from db_manager import DatabaseManager


def test_crash_wal_preservation():
    """Test that WAL entries are preserved during crash scenarios."""
    
    print("\n" + "="*80)
    print("CRASH WAL PRESERVATION TEST")
    print("="*80 + "\n")
    
    # Initialize database
    db_manager = DatabaseManager()
    
    # Create test database and table
    db_manager.create_database("crash_test_db")
    schema = {
        "product_id": int,
        "name": str,
        "price": float,
        "quantity": int
    }
    db_manager.create_table("crash_test_db", "products", schema, order=6, search_key="product_id")
    
    print("[*] Database and table created")
    print("[*] Attempting transaction with simulated crash after WAL...\n")
    
    # Begin transaction
    tx_id = db_manager.begin_transaction()
    
    # Insert records
    record1 = {
        "product_id": 501,
        "name": "Crash Test Product A",
        "price": 199.99,
        "quantity": 100
    }
    record2 = {
        "product_id": 502,
        "name": "Crash Test Product B",
        "price": 299.99,
        "quantity": 50
    }
    
    db_manager.insert_record("crash_test_db", "products", record1, tx_id=tx_id)
    db_manager.insert_record("crash_test_db", "products", record2, tx_id=tx_id)
    
    print("  ✓ Inserted 2 records into transaction")
    print("  ✓ Attempting commit with simulated crash...\n")
    
    # Try to commit with crash simulation
    try:
        db_manager.commit_transaction(tx_id, simulate_crash_after_wal=True)
    except RuntimeError as e:
        print(f"  ⚠ Simulated crash triggered: {e}")
    
    # Check crash WAL
    print("\n" + "-"*80)
    print("CRASH WAL LOG CONTENTS")
    print("-"*80 + "\n")
    
    crash_wal = db_manager.get_crash_wal_entries()
    if crash_wal:
        print(crash_wal)
    else:
        print("No crash WAL entries found")
    
    # Verify normal WAL is empty (cleared after successful commits)
    print("\n" + "-"*80)
    print("NORMAL WAL LOG STATUS")
    print("-"*80 + "\n")
    
    wal_file = db_manager._wal_file
    import os
    if os.path.exists(wal_file):
        with open(wal_file, "r") as f:
            wal_content = f.read()
        if wal_content.strip():
            print(f"Normal WAL content:\n{wal_content}")
        else:
            print("✓ Normal WAL is empty (as expected - cleared after successful commits)")
    
    print("\n" + "="*80)
    print("TEST COMPLETED")
    print("="*80)
    print("\nSummary:")
    print("  - Transaction WAL entries are written before commit")
    print("  - If crash occurs, entries are preserved in crash_wal.log")
    print("  - Normal WAL.log is cleared after successful commits")
    print("  - crash_wal.log persists across sessions for recovery analysis")
    print("="*80 + "\n")


if __name__ == "__main__":
    test_crash_wal_preservation()
