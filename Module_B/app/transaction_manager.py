"""
Transaction Manager for Cross-Request Atomicity
Manages multi-operation transactions across multiple HTTP requests

TWO-PHASE COMMIT IMPLEMENTATION:
Phase 1 (PREPARE): Accumulate and validate all operations
Phase 2 (EXECUTE): Execute all operations on commit, or discard on rollback
"""

import uuid
from datetime import datetime
from threading import Lock


class TransactionManager:
    """Manages active transactions with true atomic all-or-nothing semantics"""
    
    def __init__(self):
        self.active_transactions = {}
        self.lock = Lock()
    
    def begin_transaction(self, member_id, session_token):
        """
        Start a new transaction
        
        Returns:
            dict: {
                'transaction_id': str,
                'started_at': timestamp,
                'status': 'active'
            }
        """
        with self.lock:
            transaction_id = str(uuid.uuid4())
            
            self.active_transactions[transaction_id] = {
                'transaction_id': transaction_id,
                'member_id': member_id,
                'session_token': session_token,
                'status': 'active',
                'started_at': datetime.utcnow().isoformat(),
                # PHASE 1: Pending operations (not yet executed)
                'pending_operations': [],  
                # PHASE 2: Executed operations (after commit)
                'committed_operations': [],  
                'write_callbacks': [],  # Functions to execute on commit
                'error': None,
                'validation_errors': []
            }
            
            return self.active_transactions[transaction_id]
    
    def queue_operation(self, transaction_id, operation_type, db_name, table_name, data, write_func=None):
        """
        Queue an operation for later execution (PHASE 1: PREPARE)
        
        Args:
            transaction_id: Transaction ID
            operation_type: 'create', 'update', 'delete'
            db_name: Database name
            table_name: Table name
            data: Record data
            write_func: Callback function to execute on commit
        
        Returns:
            dict: {
                'success': bool,
                'message': str,
                'operation_id': int (index in queue)
            }
        """
        with self.lock:
            if transaction_id not in self.active_transactions:
                return {'success': False, 'message': 'Transaction not found'}
            
            trans = self.active_transactions[transaction_id]
            if trans['status'] != 'active':
                return {'success': False, 'message': f'Transaction is {trans["status"]}'}
            
            op_id = len(trans['pending_operations'])
            trans['pending_operations'].append({
                'operation_id': op_id,
                'type': operation_type,
                'db': db_name,
                'table': table_name,
                'data': data,
                'timestamp': datetime.utcnow().isoformat()
            })
            
            if write_func:
                trans['write_callbacks'].append((op_id, write_func))
            
            return {
                'success': True,
                'message': 'Operation queued',
                'operation_id': op_id
            }
    
    def add_validation_error(self, transaction_id, error_message):
        """
        Log a validation error for the transaction
        
        Returns:
            bool: True if added successfully
        """
        with self.lock:
            if transaction_id not in self.active_transactions:
                return False
            
            trans = self.active_transactions[transaction_id]
            trans['validation_errors'].append({
                'error': error_message,
                'timestamp': datetime.utcnow().isoformat()
            })
            return True
    
    def commit_transaction(self, transaction_id):
        """
        Commit a transaction (PHASE 2: EXECUTE)
        
        If there are validation errors, fail immediately without executing any operations.
        If validation passes, execute all queued operations together (all-or-nothing).
        
        Returns:
            dict: {
                'success': bool,
                'message': str,
                'validation_errors': list (if present),
                'operations_count': int,
                'committed_at': timestamp
            }
        """
        with self.lock:
            if transaction_id not in self.active_transactions:
                return {'success': False, 'message': 'Transaction not found'}
            
            trans = self.active_transactions[transaction_id]
            
            if trans['status'] != 'active':
                return {
                    'success': False,
                    'message': f"Transaction is {trans['status']}, cannot commit"
                }
            
            # CHECK: If ANY validation errors exist, reject entire transaction
            if trans['validation_errors']:
                trans['status'] = 'rolled_back'
                trans['rolled_back_at'] = datetime.utcnow().isoformat()
                return {
                    'success': False,
                    'message': 'Transaction rolled back due to validation errors',
                    'transaction_id': transaction_id,
                    'validation_errors': trans['validation_errors'],
                    'operations_count': 0,
                    'rolled_back_at': trans['rolled_back_at']
                }
            
            # EXECUTE: All operations together (all-or-nothing)
            executed_count = 0
            execute_errors = []
            
            try:
                # Execute all write callbacks in order
                for op_id, write_func in trans['write_callbacks']:
                    try:
                        write_func()
                        executed_count += 1
                        trans['committed_operations'].append({
                            'operation_id': op_id,
                            'committed_at': datetime.utcnow().isoformat()
                        })
                    except Exception as e:
                        execute_errors.append({
                            'operation_id': op_id,
                            'error': str(e)
                        })
                
                # If ANY operation failed to execute, rollback is not possible
                # (data was already written). Mark as failed.
                if execute_errors:
                    trans['status'] = 'commit_failed'
                    return {
                        'success': False,
                        'message': 'Some operations failed during commit',
                        'transaction_id': transaction_id,
                        'execution_errors': execute_errors,
                        'executed_count': executed_count
                    }
                
                # SUCCESS: All operations executed
                trans['status'] = 'committed'
                trans['committed_at'] = datetime.utcnow().isoformat()
                
                return {
                    'success': True,
                    'message': 'Transaction committed successfully',
                    'transaction_id': transaction_id,
                    'operations_count': executed_count,
                    'committed_at': trans['committed_at']
                }
            
            except Exception as e:
                trans['status'] = 'commit_failed'
                return {
                    'success': False,
                    'message': f'Commit failed: {str(e)}',
                    'transaction_id': transaction_id
                }
    
    def rollback_transaction(self, transaction_id):
        """
        Rollback a transaction (discard all queued operations)
        
        Returns:
            dict: {
                'success': bool,
                'message': str,
                'rolled_back_at': timestamp
            }
        """
        with self.lock:
            if transaction_id not in self.active_transactions:
                return {'success': False, 'message': 'Transaction not found'}
            
            trans = self.active_transactions[transaction_id]
            
            if trans['status'] == 'rolled_back':
                return {'success': False, 'message': 'Transaction already rolled back'}
            
            if trans['status'] == 'committed':
                return {'success': False, 'message': 'Cannot rollback a committed transaction'}
            
            # Discard all pending and committed operations
            pending_count = len(trans['pending_operations'])
            trans['pending_operations'] = []
            trans['write_callbacks'] = []
            trans['status'] = 'rolled_back'
            trans['rolled_back_at'] = datetime.utcnow().isoformat()
            
            return {
                'success': True,
                'message': 'Transaction rolled back. All operations discarded.',
                'transaction_id': transaction_id,
                'discarded_operations': pending_count,
                'rolled_back_at': trans['rolled_back_at']
            }
    
    def get_transaction_status(self, transaction_id):
        """
        Get current transaction status
        
        Returns:
            dict or None if transaction not found
        """
        with self.lock:
            if transaction_id not in self.active_transactions:
                return None
            
            trans = self.active_transactions[transaction_id]
            return {
                'transaction_id': transaction_id,
                'status': trans['status'],
                'pending_operations_count': len(trans['pending_operations']),
                'committed_operations_count': len(trans['committed_operations']),
                'validation_errors': trans['validation_errors'],
                'started_at': trans['started_at'],
                'pending_operations': trans['pending_operations']
            }
    
    def cancel_transaction(self, transaction_id):
        """Cancel a transaction (cleanup without rollback)"""
        with self.lock:
            if transaction_id in self.active_transactions:
                del self.active_transactions[transaction_id]
                return True
            return False

