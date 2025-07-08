"""
Stored Procedure execution functionality
"""

import pymysql
from typing import Dict, List, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from ..core.connection import MySQLConnectionManager


class StoredProcedureExecutor:
    """Executes MySQL stored procedures with support for parallel execution."""
    
    def __init__(self, connection_manager: MySQLConnectionManager):
        """
        Initialize StoredProcedureExecutor with connection manager.
        
        Args:
            connection_manager: MySQL connection manager instance
        """
        self.connection_manager = connection_manager
    
    def execute_procedure(
        self,
        procedure_name: str,
        args: Optional[List[Any]] = None
    ) -> Dict[str, Any]:
        """
        Execute a single stored procedure.
        
        Args:
            procedure_name: Name of the stored procedure
            args: Arguments to pass to the procedure
            
        Returns:
            Dictionary with execution results
        """
        try:
            with self.connection_manager.get_cursor(autocommit=True) as cursor:
                if args:
                    cursor.callproc(procedure_name, args)
                else:
                    cursor.callproc(procedure_name)
                
                return {
                    'success': True,
                    'procedure': procedure_name,
                    'message': f'Successfully executed {procedure_name}',
                    'args': args or []
                }
        except Exception as e:
            return {
                'success': False,
                'procedure': procedure_name,
                'message': f'Error executing {procedure_name}: {str(e)}',
                'args': args or [],
                'error': str(e)
            }
    
    def execute_procedures_sequential(
        self,
        procedures: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Execute multiple stored procedures sequentially.
        
        Args:
            procedures: List of procedure configurations
                Each dict should have 'name' and optionally 'args'
                
        Returns:
            List of execution results
        """
        results = []
        
        for proc_config in procedures:
            procedure_name = proc_config.get('name')
            args = proc_config.get('args')
            
            if not procedure_name:
                results.append({
                    'success': False,
                    'procedure': 'unknown',
                    'message': 'Procedure name is required',
                    'args': args or []
                })
                continue
            
            result = self.execute_procedure(procedure_name, args)
            results.append(result)
            
            # Stop on first failure if specified
            if not result['success'] and proc_config.get('stop_on_failure', False):
                break
        
        return results
    
    def execute_procedures_parallel(
        self,
        procedures: List[Dict[str, Any]],
        max_workers: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute multiple stored procedures in parallel.
        
        Args:
            procedures: List of procedure configurations
            max_workers: Maximum number of parallel workers
            
        Returns:
            List of execution results
        """
        if max_workers is None:
            max_workers = min(len(procedures), 10)
        
        results = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all procedures
            future_to_proc = {}
            for i, proc_config in enumerate(procedures):
                procedure_name = proc_config.get('name')
                args = proc_config.get('args')
                
                if not procedure_name:
                    results.append({
                        'success': False,
                        'procedure': 'unknown',
                        'message': 'Procedure name is required',
                        'args': args or [],
                        'index': i
                    })
                    continue
                
                future = executor.submit(self.execute_procedure, procedure_name, args)
                future_to_proc[future] = {'config': proc_config, 'index': i}
            
            # Collect results
            for future in as_completed(future_to_proc):
                proc_info = future_to_proc[future]
                try:
                    result = future.result()
                    result['index'] = proc_info['index']
                    results.append(result)
                except Exception as e:
                    results.append({
                        'success': False,
                        'procedure': proc_info['config'].get('name', 'unknown'),
                        'message': f'Error in parallel execution: {str(e)}',
                        'args': proc_info['config'].get('args', []),
                        'error': str(e),
                        'index': proc_info['index']
                    })
        
        # Sort results by original order
        results.sort(key=lambda x: x.get('index', 0))
        
        return results
    
    def execute_sql_query(
        self,
        query: str,
        params: Optional[List[Any]] = None,
        fetch_results: bool = False
    ) -> Dict[str, Any]:
        """
        Execute a SQL query (not a stored procedure).
        
        Args:
            query: SQL query to execute
            params: Parameters for the query
            fetch_results: Whether to fetch and return results
            
        Returns:
            Dictionary with execution results
        """
        try:
            cursor_class = pymysql.cursors.DictCursor if fetch_results else None
            
            with self.connection_manager.get_cursor(
                autocommit=True, 
                cursor_class=cursor_class
            ) as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                result = {
                    'success': True,
                    'query': query,
                    'message': 'Query executed successfully',
                    'params': params or []
                }
                
                if fetch_results:
                    result['results'] = cursor.fetchall()
                    result['row_count'] = len(result['results'])
                else:
                    result['row_count'] = cursor.rowcount
                
                return result
                
        except Exception as e:
            return {
                'success': False,
                'query': query,
                'message': f'Error executing query: {str(e)}',
                'params': params or [],
                'error': str(e)
            }
    
    def execute_sql_queries_sequential(
        self,
        queries: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Execute multiple SQL queries sequentially.
        
        Args:
            queries: List of query configurations
                Each dict should have 'query' and optionally 'params', 'fetch_results'
                
        Returns:
            List of execution results
        """
        results = []
        
        for query_config in queries:
            query = query_config.get('query')
            params = query_config.get('params')
            fetch_results = query_config.get('fetch_results', False)
            
            if not query:
                results.append({
                    'success': False,
                    'query': 'unknown',
                    'message': 'Query is required',
                    'params': params or []
                })
                continue
            
            result = self.execute_sql_query(query, params, fetch_results)
            results.append(result)
            
            # Stop on first failure if specified
            if not result['success'] and query_config.get('stop_on_failure', False):
                break
        
        return results