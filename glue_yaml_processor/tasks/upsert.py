"""
Smart Upsert functionality for MySQL tables
"""

import pymysql
from typing import Dict, List, Any, Optional, Tuple
from ..core.connection import MySQLConnectionManager
from ..utils.checksum import MD5CheckSum


class SmartUpsert:
    """Implements smart upsert functionality using checksums for change detection."""
    
    def __init__(self, connection_manager: MySQLConnectionManager):
        """
        Initialize SmartUpsert with connection manager.
        
        Args:
            connection_manager: MySQL connection manager instance
        """
        self.connection_manager = connection_manager
    
    def execute_upsert(
        self,
        source_table: str,
        target_table: str,
        primary_key: str,
        checksum_columns: List[str],
        checksum_column_name: str = 'checksum_val'
    ) -> Dict[str, Any]:
        """
        Execute smart upsert operation between source and target tables.
        
        Args:
            source_table: Name of the source table
            target_table: Name of the target table
            primary_key: Primary key column name for joining
            checksum_columns: List of columns to include in checksum calculation
            checksum_column_name: Name of the checksum column in target table
            
        Returns:
            Dictionary with operation results
        """
        with self.connection_manager.get_cursor(
            autocommit=True, 
            cursor_class=pymysql.cursors.DictCursor
        ) as cursor:
            # Read all rows from source table
            cursor.execute(f"SELECT * FROM {source_table}")
            source_rows = cursor.fetchall()
            
            if not source_rows:
                return {
                    'success': True,
                    'message': 'No data found in source table',
                    'rows_processed': 0,
                    'rows_upserted': 0
                }
            
            # Compute checksum for each source row
            all_columns = list(source_rows[0].keys())
            for row in source_rows:
                row[checksum_column_name] = MD5CheckSum.compute_row_checksum(
                    row, checksum_columns
                )
            
            # Read primary key and checksum from target table
            cursor.execute(
                f"SELECT {primary_key}, {checksum_column_name} FROM {target_table}"
            )
            target_rows = cursor.fetchall()
            target_map = {
                row[primary_key]: row[checksum_column_name] 
                for row in target_rows
            }
            
            # Identify rows that need upserting
            upsert_rows = []
            for row in source_rows:
                pk_value = row[primary_key]
                checksum = row[checksum_column_name]
                
                if pk_value not in target_map or target_map[pk_value] != checksum:
                    upsert_rows.append(row)
            
            # Perform upsert operation
            if upsert_rows:
                self._perform_upsert(
                    cursor, 
                    target_table, 
                    upsert_rows, 
                    all_columns + [checksum_column_name],
                    primary_key
                )
                
                return {
                    'success': True,
                    'message': f'Upserted {len(upsert_rows)} records',
                    'rows_processed': len(source_rows),
                    'rows_upserted': len(upsert_rows)
                }
            else:
                return {
                    'success': True,
                    'message': 'No new or changed records to upsert',
                    'rows_processed': len(source_rows),
                    'rows_upserted': 0
                }
    
    def _perform_upsert(
        self,
        cursor: pymysql.cursors.Cursor,
        target_table: str,
        upsert_rows: List[Dict[str, Any]],
        all_columns: List[str],
        primary_key: str
    ) -> None:
        """
        Perform the actual upsert operation.
        
        Args:
            cursor: Database cursor
            target_table: Target table name
            upsert_rows: Rows to upsert
            all_columns: All column names including checksum
            primary_key: Primary key column name
        """
        cols_str = ','.join(all_columns)
        placeholders = ','.join(['%s'] * len(all_columns))
        update_stmt = ','.join([
            f"{col}=VALUES({col})" 
            for col in all_columns 
            if col != primary_key
        ])
        
        sql = (
            f"INSERT INTO {target_table} ({cols_str}) "
            f"VALUES ({placeholders}) "
            f"ON DUPLICATE KEY UPDATE {update_stmt}"
        )
        
        for row in upsert_rows:
            values = [row.get(col) for col in all_columns]
            cursor.execute(sql, values)
    
    def execute_custom_upsert(
        self,
        source_query: str,
        target_table: str,
        primary_key: str,
        checksum_columns: List[str],
        checksum_column_name: str = 'checksum_val'
    ) -> Dict[str, Any]:
        """
        Execute smart upsert using a custom source query.
        
        Args:
            source_query: SQL query to get source data
            target_table: Name of the target table
            primary_key: Primary key column name for joining
            checksum_columns: List of columns to include in checksum calculation
            checksum_column_name: Name of the checksum column in target table
            
        Returns:
            Dictionary with operation results
        """
        with self.connection_manager.get_cursor(
            autocommit=True, 
            cursor_class=pymysql.cursors.DictCursor
        ) as cursor:
            # Execute source query
            cursor.execute(source_query)
            source_rows = cursor.fetchall()
            
            if not source_rows:
                return {
                    'success': True,
                    'message': 'No data found from source query',
                    'rows_processed': 0,
                    'rows_upserted': 0
                }
            
            # Compute checksum for each source row
            all_columns = list(source_rows[0].keys())
            for row in source_rows:
                row[checksum_column_name] = MD5CheckSum.compute_row_checksum(
                    row, checksum_columns
                )
            
            # Read primary key and checksum from target table
            cursor.execute(
                f"SELECT {primary_key}, {checksum_column_name} FROM {target_table}"
            )
            target_rows = cursor.fetchall()
            target_map = {
                row[primary_key]: row[checksum_column_name] 
                for row in target_rows
            }
            
            # Identify rows that need upserting
            upsert_rows = []
            for row in source_rows:
                pk_value = row[primary_key]
                checksum = row[checksum_column_name]
                
                if pk_value not in target_map or target_map[pk_value] != checksum:
                    upsert_rows.append(row)
            
            # Perform upsert operation
            if upsert_rows:
                self._perform_upsert(
                    cursor, 
                    target_table, 
                    upsert_rows, 
                    all_columns + [checksum_column_name],
                    primary_key
                )
                
                return {
                    'success': True,
                    'message': f'Upserted {len(upsert_rows)} records',
                    'rows_processed': len(source_rows),
                    'rows_upserted': len(upsert_rows)
                }
            else:
                return {
                    'success': True,
                    'message': 'No new or changed records to upsert',
                    'rows_processed': len(source_rows),
                    'rows_upserted': 0
                }