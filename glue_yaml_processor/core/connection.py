"""
MySQL Connection Manager for AWS Glue
"""

import json
import boto3
import pymysql
from typing import Dict, Optional
from contextlib import contextmanager


class MySQLConnectionManager:
    """Manages MySQL connections using AWS Glue connection configurations."""
    
    def __init__(self, glue_connection_name: str, region_name: str = 'ap-northeast-1'):
        """
        Initialize MySQL connection manager.
        
        Args:
            glue_connection_name: Name of the AWS Glue connection
            region_name: AWS region name
        """
        self.glue_connection_name = glue_connection_name
        self.region_name = region_name
        self._connection_params = None
    
    def get_connection_params(self) -> Dict[str, str]:
        """
        Get MySQL connection parameters from AWS Glue connection.
        
        Returns:
            Dictionary containing connection parameters
        """
        if self._connection_params is None:
            glue = boto3.client('glue', region_name=self.region_name)
            response = glue.get_connection(Name=self.glue_connection_name)
            props = response['Connection']['ConnectionProperties']
            
            jdbc_url = props['JDBC_CONNECTION_URL']
            secret_arn = props['SECRET_ID']
            
            # Parse JDBC URL
            host = jdbc_url.split("//")[1].split(":")[0]
            port = int(jdbc_url.split(":")[-1].split("/")[0])
            database = jdbc_url.split("/")[-1]
            
            # Get credentials from Secrets Manager
            secretsmanager = boto3.client('secretsmanager', region_name=self.region_name)
            secret_value = secretsmanager.get_secret_value(SecretId=secret_arn)
            credentials = json.loads(secret_value['SecretString'])
            
            self._connection_params = {
                'host': host,
                'port': port,
                'database': database,
                'user': credentials['username'],
                'password': credentials['password'],
                'jdbc_url': jdbc_url
            }
        
        return self._connection_params
    
    def create_connection(self, autocommit: bool = True, cursor_class=None) -> pymysql.Connection:
        """
        Create a MySQL connection.
        
        Args:
            autocommit: Whether to enable autocommit
            cursor_class: Cursor class to use
            
        Returns:
            PyMySQL connection object
        """
        params = self.get_connection_params()
        
        connection_kwargs = {
            'host': params['host'],
            'port': params['port'],
            'user': params['user'],
            'password': params['password'],
            'database': params['database'],
            'autocommit': autocommit,
        }
        
        if cursor_class is not None:
            connection_kwargs['cursorclass'] = cursor_class
        
        return pymysql.connect(**connection_kwargs)
    
    @contextmanager
    def get_connection(self, autocommit: bool = True, cursor_class=None):
        """
        Context manager for MySQL connections.
        
        Args:
            autocommit: Whether to enable autocommit
            cursor_class: Cursor class to use
            
        Yields:
            PyMySQL connection object
        """
        conn = None
        try:
            conn = self.create_connection(autocommit=autocommit, cursor_class=cursor_class)
            yield conn
        finally:
            if conn:
                conn.close()
    
    @contextmanager
    def get_cursor(self, autocommit: bool = True, cursor_class=None):
        """
        Context manager for MySQL cursors.
        
        Args:
            autocommit: Whether to enable autocommit
            cursor_class: Cursor class to use
            
        Yields:
            PyMySQL cursor object
        """
        with self.get_connection(autocommit=autocommit, cursor_class=cursor_class) as conn:
            cursor = conn.cursor()
            try:
                yield cursor
            finally:
                cursor.close()