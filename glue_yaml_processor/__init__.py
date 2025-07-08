"""
AWS Glue YAML Processor

A Python library for processing YAML configurations in AWS Glue Python shell jobs.
Supports MySQL operations, stored procedures, smart upserts, and parallel task execution.
"""

from .core.processor import YAMLProcessor
from .core.connection import MySQLConnectionManager
from .tasks.upsert import SmartUpsert
from .tasks.stored_procedure import StoredProcedureExecutor
from .utils.checksum import MD5CheckSum

__version__ = "1.0.0"
__all__ = [
    "YAMLProcessor",
    "MySQLConnectionManager", 
    "SmartUpsert",
    "StoredProcedureExecutor",
    "MD5CheckSum",
]