"""
YAML configuration parser and validator
"""

import yaml
import boto3
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass
from enum import Enum


class TaskType(Enum):
    """Supported task types."""
    UPSERT = "upsert"
    STORED_PROCEDURE = "stored_procedure"
    SQL_QUERY = "sql_query"


class ExecutionMode(Enum):
    """Task execution modes."""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"


@dataclass
class UpsertTaskConfig:
    """Configuration for upsert tasks."""
    source_table: Optional[str] = None
    source_query: Optional[str] = None
    target_table: str = ""
    primary_key: str = ""
    checksum_columns: List[str] = None
    checksum_column_name: str = "checksum_val"
    
    def __post_init__(self):
        if self.checksum_columns is None:
            self.checksum_columns = []


@dataclass
class StoredProcedureTaskConfig:
    """Configuration for stored procedure tasks."""
    name: str = ""
    args: Optional[List[Any]] = None
    stop_on_failure: bool = False
    
    def __post_init__(self):
        if self.args is None:
            self.args = []


@dataclass
class SqlQueryTaskConfig:
    """Configuration for SQL query tasks."""
    query: str = ""
    params: Optional[List[Any]] = None
    fetch_results: bool = False
    stop_on_failure: bool = False
    
    def __post_init__(self):
        if self.params is None:
            self.params = []


@dataclass
class TaskConfig:
    """Configuration for a single task."""
    name: str = ""
    type: TaskType = TaskType.SQL_QUERY
    config: Union[UpsertTaskConfig, StoredProcedureTaskConfig, SqlQueryTaskConfig] = None
    enabled: bool = True


@dataclass
class TaskGroupConfig:
    """Configuration for a group of tasks."""
    name: str = ""
    execution_mode: ExecutionMode = ExecutionMode.SEQUENTIAL
    max_workers: Optional[int] = None
    tasks: List[TaskConfig] = None
    enabled: bool = True
    
    def __post_init__(self):
        if self.tasks is None:
            self.tasks = []


@dataclass
class YAMLConfig:
    """Main YAML configuration."""
    version: str = "1.0"
    connection: Dict[str, str] = None
    task_groups: List[TaskGroupConfig] = None
    
    def __post_init__(self):
        if self.connection is None:
            self.connection = {}
        if self.task_groups is None:
            self.task_groups = []


class YAMLParser:
    """Parser for YAML configuration files."""
    
    def __init__(self, region_name: str = 'ap-northeast-1'):
        """
        Initialize YAML parser.
        
        Args:
            region_name: AWS region name
        """
        self.region_name = region_name
    
    def parse_from_file(self, file_path: str) -> YAMLConfig:
        """
        Parse YAML configuration from a file.
        
        Args:
            file_path: Path to the YAML file
            
        Returns:
            Parsed YAML configuration
        """
        with open(file_path, 'r') as f:
            yaml_content = f.read()
        
        return self.parse_from_string(yaml_content)
    
    def parse_from_s3(self, bucket: str, key: str) -> YAMLConfig:
        """
        Parse YAML configuration from S3.
        
        Args:
            bucket: S3 bucket name
            key: S3 object key
            
        Returns:
            Parsed YAML configuration
        """
        s3 = boto3.client('s3', region_name=self.region_name)
        obj = s3.get_object(Bucket=bucket, Key=key)
        yaml_content = obj['Body'].read().decode('utf-8')
        
        return self.parse_from_string(yaml_content)
    
    def parse_from_string(self, yaml_content: str) -> YAMLConfig:
        """
        Parse YAML configuration from string.
        
        Args:
            yaml_content: YAML content as string
            
        Returns:
            Parsed YAML configuration
        """
        try:
            data = yaml.safe_load(yaml_content)
            return self._convert_to_config(data)
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML format: {e}")
    
    def _convert_to_config(self, data: Dict[str, Any]) -> YAMLConfig:
        """
        Convert dictionary to YAMLConfig object.
        
        Args:
            data: Dictionary from YAML parsing
            
        Returns:
            YAMLConfig object
        """
        config = YAMLConfig()
        
        # Parse version
        config.version = data.get('version', '1.0')
        
        # Parse connection
        config.connection = data.get('connection', {})
        
        # Parse task groups
        task_groups_data = data.get('task_groups', [])
        config.task_groups = []
        
        for group_data in task_groups_data:
            group_config = self._parse_task_group(group_data)
            config.task_groups.append(group_config)
        
        return config
    
    def _parse_task_group(self, group_data: Dict[str, Any]) -> TaskGroupConfig:
        """
        Parse task group configuration.
        
        Args:
            group_data: Task group data from YAML
            
        Returns:
            TaskGroupConfig object
        """
        group_config = TaskGroupConfig()
        
        group_config.name = group_data.get('name', '')
        group_config.enabled = group_data.get('enabled', True)
        
        # Parse execution mode
        execution_mode_str = group_data.get('execution_mode', 'sequential')
        try:
            group_config.execution_mode = ExecutionMode(execution_mode_str)
        except ValueError:
            group_config.execution_mode = ExecutionMode.SEQUENTIAL
        
        group_config.max_workers = group_data.get('max_workers')
        
        # Parse tasks
        tasks_data = group_data.get('tasks', [])
        group_config.tasks = []
        
        for task_data in tasks_data:
            task_config = self._parse_task(task_data)
            group_config.tasks.append(task_config)
        
        return group_config
    
    def _parse_task(self, task_data: Dict[str, Any]) -> TaskConfig:
        """
        Parse task configuration.
        
        Args:
            task_data: Task data from YAML
            
        Returns:
            TaskConfig object
        """
        task_config = TaskConfig()
        
        task_config.name = task_data.get('name', '')
        task_config.enabled = task_data.get('enabled', True)
        
        # Parse task type
        task_type_str = task_data.get('type', 'sql_query')
        try:
            task_config.type = TaskType(task_type_str)
        except ValueError:
            task_config.type = TaskType.SQL_QUERY
        
        # Parse task-specific configuration
        config_data = task_data.get('config', {})
        
        if task_config.type == TaskType.UPSERT:
            task_config.config = self._parse_upsert_config(config_data)
        elif task_config.type == TaskType.STORED_PROCEDURE:
            task_config.config = self._parse_stored_procedure_config(config_data)
        elif task_config.type == TaskType.SQL_QUERY:
            task_config.config = self._parse_sql_query_config(config_data)
        
        return task_config
    
    def _parse_upsert_config(self, config_data: Dict[str, Any]) -> UpsertTaskConfig:
        """Parse upsert task configuration."""
        return UpsertTaskConfig(
            source_table=config_data.get('source_table'),
            source_query=config_data.get('source_query'),
            target_table=config_data.get('target_table', ''),
            primary_key=config_data.get('primary_key', ''),
            checksum_columns=config_data.get('checksum_columns', []),
            checksum_column_name=config_data.get('checksum_column_name', 'checksum_val')
        )
    
    def _parse_stored_procedure_config(self, config_data: Dict[str, Any]) -> StoredProcedureTaskConfig:
        """Parse stored procedure task configuration."""
        return StoredProcedureTaskConfig(
            name=config_data.get('name', ''),
            args=config_data.get('args', []),
            stop_on_failure=config_data.get('stop_on_failure', False)
        )
    
    def _parse_sql_query_config(self, config_data: Dict[str, Any]) -> SqlQueryTaskConfig:
        """Parse SQL query task configuration."""
        return SqlQueryTaskConfig(
            query=config_data.get('query', ''),
            params=config_data.get('params', []),
            fetch_results=config_data.get('fetch_results', False),
            stop_on_failure=config_data.get('stop_on_failure', False)
        )
    
    def validate_config(self, config: YAMLConfig) -> List[str]:
        """
        Validate YAML configuration.
        
        Args:
            config: YAML configuration to validate
            
        Returns:
            List of validation errors
        """
        errors = []
        
        # Validate connection
        if not config.connection:
            errors.append("Connection configuration is required")
        else:
            required_fields = ['glue_connection_name', 'region']
            for field in required_fields:
                if field not in config.connection:
                    errors.append(f"Connection field '{field}' is required")
        
        # Validate task groups
        if not config.task_groups:
            errors.append("At least one task group is required")
        
        for i, group in enumerate(config.task_groups):
            if not group.name:
                errors.append(f"Task group {i} must have a name")
            
            if not group.tasks:
                errors.append(f"Task group '{group.name}' must have at least one task")
            
            for j, task in enumerate(group.tasks):
                task_errors = self._validate_task(task, f"Task group '{group.name}', task {j}")
                errors.extend(task_errors)
        
        return errors
    
    def _validate_task(self, task: TaskConfig, context: str) -> List[str]:
        """Validate a single task configuration."""
        errors = []
        
        if not task.name:
            errors.append(f"{context}: Task must have a name")
        
        if task.type == TaskType.UPSERT:
            errors.extend(self._validate_upsert_task(task.config, context))
        elif task.type == TaskType.STORED_PROCEDURE:
            errors.extend(self._validate_stored_procedure_task(task.config, context))
        elif task.type == TaskType.SQL_QUERY:
            errors.extend(self._validate_sql_query_task(task.config, context))
        
        return errors
    
    def _validate_upsert_task(self, config: UpsertTaskConfig, context: str) -> List[str]:
        """Validate upsert task configuration."""
        errors = []
        
        if not config.source_table and not config.source_query:
            errors.append(f"{context}: Either source_table or source_query is required")
        
        if config.source_table and config.source_query:
            errors.append(f"{context}: Cannot specify both source_table and source_query")
        
        if not config.target_table:
            errors.append(f"{context}: target_table is required")
        
        if not config.primary_key:
            errors.append(f"{context}: primary_key is required")
        
        if not config.checksum_columns:
            errors.append(f"{context}: checksum_columns is required")
        
        return errors
    
    def _validate_stored_procedure_task(self, config: StoredProcedureTaskConfig, context: str) -> List[str]:
        """Validate stored procedure task configuration."""
        errors = []
        
        if not config.name:
            errors.append(f"{context}: Stored procedure name is required")
        
        return errors
    
    def _validate_sql_query_task(self, config: SqlQueryTaskConfig, context: str) -> List[str]:
        """Validate SQL query task configuration."""
        errors = []
        
        if not config.query:
            errors.append(f"{context}: SQL query is required")
        
        return errors