"""
Main YAML processor that orchestrates task execution
"""

import time
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from .connection import MySQLConnectionManager
from .yaml_parser import YAMLParser, YAMLConfig, TaskGroupConfig, TaskConfig, TaskType, ExecutionMode
from ..tasks.upsert import SmartUpsert
from ..tasks.stored_procedure import StoredProcedureExecutor


class YAMLProcessor:
    """Main processor that executes YAML configurations."""
    
    def __init__(self, yaml_config: YAMLConfig):
        """
        Initialize YAML processor.
        
        Args:
            yaml_config: Parsed YAML configuration
        """
        self.config = yaml_config
        self.connection_manager = MySQLConnectionManager(
            glue_connection_name=yaml_config.connection['glue_connection_name'],
            region_name=yaml_config.connection.get('region', 'ap-northeast-1')
        )
        self.smart_upsert = SmartUpsert(self.connection_manager)
        self.stored_procedure_executor = StoredProcedureExecutor(self.connection_manager)
    
    def process(self) -> Dict[str, Any]:
        """
        Process the YAML configuration and execute all tasks.
        
        Returns:
            Dictionary with execution results
        """
        start_time = time.time()
        
        results = {
            'success': True,
            'start_time': start_time,
            'task_group_results': [],
            'total_tasks': 0,
            'successful_tasks': 0,
            'failed_tasks': 0
        }
        
        try:
            # Process each task group
            for group_config in self.config.task_groups:
                if not group_config.enabled:
                    continue
                
                group_result = self._process_task_group(group_config)
                results['task_group_results'].append(group_result)
                
                # Update counters
                results['total_tasks'] += group_result['total_tasks']
                results['successful_tasks'] += group_result['successful_tasks']
                results['failed_tasks'] += group_result['failed_tasks']
                
                # Check if group failed and should stop
                if not group_result['success']:
                    results['success'] = False
                    break
            
            end_time = time.time()
            results['end_time'] = end_time
            results['duration'] = end_time - start_time
            
        except Exception as e:
            results['success'] = False
            results['error'] = str(e)
            results['end_time'] = time.time()
            results['duration'] = results['end_time'] - start_time
        
        return results
    
    def _process_task_group(self, group_config: TaskGroupConfig) -> Dict[str, Any]:
        """
        Process a single task group.
        
        Args:
            group_config: Task group configuration
            
        Returns:
            Task group execution results
        """
        start_time = time.time()
        
        group_result = {
            'name': group_config.name,
            'success': True,
            'start_time': start_time,
            'execution_mode': group_config.execution_mode.value,
            'task_results': [],
            'total_tasks': 0,
            'successful_tasks': 0,
            'failed_tasks': 0
        }
        
        try:
            # Filter enabled tasks
            enabled_tasks = [task for task in group_config.tasks if task.enabled]
            group_result['total_tasks'] = len(enabled_tasks)
            
            if not enabled_tasks:
                group_result['message'] = 'No enabled tasks in group'
                group_result['end_time'] = time.time()
                group_result['duration'] = group_result['end_time'] - start_time
                return group_result
            
            # Execute tasks based on execution mode
            if group_config.execution_mode == ExecutionMode.SEQUENTIAL:
                task_results = self._execute_tasks_sequential(enabled_tasks)
            else:
                task_results = self._execute_tasks_parallel(enabled_tasks, group_config.max_workers)
            
            group_result['task_results'] = task_results
            
            # Count successful/failed tasks
            for task_result in task_results:
                if task_result['success']:
                    group_result['successful_tasks'] += 1
                else:
                    group_result['failed_tasks'] += 1
                    group_result['success'] = False
            
            end_time = time.time()
            group_result['end_time'] = end_time
            group_result['duration'] = end_time - start_time
            
        except Exception as e:
            group_result['success'] = False
            group_result['error'] = str(e)
            group_result['end_time'] = time.time()
            group_result['duration'] = group_result['end_time'] - start_time
        
        return group_result
    
    def _execute_tasks_sequential(self, tasks: List[TaskConfig]) -> List[Dict[str, Any]]:
        """
        Execute tasks sequentially.
        
        Args:
            tasks: List of task configurations
            
        Returns:
            List of task execution results
        """
        results = []
        
        for task in tasks:
            result = self._execute_task(task)
            results.append(result)
            
            # Stop on failure if configured
            if not result['success'] and getattr(task.config, 'stop_on_failure', False):
                break
        
        return results
    
    def _execute_tasks_parallel(
        self, 
        tasks: List[TaskConfig], 
        max_workers: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute tasks in parallel.
        
        Args:
            tasks: List of task configurations
            max_workers: Maximum number of parallel workers
            
        Returns:
            List of task execution results
        """
        if max_workers is None:
            max_workers = min(len(tasks), 10)
        
        results = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_task = {}
            for i, task in enumerate(tasks):
                future = executor.submit(self._execute_task, task)
                future_to_task[future] = {'task': task, 'index': i}
            
            # Collect results
            for future in as_completed(future_to_task):
                task_info = future_to_task[future]
                try:
                    result = future.result()
                    result['index'] = task_info['index']
                    results.append(result)
                except Exception as e:
                    results.append({
                        'success': False,
                        'task_name': task_info['task'].name,
                        'task_type': task_info['task'].type.value,
                        'message': f'Error in parallel execution: {str(e)}',
                        'error': str(e),
                        'index': task_info['index']
                    })
        
        # Sort results by original order
        results.sort(key=lambda x: x.get('index', 0))
        
        return results
    
    def _execute_task(self, task: TaskConfig) -> Dict[str, Any]:
        """
        Execute a single task.
        
        Args:
            task: Task configuration
            
        Returns:
            Task execution result
        """
        start_time = time.time()
        
        result = {
            'task_name': task.name,
            'task_type': task.type.value,
            'start_time': start_time,
            'success': False
        }
        
        try:
            if task.type == TaskType.UPSERT:
                result.update(self._execute_upsert_task(task))
            elif task.type == TaskType.STORED_PROCEDURE:
                result.update(self._execute_stored_procedure_task(task))
            elif task.type == TaskType.SQL_QUERY:
                result.update(self._execute_sql_query_task(task))
            else:
                result['message'] = f'Unknown task type: {task.type}'
                result['error'] = f'Unknown task type: {task.type}'
            
            end_time = time.time()
            result['end_time'] = end_time
            result['duration'] = end_time - start_time
            
        except Exception as e:
            result['success'] = False
            result['message'] = f'Error executing task: {str(e)}'
            result['error'] = str(e)
            result['end_time'] = time.time()
            result['duration'] = result['end_time'] - start_time
        
        return result
    
    def _execute_upsert_task(self, task: TaskConfig) -> Dict[str, Any]:
        """Execute an upsert task."""
        config = task.config
        
        if config.source_table:
            return self.smart_upsert.execute_upsert(
                source_table=config.source_table,
                target_table=config.target_table,
                primary_key=config.primary_key,
                checksum_columns=config.checksum_columns,
                checksum_column_name=config.checksum_column_name
            )
        else:
            return self.smart_upsert.execute_custom_upsert(
                source_query=config.source_query,
                target_table=config.target_table,
                primary_key=config.primary_key,
                checksum_columns=config.checksum_columns,
                checksum_column_name=config.checksum_column_name
            )
    
    def _execute_stored_procedure_task(self, task: TaskConfig) -> Dict[str, Any]:
        """Execute a stored procedure task."""
        config = task.config
        
        return self.stored_procedure_executor.execute_procedure(
            procedure_name=config.name,
            args=config.args
        )
    
    def _execute_sql_query_task(self, task: TaskConfig) -> Dict[str, Any]:
        """Execute a SQL query task."""
        config = task.config
        
        return self.stored_procedure_executor.execute_sql_query(
            query=config.query,
            params=config.params,
            fetch_results=config.fetch_results
        )
    
    @classmethod
    def from_file(cls, file_path: str, region_name: str = 'ap-northeast-1') -> 'YAMLProcessor':
        """
        Create processor from YAML file.
        
        Args:
            file_path: Path to YAML file
            region_name: AWS region name
            
        Returns:
            YAMLProcessor instance
        """
        parser = YAMLParser(region_name)
        config = parser.parse_from_file(file_path)
        
        # Validate configuration
        errors = parser.validate_config(config)
        if errors:
            raise ValueError(f"Invalid YAML configuration: {', '.join(errors)}")
        
        return cls(config)
    
    @classmethod
    def from_s3(cls, bucket: str, key: str, region_name: str = 'ap-northeast-1') -> 'YAMLProcessor':
        """
        Create processor from S3 YAML file.
        
        Args:
            bucket: S3 bucket name
            key: S3 object key
            region_name: AWS region name
            
        Returns:
            YAMLProcessor instance
        """
        parser = YAMLParser(region_name)
        config = parser.parse_from_s3(bucket, key)
        
        # Validate configuration
        errors = parser.validate_config(config)
        if errors:
            raise ValueError(f"Invalid YAML configuration: {', '.join(errors)}")
        
        return cls(config)
    
    @classmethod
    def from_string(cls, yaml_content: str, region_name: str = 'ap-northeast-1') -> 'YAMLProcessor':
        """
        Create processor from YAML string.
        
        Args:
            yaml_content: YAML content as string
            region_name: AWS region name
            
        Returns:
            YAMLProcessor instance
        """
        parser = YAMLParser(region_name)
        config = parser.parse_from_string(yaml_content)
        
        # Validate configuration
        errors = parser.validate_config(config)
        if errors:
            raise ValueError(f"Invalid YAML configuration: {', '.join(errors)}")
        
        return cls(config)