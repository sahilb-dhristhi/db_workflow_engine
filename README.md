# Glue YAML Processor

A Python library for processing YAML configurations in AWS Glue Python shell jobs. This library provides a unified interface for executing MySQL operations, stored procedures, smart upserts, and parallel task execution based on YAML configuration files.

## Features

- **Smart Upsert**: Efficient upsert operations using MD5 checksums for change detection
- **Stored Procedure Execution**: Execute MySQL stored procedures with support for parameters
- **SQL Query Execution**: Run arbitrary SQL queries with parameter support
- **Parallel Execution**: Execute tasks in parallel or sequential mode
- **YAML Configuration**: Define complex workflows using simple YAML files
- **AWS Integration**: Seamless integration with AWS Glue connections and Secrets Manager
- **S3 Support**: Load YAML configurations from S3 buckets

## Installation

```bash
uv add glue-yaml-processor
```

## Quick Start

### 1. Basic Usage

```python
from glue_yaml_processor import YAMLProcessor

# Create processor from YAML file
processor = YAMLProcessor.from_file('config.yaml')

# Process the configuration
results = processor.process()

print(f"Success: {results['success']}")
print(f"Tasks processed: {results['total_tasks']}")
```

### 2. AWS Glue Job Usage

```python
import sys
from awsglue.utils import getResolvedOptions
from glue_yaml_processor import YAMLProcessor

# Get job parameters
args = getResolvedOptions(sys.argv, ['YAML_CONFIG_TYPE', 'YAML_CONFIG_VALUE', 'REGION'])

# Create processor from S3
processor = YAMLProcessor.from_s3(
    bucket='my-bucket',
    key='configs/workflow.yaml',
    region_name=args['REGION']
)

# Execute workflow
results = processor.process()
```

### 3. CLI Usage

```bash
# From file
glue-yaml-processor --config-type file --config-value ./config.yaml --region ap-northeast-1

# From S3
glue-yaml-processor --config-type s3 --config-value s3://my-bucket/config.yaml --region ap-northeast-1
```

## YAML Configuration Format

### Basic Structure

```yaml
version: "1.0"

connection:
  glue_connection_name: "my-mysql-connection"
  region: "ap-northeast-1"

task_groups:
  - name: "data_processing"
    execution_mode: "sequential"  # or "parallel"
    enabled: true
    max_workers: 3  # only for parallel mode
    tasks:
      - name: "task1"
        type: "upsert"
        enabled: true
        config:
          # task-specific configuration
```

### Task Types

#### 1. Smart Upsert

```yaml
- name: "upsert_customers"
  type: "upsert"
  enabled: true
  config:
    source_table: "staging_customers"
    target_table: "customers"
    primary_key: "customer_id"
    checksum_columns:
      - "first_name"
      - "last_name"
      - "email"
      - "phone"
    checksum_column_name: "checksum_val"
```

#### 2. Custom Query Upsert

```yaml
- name: "upsert_metrics"
  type: "upsert"
  enabled: true
  config:
    source_query: |
      SELECT 
        customer_id,
        COUNT(*) as order_count,
        SUM(total) as total_spent
      FROM orders 
      WHERE created_date >= CURDATE() - INTERVAL 30 DAY
      GROUP BY customer_id
    target_table: "customer_metrics"
    primary_key: "customer_id"
    checksum_columns:
      - "order_count"
      - "total_spent"
```

#### 3. Stored Procedure

```yaml
- name: "update_analytics"
  type: "stored_procedure"
  enabled: true
  config:
    name: "update_customer_analytics"
    args: ["param1", "param2"]
    stop_on_failure: false
```

#### 4. SQL Query

```yaml
- name: "cleanup_temp_tables"
  type: "sql_query"
  enabled: true
  config:
    query: "TRUNCATE TABLE temp_staging"
    params: []
    fetch_results: false
    stop_on_failure: true
```

### Execution Modes

#### Sequential Execution

```yaml
task_groups:
  - name: "sequential_tasks"
    execution_mode: "sequential"
    tasks:
      - name: "task1"
        # ... task configuration
      - name: "task2"
        # ... task configuration
```

#### Parallel Execution

```yaml
task_groups:
  - name: "parallel_tasks"
    execution_mode: "parallel"
    max_workers: 3
    tasks:
      - name: "task1"
        # ... task configuration
      - name: "task2"
        # ... task configuration
```

## API Reference

### YAMLProcessor

Main class for processing YAML configurations.

#### Methods

- `YAMLProcessor.from_file(file_path, region_name)` - Create from file
- `YAMLProcessor.from_s3(bucket, key, region_name)` - Create from S3
- `YAMLProcessor.from_string(yaml_content, region_name)` - Create from string
- `processor.process()` - Execute all tasks

### MySQLConnectionManager

Manages MySQL connections using AWS Glue connections.

```python
from glue_yaml_processor.core.connection import MySQLConnectionManager

manager = MySQLConnectionManager('my-glue-connection')

# Context manager usage
with manager.get_connection() as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users")
    results = cursor.fetchall()
```

### SmartUpsert

Performs efficient upsert operations using checksums.

```python
from glue_yaml_processor.tasks.upsert import SmartUpsert

upsert = SmartUpsert(connection_manager)

result = upsert.execute_upsert(
    source_table="staging_table",
    target_table="target_table",
    primary_key="id",
    checksum_columns=["name", "email", "phone"]
)
```

### StoredProcedureExecutor

Executes stored procedures and SQL queries.

```python
from glue_yaml_processor.tasks.stored_procedure import StoredProcedureExecutor

executor = StoredProcedureExecutor(connection_manager)

# Execute stored procedure
result = executor.execute_procedure("my_procedure", ["arg1", "arg2"])

# Execute SQL query
result = executor.execute_sql_query("SELECT COUNT(*) FROM users", fetch_results=True)
```

## Examples

See the `examples/` directory for complete YAML configuration examples:

- `smart_upsert_example.yaml` - Basic smart upsert
- `stored_procedure_parallel_example.yaml` - Parallel stored procedure execution
- `complex_workflow_example.yaml` - Multi-stage workflow
- `custom_query_upsert_example.yaml` - Custom query upsert

## AWS Glue Integration

### Connection Setup

1. Create a MySQL connection in AWS Glue Console
2. Configure JDBC URL: `jdbc:mysql://host:port/database`
3. Set up Secrets Manager secret with username/password
4. Reference the secret in the connection configuration

### Job Parameters

For AWS Glue jobs, use these parameters:

- `YAML_CONFIG_TYPE`: 'file', 's3', or 'string'
- `YAML_CONFIG_VALUE`: File path, S3 URI, or YAML content
- `REGION`: AWS region name

## Development

### Setup

```bash
git clone <repository>
cd glue-yaml-processor
uv venv
source .venv/bin/activate
uv sync
```

### Testing

```bash
pytest tests/
```

### Building

```bash
uv build
```

## License

MIT License

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## Support

For issues and questions, please use the GitHub issue tracker.