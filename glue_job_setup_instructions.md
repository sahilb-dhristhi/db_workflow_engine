# AWS Glue Job Setup Instructions

## Overview
This guide explains how to set up a common AWS Glue Python shell job that can process YAML configurations stored in S3.

## Files
- `glue_job_runner.py` - The main Glue job script (common for all jobs)
- Your YAML configuration files stored in S3

## Setup Steps

### 1. Upload the Library to S3
First, package and upload the `glue_yaml_processor` library to an S3 bucket:

```bash
# Create a zip file of the library
cd /path/to/your/project
zip -r glue_yaml_processor.zip glue_yaml_processor/

# Upload to S3
aws s3 cp glue_yaml_processor.zip s3://your-glue-scripts-bucket/libraries/
```

### 2. Upload the Main Script
Upload the main Glue job script:

```bash
aws s3 cp glue_job_runner.py s3://your-glue-scripts-bucket/scripts/
```

### 3. Upload YAML Configuration
Upload your YAML configuration to S3:

```bash
aws s3 cp test_config.yaml s3://your-config-bucket/configs/parallel_procedures.yaml
```

### 4. Create Glue Job

#### Option A: Using AWS Console

1. Go to AWS Glue Console
2. Click "Jobs" → "Create Job"
3. Configure the job:
   - **Name**: `yaml-processor-job`
   - **Type**: `Python shell`
   - **Python version**: `Python 3.9`
   - **Script location**: `s3://your-glue-scripts-bucket/scripts/glue_job_runner.py`
   - **Library path**: `s3://your-glue-scripts-bucket/libraries/glue_yaml_processor.zip`

4. Add job parameters:
   - `S3_BUCKET`: `your-config-bucket`
   - `S3_KEY`: `configs/parallel_procedures.yaml`
   - `REGION`: `ap-northeast-1`

5. Configure connections:
   - Add your MySQL connection (e.g., `aurora_connection`)

#### Option B: Using AWS CLI

Create a job definition file `job-definition.json`:

```json
{
    "Name": "yaml-processor-job",
    "Role": "arn:aws:iam::YOUR_ACCOUNT:role/GlueJobRole",
    "Command": {
        "Name": "pythonshell",
        "ScriptLocation": "s3://your-glue-scripts-bucket/scripts/glue_job_runner.py",
        "PythonVersion": "3.9"
    },
    "DefaultArguments": {
        "--job-language": "python",
        "--additional-python-modules": "PyMySQL,PyYAML",
        "--extra-py-files": "s3://your-glue-scripts-bucket/libraries/glue_yaml_processor.zip",
        "--S3_BUCKET": "your-config-bucket",
        "--S3_KEY": "configs/parallel_procedures.yaml",
        "--REGION": "ap-northeast-1"
    },
    "Connections": {
        "Connections": ["aurora_connection"]
    },
    "MaxRetries": 0,
    "AllocatedCapacity": 1,
    "Timeout": 60,
    "GlueVersion": "3.0"
}
```

Then create the job:

```bash
aws glue create-job --cli-input-json file://job-definition.json
```

### 5. Run the Job

#### Using AWS Console
1. Go to the job details
2. Click "Run job"
3. Optionally override parameters if needed

#### Using AWS CLI
```bash
# Run with default parameters
aws glue start-job-run --job-name yaml-processor-job

# Run with custom parameters
aws glue start-job-run --job-name yaml-processor-job --arguments '{
    "--S3_BUCKET": "your-config-bucket",
    "--S3_KEY": "configs/another_config.yaml",
    "--REGION": "ap-northeast-1"
}'
```

## Job Parameters

The job expects these parameters:

| Parameter | Description | Example |
|-----------|-------------|---------|
| `S3_BUCKET` | S3 bucket containing YAML config | `my-config-bucket` |
| `S3_KEY` | S3 key (path) to YAML file | `configs/workflow.yaml` |
| `REGION` | AWS region | `ap-northeast-1` |

## Example YAML Configuration

Your YAML file in S3 should follow this format:

```yaml
version: "1.0"

connection:
  glue_connection_name: "aurora_connection"
  region: "ap-northeast-1"

task_groups:
  - name: "parallel_stored_procedures"
    execution_mode: "parallel"
    max_workers: 2
    enabled: true
    tasks:
      - name: "execute_proc1"
        type: "stored_procedure"
        enabled: true
        config:
          name: "TEST_PROCEDURE_0001"
          args: []
          stop_on_failure: false
          
      - name: "execute_proc2"
        type: "stored_procedure"
        enabled: true
        config:
          name: "TEST_PROCEDURE_0002"
          args: []
          stop_on_failure: false
```

## Monitoring

### CloudWatch Logs
Check CloudWatch logs for job execution details:
- Log group: `/aws-glue/python-jobs/output`
- Log stream: `{job-name}_{job-run-id}`

### Job Run History
Monitor job runs in the Glue console:
- Go to Jobs → Select your job → "History" tab
- Check run status, duration, and logs

## Creating Different Jobs

To create different jobs for different workflows:

1. **Same script, different YAML**: Use the same `glue_job_runner.py` but point to different S3 YAML files
2. **Different job names**: Create multiple Glue jobs with different names but same script
3. **Parameter overrides**: Use job parameters to customize behavior

### Example: Multiple Job Setup

```bash
# Job 1: Customer data processing
aws glue start-job-run --job-name yaml-processor-job --arguments '{
    "--S3_BUCKET": "config-bucket",
    "--S3_KEY": "workflows/customer_processing.yaml",
    "--REGION": "ap-northeast-1"
}'

# Job 2: Order data processing  
aws glue start-job-run --job-name yaml-processor-job --arguments '{
    "--S3_BUCKET": "config-bucket", 
    "--S3_KEY": "workflows/order_processing.yaml",
    "--REGION": "ap-northeast-1"
}'
```

## Troubleshooting

### Common Issues

1. **Import errors**: Ensure `glue_yaml_processor.zip` is uploaded and referenced correctly
2. **Connection errors**: Verify Glue connection is configured and accessible
3. **Permission errors**: Check IAM roles have necessary S3 and Glue permissions
4. **YAML validation errors**: Validate YAML syntax and required fields

### Required IAM Permissions

The Glue job role needs these permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::your-config-bucket",
                "arn:aws:s3:::your-config-bucket/*",
                "arn:aws:s3:::your-glue-scripts-bucket",
                "arn:aws:s3:::your-glue-scripts-bucket/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "glue:GetConnection",
                "secretsmanager:GetSecretValue"
            ],
            "Resource": "*"
        }
    ]
}
```