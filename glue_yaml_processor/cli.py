"""
Command-line interface for the YAML processor
"""

import sys
import json
import argparse
from typing import Optional
from awsglue.utils import getResolvedOptions

from .core.processor import YAMLProcessor


def main():
    """Main entry point for the CLI."""
    try:
        # Try to get options from Glue first
        args = getResolvedOptions(
            sys.argv,
            [
                'YAML_CONFIG_TYPE',  # 'file', 's3', or 'string'
                'YAML_CONFIG_VALUE',  # file path, S3 URI, or YAML content
                'REGION'
            ]
        )
        
        config_type = args['YAML_CONFIG_TYPE']
        config_value = args['YAML_CONFIG_VALUE']
        region = args['REGION']
        
        print(f"Processing YAML configuration from {config_type}: {config_value}")
        
        # Create processor based on configuration type
        if config_type == 'file':
            processor = YAMLProcessor.from_file(config_value, region)
        elif config_type == 's3':
            # Parse S3 URI (s3://bucket/key)
            if config_value.startswith('s3://'):
                s3_path = config_value[5:]  # Remove 's3://'
                bucket, key = s3_path.split('/', 1)
                processor = YAMLProcessor.from_s3(bucket, key, region)
            else:
                raise ValueError("S3 URI must start with 's3://'")
        elif config_type == 'string':
            processor = YAMLProcessor.from_string(config_value, region)
        else:
            raise ValueError(f"Unknown config type: {config_type}")
        
        # Process the configuration
        results = processor.process()
        
        # Print results
        print("\n" + "="*50)
        print("EXECUTION RESULTS")
        print("="*50)
        print(json.dumps(results, indent=2, default=str))
        
        # Exit with appropriate code
        if results['success']:
            print(f"\nSUCCESS: Processed {results['successful_tasks']} tasks successfully")
            sys.exit(0)
        else:
            print(f"\nFAILURE: {results['failed_tasks']} tasks failed")
            sys.exit(1)
            
    except Exception as e:
        print(f"ERROR: {str(e)}")
        sys.exit(1)


def standalone_main():
    """Standalone entry point for testing outside of Glue."""
    parser = argparse.ArgumentParser(description='Process YAML configurations for AWS Glue')
    parser.add_argument('--config-type', 
                       choices=['file', 's3', 'string'],
                       required=True,
                       help='Type of configuration source')
    parser.add_argument('--config-value',
                       required=True,
                       help='Configuration value (file path, S3 URI, or YAML content)')
    parser.add_argument('--region',
                       default='ap-northeast-1',
                       help='AWS region')
    
    args = parser.parse_args()
    
    try:
        print(f"Processing YAML configuration from {args.config_type}: {args.config_value}")
        
        # Create processor based on configuration type
        if args.config_type == 'file':
            processor = YAMLProcessor.from_file(args.config_value, args.region)
        elif args.config_type == 's3':
            # Parse S3 URI (s3://bucket/key)
            if args.config_value.startswith('s3://'):
                s3_path = args.config_value[5:]  # Remove 's3://'
                bucket, key = s3_path.split('/', 1)
                processor = YAMLProcessor.from_s3(bucket, key, args.region)
            else:
                raise ValueError("S3 URI must start with 's3://'")
        elif args.config_type == 'string':
            processor = YAMLProcessor.from_string(args.config_value, args.region)
        else:
            raise ValueError(f"Unknown config type: {args.config_type}")
        
        # Process the configuration
        results = processor.process()
        
        # Print results
        print("\n" + "="*50)
        print("EXECUTION RESULTS")
        print("="*50)
        print(json.dumps(results, indent=2, default=str))
        
        # Exit with appropriate code
        if results['success']:
            print(f"\nSUCCESS: Processed {results['successful_tasks']} tasks successfully")
            sys.exit(0)
        else:
            print(f"\nFAILURE: {results['failed_tasks']} tasks failed")
            sys.exit(1)
            
    except Exception as e:
        print(f"ERROR: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    standalone_main()