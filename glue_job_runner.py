#!/usr/bin/env python3
"""
Common AWS Glue Python Shell Job Runner
This script can be used for all Glue jobs that process YAML configurations from S3.
"""

import sys
import json
import boto3
from awsglue.utils import getResolvedOptions
from glue_yaml_processor.core.processor import YAMLProcessor


def main():
    """Main entry point for the Glue job."""
    
    # Get job parameters from Glue
    try:
        args = getResolvedOptions(
            sys.argv,
            [
                'S3_BUCKET',        # S3 bucket containing YAML config
                'S3_KEY',           # S3 key (path) to YAML config file
                'REGION'            # AWS region
            ]
        )
        
        s3_bucket = args['S3_BUCKET']
        s3_key = args['S3_KEY']
        region = args['REGION']
        
        print(f"Starting Glue job with parameters:")
        print(f"  S3 Bucket: {s3_bucket}")
        print(f"  S3 Key: {s3_key}")
        print(f"  Region: {region}")
        
    except Exception as e:
        print(f"ERROR: Failed to get job parameters: {str(e)}")
        print("Required parameters: S3_BUCKET, S3_KEY, REGION")
        sys.exit(1)
    
    try:
        # Create processor from S3 configuration
        print(f"\nLoading YAML configuration from s3://{s3_bucket}/{s3_key}")
        processor = YAMLProcessor.from_s3(
            bucket=s3_bucket,
            key=s3_key,
            region_name=region
        )
        
        print("✅ YAML configuration loaded and validated successfully")
        
        # Process the configuration
        print("\n" + "="*60)
        print("STARTING TASK EXECUTION")
        print("="*60)
        
        results = processor.process()
        
        # Print execution summary
        print("\n" + "="*60)
        print("EXECUTION SUMMARY")
        print("="*60)
        print(f"Overall Status: {'SUCCESS' if results['success'] else 'FAILURE'}")
        print(f"Total Tasks: {results['total_tasks']}")
        print(f"Successful Tasks: {results['successful_tasks']}")
        print(f"Failed Tasks: {results['failed_tasks']}")
        print(f"Execution Duration: {results['duration']:.2f} seconds")
        
        # Print task group details
        print(f"\nTask Group Results:")
        for group_result in results['task_group_results']:
            print(f"  - {group_result['name']}: {'✅' if group_result['success'] else '❌'} "
                  f"({group_result['successful_tasks']}/{group_result['total_tasks']} tasks)")
        
        # Print detailed results (optional - can be disabled for cleaner output)
        if results.get('failed_tasks', 0) > 0:
            print("\n" + "="*60)
            print("DETAILED RESULTS")
            print("="*60)
            print(json.dumps(results, indent=2, default=str))
        
        # Exit with appropriate code
        if results['success']:
            print(f"\n🎉 JOB COMPLETED SUCCESSFULLY")
            sys.exit(0)
        else:
            print(f"\n❌ JOB FAILED")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {str(e)}")
        print(f"Job failed with exception: {type(e).__name__}")
        sys.exit(1)


if __name__ == "__main__":
    main()