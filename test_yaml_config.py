#!/usr/bin/env python3
"""
Test script to validate YAML configuration
"""

import sys
import json
from glue_yaml_processor.core.yaml_parser import YAMLParser

def test_yaml_config():
    """Test the YAML configuration parsing and validation."""
    
    # Initialize parser
    parser = YAMLParser(region_name='ap-northeast-1')
    
    try:
        # Parse the YAML file
        print("Parsing YAML configuration...")
        config = parser.parse_from_file('./test_config.yaml')
        
        print("✅ YAML parsed successfully!")
        print(f"Version: {config.version}")
        print(f"Connection: {config.connection}")
        print(f"Task Groups: {len(config.task_groups)}")
        
        # Validate configuration
        print("\nValidating configuration...")
        errors = parser.validate_config(config)
        
        if errors:
            print("❌ Validation errors found:")
            for error in errors:
                print(f"  - {error}")
            return False
        else:
            print("✅ Configuration is valid!")
        
        # Print detailed configuration
        print("\n" + "="*50)
        print("CONFIGURATION DETAILS")
        print("="*50)
        
        for i, group in enumerate(config.task_groups):
            print(f"\nTask Group {i+1}: {group.name}")
            print(f"  Execution Mode: {group.execution_mode.value}")
            print(f"  Max Workers: {group.max_workers}")
            print(f"  Enabled: {group.enabled}")
            print(f"  Tasks: {len(group.tasks)}")
            
            for j, task in enumerate(group.tasks):
                print(f"    Task {j+1}: {task.name}")
                print(f"      Type: {task.type.value}")
                print(f"      Enabled: {task.enabled}")
                print(f"      Config: {task.config.__dict__}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_yaml_config()
    sys.exit(0 if success else 1)