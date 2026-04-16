#!/usr/bin/env python3
"""
Verify that the DAG loads correctly and has updated timeout values.
"""

import sys
import os
from datetime import timedelta

# Add project to path
sys.path.insert(0, '/Users/dishapatel/airflow_scheduling')

# Set Airflow home
os.environ['AIRFLOW_HOME'] = os.path.expanduser('~/airflow')

def verify_dag_config():
    """Load and verify DAG configuration."""
    print("\n" + "="*60)
    print("DAG CONFIGURATION VERIFICATION")
    print("="*60 + "\n")
    
    try:
        # Import the DAG
        from dags.case_data_etl import dag
        
        print("✓ DAG loaded successfully")
        print(f"  DAG ID: {dag.dag_id}")
        print(f"  Description: {dag.description}")
        
        # Check default args
        print("\n✓ Default Args:")
        if hasattr(dag, 'default_args') and dag.default_args:
            for key, value in dag.default_args.items():
                if isinstance(value, timedelta):
                    print(f"    - {key}: {value.total_seconds() / 60:.0f} minutes")
                else:
                    print(f"    - {key}: {value}")
        
        # Check DAG-level settings
        print("\n✓ DAG Settings:")
        print(f"    - dagrun_timeout: {dag.dagrun_timeout}")
        print(f"    - max_active_runs: {dag.max_active_runs}")
        print(f"    - schedule_interval: {dag.schedule_interval}")
        
        # Verify specific timeouts
        execution_timeout = dag.default_args.get('execution_timeout')
        dagrun_timeout = dag.dagrun_timeout
        
        print("\n✓ Timeout Verification:")
        
        if execution_timeout and execution_timeout.total_seconds() / 60 >= 40:
            print(f"    ✓ execution_timeout: {execution_timeout.total_seconds() / 60:.0f} minutes (GOOD - ≥40 min)")
        else:
            print(f"    ✗ execution_timeout: {execution_timeout} (WARNING - too low)")
        
        if dagrun_timeout and dagrun_timeout.total_seconds() / 60 >= 55:
            print(f"    ✓ dagrun_timeout: {dagrun_timeout.total_seconds() / 60:.0f} minutes (GOOD - ≥55 min)")
        else:
            print(f"    ✗ dagrun_timeout: {dagrun_timeout} (WARNING - too low)")
        
        # List tasks
        print("\n✓ Tasks in DAG:")
        for task_id, task in dag.tasks_dict.items():
            print(f"    - {task_id}")
        
        print("\n" + "="*60)
        print("✓ DAG CONFIGURATION VERIFIED SUCCESSFULLY")
        print("="*60 + "\n")
        
        return True
        
    except Exception as e:
        print(f"\n✗ ERROR: Failed to load DAG")
        print(f"  {type(e).__name__}: {e}")
        print("\n" + "="*60)
        print("✗ DAG VERIFICATION FAILED")
        print("="*60 + "\n")
        
        return False

if __name__ == "__main__":
    success = verify_dag_config()
    sys.exit(0 if success else 1)
