# Exercise 3: Build an Airflow DAG

## Objective
Create an Apache Airflow DAG that orchestrates a multi-step data pipeline.

## Background
You need to automate a daily data pipeline that processes customer activity logs. The pipeline should extract data, transform it, load it to a database, and send a notification.

## Requirements

### DAG Configuration
- Name: `customer_activity_pipeline`
- Schedule: Daily at 3 AM
- Owner: Your name
- Retries: 2
- Retry delay: 5 minutes
- Email on failure: True

### Tasks

**Task 1: Extract Customer Activity (`extract_activity`)**
- Read customer activity logs from a CSV file
- File path should use Airflow variable: `activity_logs_path`
- Push extracted data to XCom

**Task 2: Transform Data (`transform_activity`)**
- Pull data from previous task
- Remove duplicate activities
- Calculate activity duration (end_time - start_time)
- Categorize activity type (reading, writing, browsing)
- Push transformed data to XCom

**Task 3: Calculate Metrics (`calculate_metrics`)**
- Calculate:
  - Total activities per customer
  - Average activity duration
  - Most common activity type
- Push metrics to XCom

**Task 4: Load to Database (`load_to_database`)**
- Simulate loading transformed data to database
- In production, would use SQLAlchemy
- For this exercise, save to CSV

**Task 5: Data Quality Check (`quality_check`)**
- Verify no null values in critical fields
- Check that all durations are positive
- Verify minimum record count threshold
- Raise error if checks fail

**Task 6: Send Notification (`send_notification`)**
- Use BashOperator to print success message
- Include execution date and record count

### Task Dependencies

```
extract_activity >> transform_activity >> [calculate_metrics, load_to_database] >> quality_check >> send_notification
```

## Starter Code

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    # TODO: Define default arguments
}

dag = DAG(
    # TODO: Define DAG
)

def extract_activity_data(**context):
    """Extract customer activity data"""
    # TODO: Implement extraction
    pass

def transform_activity_data(**context):
    """Transform activity data"""
    # TODO: Implement transformation
    pass

def calculate_activity_metrics(**context):
    """Calculate aggregated metrics"""
    # TODO: Implement metric calculation
    pass

def load_data(**context):
    """Load data to database"""
    # TODO: Implement loading
    pass

def check_data_quality(**context):
    """Perform data quality checks"""
    # TODO: Implement quality checks
    pass

# TODO: Define tasks and dependencies
```

## Sample Input Data

Create `customer_activity.csv`:
```csv
activity_id,customer_id,activity_type,start_time,end_time,page_views
1,C001,browsing,2024-01-15 10:00:00,2024-01-15 10:15:00,5
2,C002,reading,2024-01-15 10:05:00,2024-01-15 10:25:00,3
3,C001,writing,2024-01-15 11:00:00,2024-01-15 11:30:00,1
4,C003,browsing,2024-01-15 14:00:00,2024-01-15 14:10:00,8
1,C001,browsing,2024-01-15 10:00:00,2024-01-15 10:15:00,5
```

## Expected Behavior

1. DAG should appear in Airflow UI
2. Manual trigger should execute all tasks in order
3. Duplicate activities should be removed
4. Metrics should be calculated correctly
5. Quality checks should pass
6. Success notification should be printed

## Testing Your DAG

```bash
# Test import
python your_dag.py

# List DAGs
airflow dags list

# Test specific task
airflow tasks test customer_activity_pipeline extract_activity 2024-01-15

# Backfill
airflow dags backfill customer_activity_pipeline -s 2024-01-15 -e 2024-01-15
```

## Validation Criteria

Your DAG should:
- Have correct schedule and configuration
- Execute tasks in proper order
- Handle XCom communication between tasks
- Implement proper error handling
- Pass data quality checks
- Be idempotent (can run multiple times safely)

## Bonus Challenges

1. Add a branching task that routes to different processing based on data volume
2. Implement a sensor that waits for the input file to exist
3. Add a task that sends email notification with summary statistics
4. Create dynamic task generation based on customer segments
5. Add SLA monitoring for task execution times
6. Implement a cleanup task that runs on success or failure

## Tips

- Test each task function independently before creating the DAG
- Use `airflow tasks test` for individual task testing
- Check Airflow logs for debugging
- Use XCom wisely (it has size limits)
- Make tasks idempotent
- Add logging statements for debugging
