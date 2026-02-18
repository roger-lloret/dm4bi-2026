# Apache Airflow Guide

## What is Apache Airflow?

Apache Airflow is an open-source platform for orchestrating complex data workflows. It allows you to programmatically author, schedule, and monitor workflows as Directed Acyclic Graphs (DAGs).

## Key Concepts

### DAG (Directed Acyclic Graph)
A collection of tasks with defined dependencies and execution order.
- **Directed**: Tasks flow in specific directions
- **Acyclic**: No circular dependencies (no loops)
- **Graph**: Visual representation of the workflow

### Task
A single unit of work in a DAG (e.g., run a Python function, execute a SQL query).

### Operator
A template for a specific type of task:
- `PythonOperator`: Execute Python functions
- `BashOperator`: Execute bash commands
- `SQLOperator`: Execute SQL queries
- `EmailOperator`: Send emails

### Scheduler
The component that triggers workflows based on schedule or dependencies.

### Executor
Determines how tasks are executed (Sequential, Local, Celery, Kubernetes).

## Basic DAG Structure

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Define default arguments
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'my_first_dag',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval='@daily',  # Run once per day
    catchup=False,
)

# Define tasks
def task_1():
    print("Executing task 1")
    return "Task 1 completed"

def task_2():
    print("Executing task 2")
    return "Task 2 completed"

# Create operators
t1 = PythonOperator(
    task_id='task_1',
    python_callable=task_1,
    dag=dag,
)

t2 = PythonOperator(
    task_id='task_2',
    python_callable=task_2,
    dag=dag,
)

# Define dependencies
t1 >> t2  # t1 must complete before t2 starts
```

## Schedule Intervals

Common schedule expressions:

| Expression | Meaning |
|------------|---------|
| `@once` | Run once |
| `@hourly` | Every hour |
| `@daily` | Every day at midnight |
| `@weekly` | Every Sunday at midnight |
| `@monthly` | First day of month at midnight |
| `@yearly` | January 1st at midnight |
| `0 0 * * *` | Daily at midnight (cron) |
| `0 */4 * * *` | Every 4 hours (cron) |
| `timedelta(hours=1)` | Every hour (Python) |

## ETL Pipeline Example with Airflow

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine

default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_sales_etl',
    default_args=default_args,
    description='Daily ETL pipeline for sales data',
    schedule_interval='@daily',
    catchup=False,
)

def extract_data(**context):
    """Extract data from source"""
    # Read from CSV (in production, might be API or database)
    df = pd.read_csv('/data/sales_raw.csv')
    
    # Save to XCom for next task
    context['ti'].xcom_push(key='raw_data', value=df.to_json())
    print(f"Extracted {len(df)} records")

def transform_data(**context):
    """Transform the extracted data"""
    # Get data from previous task
    raw_data = context['ti'].xcom_pull(key='raw_data', task_ids='extract')
    df = pd.read_json(raw_data)
    
    # Data cleaning
    df = df.drop_duplicates()
    df = df.dropna(subset=['transaction_id'])
    
    # Data transformation
    df['total_amount'] = df['quantity'] * df['unit_price']
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])
    
    # Data validation
    assert len(df) > 0, "No data after transformation"
    assert df['total_amount'].min() >= 0, "Negative amounts found"
    
    # Save transformed data
    context['ti'].xcom_push(key='transformed_data', value=df.to_json())
    print(f"Transformed {len(df)} records")

def load_data(**context):
    """Load data to destination"""
    # Get transformed data
    transformed_data = context['ti'].xcom_pull(key='transformed_data', task_ids='transform')
    df = pd.read_json(transformed_data)
    
    # Load to database
    engine = create_engine('postgresql://user:pass@localhost/warehouse')
    df.to_sql('sales_fact', engine, if_exists='append', index=False)
    
    print(f"Loaded {len(df)} records to database")

def data_quality_check(**context):
    """Validate loaded data"""
    engine = create_engine('postgresql://user:pass@localhost/warehouse')
    
    # Check record count
    result = pd.read_sql("SELECT COUNT(*) as cnt FROM sales_fact WHERE DATE(transaction_date) = CURRENT_DATE", engine)
    count = result['cnt'][0]
    
    assert count > 0, "No records loaded for today"
    print(f"Data quality check passed: {count} records found")

# Define tasks
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load_data,
    dag=dag,
)

quality_check_task = PythonOperator(
    task_id='data_quality_check',
    python_callable=data_quality_check,
    dag=dag,
)

# Define task dependencies
extract_task >> transform_task >> load_task >> quality_check_task
```

## Advanced Features

### Task Dependencies

```python
# Linear dependency
task_1 >> task_2 >> task_3

# Multiple dependencies
task_1 >> [task_2, task_3] >> task_4

# Cross dependencies
task_1 >> task_3
task_2 >> task_3
```

### XCom (Cross-Communication)

Share data between tasks:

```python
# Push data
context['ti'].xcom_push(key='my_key', value='my_value')

# Pull data
value = context['ti'].xcom_pull(key='my_key', task_ids='previous_task')
```

### Branching

Conditional execution:

```python
from airflow.operators.python import BranchPythonOperator

def choose_branch(**context):
    value = context['ti'].xcom_pull(task_ids='check_task')
    if value > 100:
        return 'high_value_task'
    else:
        return 'low_value_task'

branch_task = BranchPythonOperator(
    task_id='branch',
    python_callable=choose_branch,
    dag=dag,
)
```

### Sensors

Wait for external conditions:

```python
from airflow.sensors.filesystem import FileSensor

wait_for_file = FileSensor(
    task_id='wait_for_file',
    filepath='/data/input.csv',
    poke_interval=60,  # Check every 60 seconds
    timeout=3600,  # Timeout after 1 hour
    dag=dag,
)
```

## Best Practices

1. **Keep Tasks Atomic**: Each task should do one thing well
2. **Use Idempotent Tasks**: Tasks should produce same result when re-run
3. **Avoid Top-Level Code**: Don't execute code when DAG file is parsed
4. **Use Variables**: Store configuration in Airflow Variables or Connections
5. **Set Appropriate Timeouts**: Prevent tasks from running indefinitely
6. **Implement Data Quality Checks**: Validate data at key points
7. **Use Task Groups**: Organize related tasks for better visualization
8. **Monitor DAG Performance**: Review task duration and failure rates
9. **Document Your DAGs**: Add clear descriptions and comments
10. **Test Locally**: Test DAGs before deploying to production

## Common Patterns

### Backfilling
Run historical DAGs:
```bash
airflow dags backfill -s 2024-01-01 -e 2024-01-31 my_dag
```

### Dynamic DAG Generation
Create DAGs programmatically:
```python
for country in ['US', 'UK', 'DE']:
    dag_id = f'process_{country}_data'
    dag = DAG(dag_id, ...)
    # Define tasks...
    globals()[dag_id] = dag
```

### Failure Callbacks
```python
def on_failure(context):
    # Send alert
    print(f"Task {context['task_instance'].task_id} failed")

task = PythonOperator(
    task_id='my_task',
    python_callable=my_function,
    on_failure_callback=on_failure,
    dag=dag,
)
```

## Troubleshooting

Common issues and solutions:

1. **DAG not appearing**: Check DAG file syntax and location
2. **Tasks not running**: Verify scheduler is running
3. **Import errors**: Ensure all dependencies are installed
4. **Timezone issues**: Set timezone in airflow.cfg
5. **Memory issues**: Optimize task memory usage or use appropriate executor

## Next Steps

- Explore Airflow DAG examples in `/examples/airflow_dags/`
- Practice with Airflow exercises in `/exercises/`
- Read the official Airflow documentation at https://airflow.apache.org/
