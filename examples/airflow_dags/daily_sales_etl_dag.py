"""
Airflow DAG Example: Daily Sales ETL Pipeline

This DAG demonstrates a complete ETL workflow using Apache Airflow:
1. Extract sales data from a source
2. Transform and clean the data
3. Load to a data warehouse
4. Perform data quality checks

Schedule: Runs daily at 2 AM
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import logging

# Configure logging
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['data-team@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

# Create the DAG
dag = DAG(
    'daily_sales_etl',
    default_args=default_args,
    description='Daily ETL pipeline for sales data',
    schedule_interval='0 2 * * *',  # Run at 2 AM daily
    catchup=False,  # Don't backfill
    tags=['sales', 'etl', 'daily'],
)

def extract_sales_data(**context):
    """
    Extract sales data from source system
    
    In production, this would connect to:
    - Database (using SQLAlchemy)
    - API (using requests)
    - File storage (S3, Azure Blob)
    """
    logger.info("Starting data extraction...")
    
    # Simulate data extraction (in production, fetch from actual source)
    execution_date = context['ds']  # Airflow execution date
    
    # Sample data
    data = {
        'transaction_id': range(1, 101),
        'customer_id': [f'CUST{i%20:03d}' for i in range(1, 101)],
        'product_id': [f'PROD{i%10:03d}' for i in range(1, 101)],
        'quantity': [i % 5 + 1 for i in range(1, 101)],
        'unit_price': [10.0 + (i % 10) * 5 for i in range(1, 101)],
        'transaction_date': execution_date,
        'status': ['completed' if i % 10 != 0 else 'pending' for i in range(1, 101)],
    }
    
    df = pd.DataFrame(data)
    
    # Push to XCom for next task
    records_json = df.to_json(orient='records')
    context['ti'].xcom_push(key='extracted_data', value=records_json)
    
    logger.info(f"Extracted {len(df)} records for date {execution_date}")
    
    return len(df)

def transform_sales_data(**context):
    """
    Transform and clean the extracted data
    """
    logger.info("Starting data transformation...")
    
    # Pull data from previous task
    records_json = context['ti'].xcom_pull(key='extracted_data', task_ids='extract_data')
    df = pd.read_json(records_json, orient='records')
    
    logger.info(f"Received {len(df)} records for transformation")
    
    # Data Cleaning
    # 1. Remove duplicates
    initial_count = len(df)
    df = df.drop_duplicates(subset=['transaction_id'])
    logger.info(f"Removed {initial_count - len(df)} duplicate records")
    
    # 2. Filter only completed transactions
    df = df[df['status'] == 'completed']
    logger.info(f"Filtered to {len(df)} completed transactions")
    
    # Data Transformation
    # 1. Calculate total amount
    df['total_amount'] = df['quantity'] * df['unit_price']
    
    # 2. Add derived columns
    df['revenue_category'] = df['total_amount'].apply(
        lambda x: 'high' if x > 100 else 'medium' if x > 50 else 'low'
    )
    
    # 3. Add processing metadata
    df['processed_at'] = datetime.now()
    df['pipeline_version'] = '1.0'
    
    # Data Validation
    assert len(df) > 0, "No records after transformation"
    assert df['total_amount'].min() >= 0, "Negative amounts detected"
    assert df['quantity'].min() > 0, "Invalid quantities detected"
    
    # Push transformed data
    transformed_json = df.to_json(orient='records')
    context['ti'].xcom_push(key='transformed_data', value=transformed_json)
    
    logger.info(f"Transformation complete: {len(df)} records ready for loading")
    
    return len(df)

def load_sales_data(**context):
    """
    Load transformed data to data warehouse
    
    In production, this would load to:
    - PostgreSQL, MySQL, etc.
    - Data Warehouse (Snowflake, BigQuery, Redshift)
    - Data Lake (S3 + Parquet)
    """
    logger.info("Starting data load...")
    
    # Pull transformed data
    transformed_json = context['ti'].xcom_pull(key='transformed_data', task_ids='transform_data')
    df = pd.read_json(transformed_json, orient='records')
    
    # In production, use:
    # from sqlalchemy import create_engine
    # engine = create_engine('postgresql://user:pass@host/db')
    # df.to_sql('sales_fact', engine, if_exists='append', index=False)
    
    # For this example, save to CSV
    output_file = f'/tmp/sales_data_{context["ds"]}.csv'
    df.to_csv(output_file, index=False)
    
    logger.info(f"Loaded {len(df)} records to {output_file}")
    
    return len(df)

def validate_data_quality(**context):
    """
    Perform data quality checks on loaded data
    """
    logger.info("Starting data quality validation...")
    
    # Pull loaded data info
    transformed_json = context['ti'].xcom_pull(key='transformed_data', task_ids='transform_data')
    df = pd.read_json(transformed_json, orient='records')
    
    # Quality checks
    checks_passed = []
    checks_failed = []
    
    # Check 1: No null values in critical columns
    critical_columns = ['transaction_id', 'customer_id', 'total_amount']
    null_counts = df[critical_columns].isnull().sum()
    if null_counts.sum() == 0:
        checks_passed.append("No null values in critical columns")
    else:
        checks_failed.append(f"Null values found: {null_counts.to_dict()}")
    
    # Check 2: All amounts are positive
    if df['total_amount'].min() >= 0:
        checks_passed.append("All amounts are positive")
    else:
        checks_failed.append("Negative amounts detected")
    
    # Check 3: Record count threshold
    if len(df) >= 10:  # Minimum expected records
        checks_passed.append(f"Record count check passed: {len(df)} records")
    else:
        checks_failed.append(f"Record count too low: {len(df)} records")
    
    # Log results
    logger.info(f"Quality checks passed: {len(checks_passed)}")
    logger.info(f"Quality checks failed: {len(checks_failed)}")
    
    for check in checks_passed:
        logger.info(f"✓ {check}")
    
    for check in checks_failed:
        logger.error(f"✗ {check}")
    
    # Raise error if any checks failed
    if checks_failed:
        raise ValueError(f"Data quality checks failed: {checks_failed}")
    
    return {"passed": len(checks_passed), "failed": len(checks_failed)}

# Define tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_sales_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_sales_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_sales_data,
    dag=dag,
)

quality_check_task = PythonOperator(
    task_id='data_quality_check',
    python_callable=validate_data_quality,
    dag=dag,
)

# Optional: Send success notification
success_notification = BashOperator(
    task_id='send_success_notification',
    bash_command='echo "ETL pipeline completed successfully for {{ ds }}"',
    dag=dag,
)

# Define task dependencies (pipeline flow)
extract_task >> transform_task >> load_task >> quality_check_task >> success_notification

# Alternative syntax for dependencies:
# extract_task.set_downstream(transform_task)
# transform_task.set_downstream(load_task)
# load_task.set_downstream(quality_check_task)
# quality_check_task.set_downstream(success_notification)
