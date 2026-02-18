# ETL Processes

## What is ETL?

**ETL** stands for Extract, Transform, Load. It's a fundamental data integration pattern used to move and process data from multiple sources into a target system.

## The Three Stages

### 1. Extract

The process of reading data from source systems.

#### Common Sources:
- **Relational Databases**: MySQL, PostgreSQL, Oracle
- **NoSQL Databases**: MongoDB, Cassandra
- **APIs**: REST APIs, SOAP services
- **Files**: CSV, JSON, XML, Excel
- **Cloud Storage**: S3, Azure Blob Storage
- **Streaming**: Kafka, RabbitMQ

#### Extraction Methods:
- **Full Extraction**: Pull all data every time
- **Incremental Extraction**: Only pull new/changed data
- **CDC (Change Data Capture)**: Track changes at database level

#### Example: Extracting from CSV
```python
import pandas as pd

# Extract data from CSV file
df = pd.read_csv('source_data.csv')
print(f"Extracted {len(df)} rows")
```

### 2. Transform

The process of converting data into the desired format and structure.

#### Common Transformations:

**Data Cleaning:**
- Remove duplicates
- Handle missing values
- Fix data types
- Standardize formats

**Data Validation:**
- Check for valid ranges
- Verify required fields
- Ensure referential integrity

**Data Enrichment:**
- Add calculated fields
- Lookup additional information
- Aggregate data

**Data Structuring:**
- Normalize/denormalize
- Pivot/unpivot
- Split/merge columns

#### Example: Basic Transformations
```python
import pandas as pd

# Clean data
df = df.drop_duplicates()
df = df.dropna(subset=['id'])

# Transform data
df['full_name'] = df['first_name'] + ' ' + df['last_name']
df['date'] = pd.to_datetime(df['date_string'])
df['amount'] = df['amount'].astype(float)

# Filter data
df = df[df['status'] == 'active']
```

### 3. Load

The process of writing transformed data to the destination.

#### Loading Strategies:

**Full Load:**
- Replace all existing data
- Simple but can be slow
- Good for small datasets

**Incremental Load:**
- Add only new records
- More efficient
- Requires tracking what's new

**Upsert (Update + Insert):**
- Update existing records
- Insert new records
- Most flexible but complex

**Append:**
- Only add new records
- No updates or deletes
- Good for log/event data

#### Example: Loading to Database
```python
from sqlalchemy import create_engine

# Create database connection
engine = create_engine('postgresql://user:pass@localhost/db')

# Load data
df.to_sql('target_table', engine, if_exists='append', index=False)
print(f"Loaded {len(df)} rows")
```

## Complete ETL Example

```python
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

def etl_process():
    """Complete ETL pipeline example"""
    
    # EXTRACT
    print("Starting extraction...")
    source_df = pd.read_csv('sales_data.csv')
    print(f"Extracted {len(source_df)} records")
    
    # TRANSFORM
    print("Starting transformation...")
    
    # Clean: Remove duplicates
    source_df = source_df.drop_duplicates(subset=['transaction_id'])
    
    # Clean: Handle missing values
    source_df['customer_name'] = source_df['customer_name'].fillna('Unknown')
    
    # Transform: Calculate total
    source_df['total'] = source_df['quantity'] * source_df['price']
    
    # Transform: Add timestamp
    source_df['processed_at'] = datetime.now()
    
    # Transform: Data type conversions
    source_df['transaction_date'] = pd.to_datetime(source_df['transaction_date'])
    
    # Filter: Only completed transactions
    transformed_df = source_df[source_df['status'] == 'completed']
    
    print(f"Transformed to {len(transformed_df)} records")
    
    # LOAD
    print("Starting load...")
    engine = create_engine('postgresql://user:pass@localhost/warehouse')
    transformed_df.to_sql('sales_fact', engine, if_exists='append', index=False)
    
    print("ETL process completed successfully!")
    
    return len(transformed_df)

if __name__ == "__main__":
    records_processed = etl_process()
    print(f"Total records processed: {records_processed}")
```

## Error Handling in ETL

```python
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def robust_etl_process():
    """ETL process with error handling"""
    
    start_time = datetime.now()
    stats = {'extracted': 0, 'transformed': 0, 'loaded': 0, 'errors': 0}
    
    try:
        # Extract with error handling
        try:
            source_df = pd.read_csv('sales_data.csv')
            stats['extracted'] = len(source_df)
            logger.info(f"Extracted {stats['extracted']} records")
        except FileNotFoundError:
            logger.error("Source file not found")
            raise
        except Exception as e:
            logger.error(f"Extraction failed: {str(e)}")
            raise
        
        # Transform with validation
        try:
            # Apply transformations
            transformed_df = source_df.copy()
            
            # Validate required columns
            required_cols = ['transaction_id', 'amount', 'date']
            missing_cols = set(required_cols) - set(transformed_df.columns)
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")
            
            # Data quality checks
            null_counts = transformed_df[required_cols].isnull().sum()
            if null_counts.any():
                logger.warning(f"Found null values: {null_counts.to_dict()}")
            
            stats['transformed'] = len(transformed_df)
            logger.info(f"Transformed {stats['transformed']} records")
            
        except Exception as e:
            logger.error(f"Transformation failed: {str(e)}")
            raise
        
        # Load with retry logic
        max_retries = 3
        for attempt in range(max_retries):
            try:
                engine = create_engine('postgresql://user:pass@localhost/db')
                transformed_df.to_sql('target', engine, if_exists='append', index=False)
                stats['loaded'] = len(transformed_df)
                logger.info(f"Loaded {stats['loaded']} records")
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Load attempt {attempt + 1} failed, retrying...")
                else:
                    logger.error(f"Load failed after {max_retries} attempts: {str(e)}")
                    raise
        
    except Exception as e:
        stats['errors'] = 1
        logger.error(f"ETL process failed: {str(e)}")
        raise
    
    finally:
        # Log summary
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"ETL Summary: {stats}, Duration: {duration}s")
    
    return stats

if __name__ == "__main__":
    try:
        result = robust_etl_process()
        print(f"ETL completed: {result}")
    except Exception as e:
        print(f"ETL failed: {str(e)}")
```

## Best Practices

1. **Make it Idempotent**: Running the same ETL multiple times should produce the same result
2. **Use Incremental Processing**: Only process new/changed data when possible
3. **Implement Data Quality Checks**: Validate data at each stage
4. **Log Everything**: Keep detailed logs for debugging and auditing
5. **Handle Errors Gracefully**: Implement retry logic and proper error handling
6. **Optimize for Performance**: Use appropriate data types, batch processing
7. **Document Transformations**: Keep clear documentation of all data transformations
8. **Test Thoroughly**: Test with edge cases and various data scenarios

## Next Steps

- Review the ETL examples in `/examples/basic_etl/`
- Try the ETL exercises in `/exercises/`
- Learn about workflow orchestration with Airflow
