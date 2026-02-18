# Quick Reference Guide

## Common Commands

### Virtual Environment

```bash
# Create virtual environment
python -m venv venv

# Activate (Linux/Mac)
source venv/bin/activate

# Activate (Windows)
venv\Scripts\activate

# Deactivate
deactivate

# Install dependencies
pip install -r requirements.txt
```

### Running Examples

```bash
# Basic ETL
cd examples/basic_etl
python simple_etl.py

# Data Transformations
cd examples/data_transformation
python transformation_examples.py
```

### Airflow Commands

```bash
# Set Airflow home
export AIRFLOW_HOME=~/airflow

# Initialize database
airflow db init

# Create admin user
airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com

# Start webserver
airflow webserver --port 8080

# Start scheduler (in separate terminal)
airflow scheduler

# List DAGs
airflow dags list

# Test a task
airflow tasks test <dag_id> <task_id> <execution_date>

# Trigger a DAG
airflow dags trigger <dag_id>
```

### Git Commands

```bash
# Check status
git status

# Add files
git add .

# Commit
git commit -m "Your message"

# Push
git push

# Pull latest changes
git pull
```

## Pandas Cheat Sheet

### Reading Data

```python
# CSV
df = pd.read_csv('file.csv')

# Excel
df = pd.read_excel('file.xlsx')

# JSON
df = pd.read_json('file.json')

# SQL
df = pd.read_sql(query, connection)
```

### Data Inspection

```python
# First/last rows
df.head()
df.tail()

# Shape
df.shape

# Info
df.info()

# Statistics
df.describe()

# Columns
df.columns

# Data types
df.dtypes

# Null counts
df.isnull().sum()
```

### Data Cleaning

```python
# Remove duplicates
df.drop_duplicates()

# Drop null rows
df.dropna()

# Fill null values
df.fillna(value)

# Replace values
df.replace(old, new)

# Rename columns
df.rename(columns={'old': 'new'})

# Change data type
df['column'].astype(int)

# Convert to datetime
pd.to_datetime(df['column'])
```

### Data Selection

```python
# Select column
df['column']
df[['col1', 'col2']]

# Filter rows
df[df['column'] > 5]
df[df['column'].isin(['A', 'B'])]

# Select by position
df.iloc[0:5]

# Select by label
df.loc[df['column'] > 5, ['col1', 'col2']]
```

### Data Transformation

```python
# Apply function
df['new'] = df['column'].apply(lambda x: x * 2)

# Group by
df.groupby('column').sum()
df.groupby('column').agg({'col1': 'sum', 'col2': 'mean'})

# Sort
df.sort_values('column', ascending=False)

# Merge
pd.merge(df1, df2, on='key')

# Concatenate
pd.concat([df1, df2])

# Pivot
df.pivot(index='row', columns='col', values='val')
```

### Writing Data

```python
# CSV
df.to_csv('file.csv', index=False)

# Excel
df.to_excel('file.xlsx', index=False)

# JSON
df.to_json('file.json')

# SQL
df.to_sql('table', connection, if_exists='append')
```

## Common ETL Patterns

### Extract-Transform-Load

```python
def etl_pipeline():
    # Extract
    df = pd.read_csv('source.csv')
    
    # Transform
    df = df.drop_duplicates()
    df['new_col'] = df['col1'] * df['col2']
    
    # Load
    df.to_csv('output.csv', index=False)
```

### Incremental Processing

```python
def incremental_etl(last_processed_id):
    # Extract only new records
    df = pd.read_sql(f"SELECT * FROM table WHERE id > {last_processed_id}", conn)
    
    # Transform
    df = transform(df)
    
    # Load
    df.to_sql('target', conn, if_exists='append')
    
    return df['id'].max()
```

### Error Handling

```python
def safe_etl():
    try:
        # Extract
        df = extract()
        
        # Transform with validation
        df = transform(df)
        assert len(df) > 0, "No data to process"
        
        # Load
        load(df)
        
    except Exception as e:
        logger.error(f"ETL failed: {e}")
        raise
```

## Data Quality Checks

```python
def validate_data(df):
    """Common data quality checks"""
    
    # Check for nulls
    assert df['id'].notnull().all(), "Null IDs found"
    
    # Check data types
    assert df['amount'].dtype == float, "Invalid amount type"
    
    # Check ranges
    assert df['amount'].min() >= 0, "Negative amounts found"
    
    # Check uniqueness
    assert len(df['id'].unique()) == len(df), "Duplicate IDs found"
    
    # Check record count
    assert len(df) > 0, "Empty dataset"
    
    return True
```

## Useful SQL Snippets

```sql
-- Select with filter
SELECT * FROM table WHERE column > value;

-- Aggregation
SELECT region, SUM(sales) as total_sales
FROM sales
GROUP BY region
ORDER BY total_sales DESC;

-- Join
SELECT a.*, b.category
FROM orders a
JOIN products b ON a.product_id = b.id;

-- Window function
SELECT 
    date,
    sales,
    SUM(sales) OVER (ORDER BY date) as running_total
FROM daily_sales;
```

## Debugging Tips

### Print Intermediate Results

```python
print(f"Shape: {df.shape}")
print(f"Columns: {df.columns.tolist()}")
print(df.head())
print(df.dtypes)
```

### Use Try-Except with Details

```python
try:
    result = risky_operation()
except Exception as e:
    print(f"Error: {type(e).__name__}")
    print(f"Message: {str(e)}")
    import traceback
    traceback.print_exc()
```

### Check Data Quality at Each Step

```python
# After each transformation
print(f"Records: {len(df)}")
print(f"Nulls: {df.isnull().sum().sum()}")
print(f"Duplicates: {df.duplicated().sum()}")
```

## Performance Tips

### Use Efficient Data Types

```python
# Instead of object, use category for categorical data
df['category'] = df['category'].astype('category')

# Use appropriate numeric types
df['int_col'] = df['int_col'].astype('int32')  # instead of int64
```

### Process in Chunks

```python
# For large files
for chunk in pd.read_csv('large_file.csv', chunksize=10000):
    process(chunk)
```

### Use Vectorized Operations

```python
# Good (vectorized)
df['total'] = df['quantity'] * df['price']

# Bad (loop)
for i, row in df.iterrows():
    df.at[i, 'total'] = row['quantity'] * row['price']
```

## Common Errors and Solutions

### KeyError: 'column'
**Cause:** Column doesn't exist  
**Solution:** Check `df.columns` to see available columns

### ValueError: could not convert string to float
**Cause:** Non-numeric data in numeric column  
**Solution:** Clean data first: `df['col'] = pd.to_numeric(df['col'], errors='coerce')`

### MemoryError
**Cause:** Dataset too large  
**Solution:** Process in chunks or use more efficient data types

### ModuleNotFoundError
**Cause:** Package not installed  
**Solution:** `pip install package_name`

---

**Pro Tip:** Keep this guide handy while working on exercises and projects!
