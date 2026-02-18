# Examples Directory

This directory contains practical examples of data pipelines and related concepts.

## Available Examples

### 1. Basic ETL (`basic_etl/`)

**simple_etl.py** - A beginner-friendly ETL pipeline that demonstrates:
- Extracting data from CSV files
- Transforming data (cleaning, deduplication)
- Loading data to output files
- Error handling and logging

**How to run:**
```bash
cd basic_etl
python simple_etl.py
```

### 2. Data Transformation (`data_transformation/`)

**transformation_examples.py** - Comprehensive examples covering:
- Data cleaning techniques
- Type conversions
- Feature engineering
- Aggregations and grouping
- Pivoting and reshaping data

**How to run:**
```bash
cd data_transformation
python transformation_examples.py
```

### 3. Airflow DAGs (`airflow_dags/`)

**daily_sales_etl_dag.py** - Production-ready Airflow DAG showing:
- Complete ETL workflow
- Task dependencies
- XCom for inter-task communication
- Data quality checks
- Error handling and retries

**How to use:**
1. Copy to Airflow DAGs folder: `cp daily_sales_etl_dag.py ~/airflow/dags/`
2. Start Airflow webserver and scheduler
3. Access the UI at `http://localhost:8080`
4. Trigger the DAG manually or wait for scheduled run

## Learning Approach

1. **Read the Code**: Each example is heavily commented to explain what's happening
2. **Run the Example**: Execute the code to see it in action
3. **Modify and Experiment**: Try changing parameters, adding features, or fixing issues
4. **Compare with Documentation**: Reference the `/docs` folder for theoretical background

## Key Concepts Demonstrated

### ETL Pattern
- **Extract**: Reading data from various sources
- **Transform**: Cleaning, validating, and enriching data
- **Load**: Writing processed data to destinations

### Data Quality
- Duplicate detection and removal
- Missing value handling
- Data type validation
- Sanity checks on values

### Error Handling
- Try-except blocks
- Logging for debugging
- Graceful degradation
- Retry mechanisms

### Best Practices
- Modular, reusable functions
- Clear documentation
- Logging at each stage
- Idempotent operations
- Configuration management

## Next Steps

After exploring these examples:
1. Complete the exercises in `/exercises`
2. Try creating your own variations
3. Combine concepts from multiple examples
4. Build a complete pipeline for real data

## Contributing

Feel free to add your own examples! Guidelines:
- Include clear comments
- Add a brief description in this README
- Ensure code runs without errors
- Follow existing code style
