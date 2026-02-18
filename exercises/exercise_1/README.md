# Exercise 1: Build Your First ETL Pipeline

## Objective
Create a simple ETL pipeline that extracts data from a CSV file, transforms it, and loads it to another CSV file.

## Background
You work for an e-commerce company. The sales team exports daily transaction data, but it needs to be cleaned and processed before analysis.

## Requirements

### Extract
- Read the provided `raw_transactions.csv` file
- The file contains: transaction_id, customer_email, product_name, quantity, price, transaction_date

### Transform
Implement the following transformations:
1. Remove any duplicate transactions (based on transaction_id)
2. Filter out transactions with quantity <= 0
3. Convert customer_email to lowercase
4. Add a new column `total_amount` = quantity * price
5. Add a new column `transaction_month` extracted from transaction_date
6. Add a column `processed_date` with the current date

### Load
- Save the transformed data to `processed_transactions.csv`
- The output should include all original columns plus the new calculated columns

## Starter Code

```python
import pandas as pd
from datetime import datetime

def extract(file_path):
    """
    Extract data from CSV file
    
    Args:
        file_path: Path to the input CSV file
    
    Returns:
        DataFrame with extracted data
    """
    # TODO: Implement extraction logic
    pass

def transform(df):
    """
    Transform the extracted data
    
    Args:
        df: Input DataFrame
    
    Returns:
        Transformed DataFrame
    """
    # TODO: Implement transformation logic
    # Hint: Use df.drop_duplicates(), df[df['column'] > 0], etc.
    pass

def load(df, output_path):
    """
    Load data to CSV file
    
    Args:
        df: DataFrame to load
        output_path: Path to output CSV file
    """
    # TODO: Implement load logic
    pass

def run_pipeline(input_file, output_file):
    """
    Run the complete ETL pipeline
    """
    # TODO: Call extract, transform, and load functions
    pass

if __name__ == "__main__":
    run_pipeline('raw_transactions.csv', 'processed_transactions.csv')
```

## Sample Input Data

Create a file `raw_transactions.csv` with this content:
```csv
transaction_id,customer_email,product_name,quantity,price,transaction_date
1,ALICE@EXAMPLE.COM,Laptop,1,999.99,2024-01-15
2,bob@example.com,Mouse,2,25.50,2024-01-15
3,charlie@EXAMPLE.com,Keyboard,0,75.00,2024-01-16
4,alice@example.com,Monitor,1,299.99,2024-01-16
1,ALICE@EXAMPLE.COM,Laptop,1,999.99,2024-01-15
5,dave@example.com,Webcam,-1,89.99,2024-01-17
```

## Expected Output

Your `processed_transactions.csv` should have:
- 3 valid transactions (duplicates and invalid quantities removed)
- All emails in lowercase
- New columns: total_amount, transaction_month, processed_date

## Validation

Your solution should:
1. Remove the duplicate transaction (transaction_id = 1)
2. Remove transactions with quantity <= 0 (transaction_id = 3, 5)
3. Keep only valid transactions (transaction_id = 1, 2, 4)
4. Calculate correct total amounts
5. Extract month from transaction_date

## Bonus Challenges

1. Add error handling for missing files
2. Log the number of records at each stage
3. Add data quality checks (e.g., verify no negative prices)
4. Create a summary report showing:
   - Total records processed
   - Number of duplicates removed
   - Number of invalid records filtered
   - Total revenue (sum of all total_amount)

## Solution

Once you've completed the exercise, you can check your solution against the provided solution in `solution.py`.
