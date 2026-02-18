"""
Simple ETL Pipeline Example

This example demonstrates a basic ETL pipeline that:
1. Extracts data from a CSV file
2. Transforms the data (cleaning and calculations)
3. Loads the data to a new CSV file

This is a beginner-friendly example to understand ETL concepts.
"""

import pandas as pd
from datetime import datetime
import os

def extract(file_path):
    """
    Extract data from a CSV file
    
    Args:
        file_path: Path to the source CSV file
    
    Returns:
        DataFrame containing the extracted data
    """
    print(f"[EXTRACT] Reading data from {file_path}")
    
    try:
        df = pd.read_csv(file_path)
        print(f"[EXTRACT] Successfully extracted {len(df)} rows and {len(df.columns)} columns")
        return df
    except FileNotFoundError:
        print(f"[EXTRACT ERROR] File not found: {file_path}")
        raise
    except Exception as e:
        print(f"[EXTRACT ERROR] {str(e)}")
        raise

def transform(df):
    """
    Transform the extracted data
    
    Transformations include:
    - Remove duplicate rows
    - Handle missing values
    - Add calculated columns
    - Filter invalid data
    
    Args:
        df: Input DataFrame
    
    Returns:
        Transformed DataFrame
    """
    print(f"[TRANSFORM] Starting transformation on {len(df)} rows")
    
    # Create a copy to avoid modifying the original
    transformed_df = df.copy()
    
    # 1. Remove duplicates
    initial_count = len(transformed_df)
    transformed_df = transformed_df.drop_duplicates()
    duplicates_removed = initial_count - len(transformed_df)
    print(f"[TRANSFORM] Removed {duplicates_removed} duplicate rows")
    
    # 2. Handle missing values
    # Fill missing numeric values with 0
    numeric_columns = transformed_df.select_dtypes(include=['number']).columns
    for col in numeric_columns:
        missing_count = transformed_df[col].isnull().sum()
        if missing_count > 0:
            transformed_df[col] = transformed_df[col].fillna(0)
            print(f"[TRANSFORM] Filled {missing_count} missing values in {col}")
    
    # Fill missing string values with 'Unknown'
    string_columns = transformed_df.select_dtypes(include=['object']).columns
    for col in string_columns:
        missing_count = transformed_df[col].isnull().sum()
        if missing_count > 0:
            transformed_df[col] = transformed_df[col].fillna('Unknown')
            print(f"[TRANSFORM] Filled {missing_count} missing values in {col}")
    
    # 3. Add metadata columns
    transformed_df['processed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    transformed_df['data_quality_score'] = 100  # Placeholder for quality metric
    
    print(f"[TRANSFORM] Transformation complete. Output: {len(transformed_df)} rows")
    
    return transformed_df

def load(df, output_path):
    """
    Load transformed data to a CSV file
    
    Args:
        df: DataFrame to load
        output_path: Path to the output CSV file
    """
    print(f"[LOAD] Writing {len(df)} rows to {output_path}")
    
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Write to CSV
        df.to_csv(output_path, index=False)
        
        print(f"[LOAD] Successfully loaded data to {output_path}")
    except Exception as e:
        print(f"[LOAD ERROR] {str(e)}")
        raise

def run_etl_pipeline(source_file, target_file):
    """
    Execute the complete ETL pipeline
    
    Args:
        source_file: Path to source CSV file
        target_file: Path to target CSV file
    
    Returns:
        Number of rows processed
    """
    print("="*50)
    print("Starting ETL Pipeline")
    print("="*50)
    
    start_time = datetime.now()
    
    try:
        # Extract
        raw_data = extract(source_file)
        
        # Transform
        transformed_data = transform(raw_data)
        
        # Load
        load(transformed_data, target_file)
        
        # Calculate duration
        duration = (datetime.now() - start_time).total_seconds()
        
        print("="*50)
        print(f"ETL Pipeline Completed Successfully")
        print(f"Duration: {duration:.2f} seconds")
        print(f"Rows Processed: {len(transformed_data)}")
        print("="*50)
        
        return len(transformed_data)
        
    except Exception as e:
        print("="*50)
        print(f"ETL Pipeline Failed: {str(e)}")
        print("="*50)
        raise

if __name__ == "__main__":
    # Example usage
    source = "../../data/sample_sales.csv"
    target = "../../data/processed_sales.csv"
    
    # Check if source file exists
    if not os.path.exists(source):
        print(f"Source file not found: {source}")
        print("Creating sample data for demonstration...")
        
        # Create sample data
        sample_data = pd.DataFrame({
            'order_id': [1, 2, 3, 4, 5, 5],  # Note: duplicate row
            'customer_name': ['Alice', 'Bob', None, 'David', 'Eve', 'Eve'],
            'product': ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Laptop', 'Laptop'],
            'quantity': [1, 2, 1, 1, None, None],
            'price': [1000, 25, 75, 300, 1000, 1000]
        })
        
        os.makedirs(os.path.dirname(source), exist_ok=True)
        sample_data.to_csv(source, index=False)
        print(f"Sample data created at {source}")
    
    # Run the pipeline
    run_etl_pipeline(source, target)
