"""
Data Transformation Examples

This module demonstrates common data transformation techniques:
1. Data cleaning
2. Data type conversions
3. Feature engineering
4. Aggregations
5. Pivoting and reshaping
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def example_data_cleaning():
    """
    Demonstrate data cleaning techniques
    """
    print("=" * 60)
    print("Example 1: Data Cleaning")
    print("=" * 60)
    
    # Create sample data with quality issues
    data = {
        'customer_id': [1, 2, 3, 4, 5, 5, 6],  # Has duplicate
        'name': ['Alice', 'Bob', None, 'David', 'Eve', 'Eve', 'Frank'],  # Has null
        'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com', 
                  'DAVID@EXAMPLE.COM', 'eve@example.com', 'eve@example.com', 'frank@example.com'],
        'age': [25, 30, None, 35, 28, 28, -5],  # Has null and invalid value
        'balance': [1000.50, 2500.00, 1500.00, 3000.00, None, None, 500.00],  # Has null
    }
    
    df = pd.DataFrame(data)
    
    print("\nOriginal Data:")
    print(df)
    print(f"\nShape: {df.shape}")
    print(f"\nNull values:\n{df.isnull().sum()}")
    
    # Cleaning operations
    
    # 1. Remove duplicates
    df_clean = df.drop_duplicates(subset=['customer_id'], keep='first')
    print(f"\nAfter removing duplicates: {len(df_clean)} rows")
    
    # 2. Handle missing values
    df_clean['name'] = df_clean['name'].fillna('Unknown')
    df_clean['age'] = df_clean['age'].fillna(df_clean['age'].median())
    df_clean['balance'] = df_clean['balance'].fillna(0)
    
    # 3. Fix data quality issues
    df_clean['email'] = df_clean['email'].str.lower()  # Standardize email case
    df_clean['age'] = df_clean['age'].apply(lambda x: abs(x) if x < 0 else x)  # Fix negative ages
    
    # 4. Validate data
    assert df_clean['age'].min() >= 0, "Negative ages still exist"
    assert df_clean['balance'].min() >= 0, "Negative balances found"
    
    print("\nCleaned Data:")
    print(df_clean)
    
    return df_clean

def example_type_conversions():
    """
    Demonstrate data type conversions
    """
    print("\n" + "=" * 60)
    print("Example 2: Data Type Conversions")
    print("=" * 60)
    
    # Sample data with mixed types
    data = {
        'date_string': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'amount_string': ['100.50', '200.75', '150.25'],
        'category_number': [1, 2, 1],
        'is_active': ['true', 'false', 'true'],
    }
    
    df = pd.DataFrame(data)
    
    print("\nOriginal Data Types:")
    print(df.dtypes)
    print("\nOriginal Data:")
    print(df)
    
    # Type conversions
    df['date'] = pd.to_datetime(df['date_string'])
    df['amount'] = df['amount_string'].astype(float)
    df['category'] = df['category_number'].astype('category')
    df['is_active_bool'] = df['is_active'].map({'true': True, 'false': False})
    
    print("\nConverted Data Types:")
    print(df.dtypes)
    print("\nConverted Data:")
    print(df)
    
    return df

def example_feature_engineering():
    """
    Demonstrate feature engineering techniques
    """
    print("\n" + "=" * 60)
    print("Example 3: Feature Engineering")
    print("=" * 60)
    
    # Sample sales data
    data = {
        'order_id': range(1, 11),
        'customer_id': [1, 2, 1, 3, 2, 1, 4, 3, 2, 1],
        'order_date': pd.date_range(start='2024-01-01', periods=10, freq='D'),
        'amount': [100, 150, 200, 80, 120, 90, 250, 180, 110, 300],
        'quantity': [2, 3, 4, 1, 2, 1, 5, 3, 2, 6],
    }
    
    df = pd.DataFrame(data)
    
    print("\nOriginal Data:")
    print(df)
    
    # Feature engineering
    
    # 1. Calculate unit price
    df['unit_price'] = df['amount'] / df['quantity']
    
    # 2. Extract date components
    df['day_of_week'] = df['order_date'].dt.day_name()
    df['month'] = df['order_date'].dt.month
    df['is_weekend'] = df['order_date'].dt.dayofweek >= 5
    
    # 3. Create categorical features
    df['amount_category'] = pd.cut(
        df['amount'], 
        bins=[0, 100, 200, float('inf')], 
        labels=['low', 'medium', 'high']
    )
    
    # 4. Customer aggregations (using groupby)
    customer_stats = df.groupby('customer_id').agg({
        'order_id': 'count',
        'amount': 'sum'
    }).rename(columns={'order_id': 'total_orders', 'amount': 'total_spent'})
    
    df = df.merge(customer_stats, on='customer_id', how='left')
    
    # 5. Running totals
    df = df.sort_values('order_date')
    df['cumulative_revenue'] = df['amount'].cumsum()
    
    print("\nData with Engineered Features:")
    print(df)
    
    return df

def example_aggregations():
    """
    Demonstrate aggregation techniques
    """
    print("\n" + "=" * 60)
    print("Example 4: Data Aggregations")
    print("=" * 60)
    
    # Sample transaction data
    data = {
        'date': pd.date_range(start='2024-01-01', periods=30, freq='D'),
        'region': ['North', 'South', 'East', 'West'] * 7 + ['North', 'South'],
        'product': ['A', 'B'] * 15,
        'sales': np.random.randint(100, 1000, 30),
        'units': np.random.randint(10, 50, 30),
    }
    
    df = pd.DataFrame(data)
    
    print("\nOriginal Data (first 10 rows):")
    print(df.head(10))
    
    # Various aggregations
    
    # 1. Simple aggregation
    print("\nTotal Sales by Region:")
    region_sales = df.groupby('region')['sales'].sum().sort_values(ascending=False)
    print(region_sales)
    
    # 2. Multiple aggregations
    print("\nSales Statistics by Product:")
    product_stats = df.groupby('product').agg({
        'sales': ['sum', 'mean', 'count'],
        'units': ['sum', 'mean']
    })
    print(product_stats)
    
    # 3. Custom aggregations
    print("\nCustom Aggregations by Region:")
    custom_agg = df.groupby('region').agg({
        'sales': ['sum', 'mean', lambda x: x.max() - x.min()],
        'units': 'sum'
    })
    custom_agg.columns = ['total_sales', 'avg_sales', 'sales_range', 'total_units']
    print(custom_agg)
    
    return df

def example_pivoting():
    """
    Demonstrate pivoting and reshaping
    """
    print("\n" + "=" * 60)
    print("Example 5: Pivoting and Reshaping")
    print("=" * 60)
    
    # Sample data
    data = {
        'date': ['2024-01-01', '2024-01-01', '2024-01-02', '2024-01-02'],
        'region': ['North', 'South', 'North', 'South'],
        'sales': [1000, 1500, 1100, 1600],
    }
    
    df = pd.DataFrame(data)
    
    print("\nOriginal Data:")
    print(df)
    
    # Pivot table
    pivot_df = df.pivot(index='date', columns='region', values='sales')
    
    print("\nPivoted Data (Region as columns):")
    print(pivot_df)
    
    # Pivot with aggregation
    data_multi = pd.DataFrame({
        'date': ['2024-01-01', '2024-01-01', '2024-01-01', '2024-01-02', '2024-01-02'],
        'region': ['North', 'South', 'North', 'North', 'South'],
        'product': ['A', 'A', 'B', 'A', 'B'],
        'sales': [1000, 1500, 800, 1100, 1200],
    })
    
    print("\nData with Multiple Products:")
    print(data_multi)
    
    pivot_table = pd.pivot_table(
        data_multi, 
        values='sales', 
        index='date', 
        columns=['region', 'product'],
        aggfunc='sum',
        fill_value=0
    )
    
    print("\nPivot Table (Multi-level columns):")
    print(pivot_table)
    
    return pivot_df

if __name__ == "__main__":
    # Run all examples
    example_data_cleaning()
    example_type_conversions()
    example_feature_engineering()
    example_aggregations()
    example_pivoting()
    
    print("\n" + "=" * 60)
    print("All transformation examples completed!")
    print("=" * 60)
