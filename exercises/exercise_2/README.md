# Exercise 2: Data Aggregation and Reporting

## Objective
Build a pipeline that aggregates sales data and generates summary reports.

## Background
You have daily sales data across multiple regions and products. Your task is to create aggregated views for business analysis.

## Dataset

`sales_data.csv` contains:
- date: Transaction date
- region: Sales region (North, South, East, West)
- product_category: Product category (Electronics, Clothing, Food, Books)
- product_name: Specific product
- quantity: Units sold
- unit_price: Price per unit
- customer_id: Unique customer identifier

## Requirements

### Part 1: Data Cleaning
1. Remove any duplicate rows
2. Handle missing values appropriately
3. Ensure all numeric fields are valid (no negatives for quantity/price)
4. Convert date to datetime format

### Part 2: Create Calculated Fields
1. `total_sales` = quantity * unit_price
2. `month` = extracted from date
3. `day_of_week` = extracted from date
4. `is_weekend` = boolean indicating if sale occurred on weekend

### Part 3: Generate Aggregated Reports

Create the following output files:

**1. `sales_by_region.csv`**
- Columns: region, total_sales, total_quantity, avg_sale_amount, transaction_count
- Sorted by total_sales descending

**2. `sales_by_category.csv`**
- Columns: product_category, total_sales, total_quantity, unique_products, avg_price
- Sorted by total_sales descending

**3. `daily_sales_summary.csv`**
- Columns: date, total_sales, transaction_count, unique_customers
- Sorted by date

**4. `top_products.csv`**
- Top 10 products by total sales
- Columns: product_name, total_sales, quantity_sold, times_purchased

### Part 4: Insights Report

Generate a text file `insights.txt` with:
- Total revenue across all regions
- Best performing region
- Best selling product category
- Day of week with highest sales
- Average transaction value

## Starter Code

```python
import pandas as pd

def load_and_clean_data(file_path):
    """Load and clean the sales data"""
    # TODO: Implement
    pass

def create_calculated_fields(df):
    """Add calculated fields to dataframe"""
    # TODO: Implement
    pass

def generate_region_report(df):
    """Generate sales by region report"""
    # TODO: Implement
    pass

def generate_category_report(df):
    """Generate sales by category report"""
    # TODO: Implement
    pass

def generate_daily_summary(df):
    """Generate daily sales summary"""
    # TODO: Implement
    pass

def generate_top_products(df, top_n=10):
    """Generate top N products report"""
    # TODO: Implement
    pass

def generate_insights(df):
    """Generate insights text report"""
    # TODO: Implement
    pass

def run_reporting_pipeline(input_file):
    """Run the complete reporting pipeline"""
    # TODO: Implement
    pass

if __name__ == "__main__":
    run_reporting_pipeline('sales_data.csv')
```

## Tips

1. Use `groupby()` for aggregations
2. Use `agg()` to apply multiple aggregation functions
3. Use `sort_values()` to sort results
4. Use `to_csv()` to write output files
5. Use `dt` accessor for datetime operations (e.g., `df['date'].dt.month`)

## Validation Criteria

Your solution should:
- Handle all data quality issues
- Generate all required reports correctly
- Calculate aggregations accurately
- Sort outputs as specified
- Create meaningful insights

## Bonus Challenges

1. Add visualization: Create a bar chart of sales by region
2. Identify seasonal trends in the data
3. Calculate month-over-month growth rates
4. Find correlation between product categories
5. Add a customer segmentation report (high/medium/low value customers)
