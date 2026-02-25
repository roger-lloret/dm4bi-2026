import logging
import time
import tracemalloc
import yaml
import pandas as pd
import numpy as np
from datetime import date
from pathlib import Path

from utils import get_db_engine, read_from_database, write_to_database, load_sql_file, classify_invoices

# --- Configuration & Constants ---
# Use pathlib for cross-platform compatibility and robust path handling
BASE_DIR = Path(__file__).parent if "__file__" in locals() else Path.cwd()
CREDS_PATH = Path("C:/Users/roger.lloret/Documents/creds/creds_dmbi.yml")
SQL_DIR = BASE_DIR / "sql"
TABLE_NAME = 'gen_kpi_ft'

# Configure logging to replace simple print statements
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# --- Main Execution Flow ---

def main():
    # 1. Load Credentials
    if not CREDS_PATH.exists():
        logger.error(f"Credentials file not found at {CREDS_PATH}")
        return

    with open(CREDS_PATH, "r") as file:
        creds = yaml.safe_load(file)

    # 2. Initialize Database Engine
    # Create the engine once and reuse it, rather than recreating it inside every function
    db_engine = get_db_engine(creds['data_warehouse'])

    # 3. Load SQL Queries
    logger.info("Loading SQL queries...")
    invoices_sql = load_sql_file(SQL_DIR / "invoices_main.sql")
    contracts_sql = load_sql_file(SQL_DIR / "contracts_main.sql")

    # 4. Extract Data
    logger.info("Extracting data from database...")
    invoices_df = read_from_database(db_engine, invoices_sql)
    contracts_df = read_from_database(db_engine, contracts_sql)

    # 5. Transform Data
    logger.info("Transforming data...")
    # Deduplicate contracts
    contracts_df = contracts_df.drop_duplicates()
    # Calculate categories (Vectorized)
    invoices_df = classify_invoices(invoices_df, 'total_import_euros')
    # Ensure join keys are numeric
    invoices_df['contract_id'] = pd.to_numeric(invoices_df['contract_id'], errors='coerce')
    contracts_df['contract_id'] = pd.to_numeric(contracts_df['contract_id'], errors='coerce')
    # Merge
    merged_df = invoices_df.merge(contracts_df, on='contract_id', how='left')

    # 6. Create Indicators (KPIs)
    
    # KPI 1: Amount by Category & Client Type
    kpi_category_df = merged_df.groupby(['category', 'client_type_description'])['total_import_euros'].sum().reset_index()
    kpi_category_df['kpi_name'] = (
        'Total amount in euros of the customers with invoices ' + 
        kpi_category_df['category'] + ' euros and ' + 
        kpi_category_df['client_type_description']
    )
    kpi_category_df = kpi_category_df.rename(columns={'total_import_euros': 'kpi_value'})[['kpi_name', 'kpi_value']]

    # KPI 2: Document Type Count
    kpi_doctype_df = merged_df.groupby(['document_type_description'])['total_import_euros'].count().reset_index()
    kpi_doctype_df['kpi_name'] = 'Number of invoices of invoice type ' + kpi_doctype_df['document_type_description']
    kpi_doctype_df = kpi_doctype_df.rename(columns={'total_import_euros': 'kpi_value'})[['kpi_name', 'kpi_value']]

    # 7. Aggregate and Load
    logger.info("Loading results to database...")
    main_df = pd.concat([kpi_category_df, kpi_doctype_df])
    main_df['kpi_date'] = date.today()
    
    # Ensure column order matches target table
    output_df = main_df[['kpi_date', 'kpi_name', 'kpi_value']]
    
    write_to_database(db_engine, output_df, TABLE_NAME)
    logger.info("ETL Process completed successfully.")

if __name__ == "__main__":
    tracemalloc.start()
    start_time = time.time()
    main()
    elapsed = time.time() - start_time
    _, peak_memory = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    print(f"Pandas pipeline executed in {elapsed:.2f} seconds.")
    print(f"Peak RAM usage: {peak_memory / (1024 * 1024):.2f} MB")