import time
import tracemalloc
import yaml
import polars as pl
from datetime import date
from pathlib import Path
from sqlalchemy import create_engine

from utils import get_db_engine, load_sql_file

# --- Configuration & Constants ---
BASE_DIR = Path(__file__).parent if "__file__" in locals() else Path.cwd()
CREDS_PATH = Path("C:/Users/roger.lloret/Documents/creds/creds_dmbi.yml")
SQL_DIR = BASE_DIR / "sql"
TABLE_NAME = "gen_kpi_ft"


# --- Polars Helper Functions ---

def read_from_database_pl(engine, query: str) -> pl.DataFrame:
    """Reads a SQL query into a Polars DataFrame via the SQLAlchemy engine."""
    # render_as_string with hide_password=False to include credentials in the URI
    uri = engine.url.render_as_string(hide_password=False)
    return pl.read_database_uri(query, uri=uri)


def write_to_database_pl(engine, df: pl.DataFrame, table_name: str, if_exists: str = "append"):
    """Writes a Polars DataFrame to MySQL by converting to pandas for to_sql."""
    with engine.begin() as connection:
        df.to_pandas().to_sql(table_name, con=connection, if_exists=if_exists, index=False)


def classify_invoices_pl(df: pl.DataFrame, column_name: str) -> pl.DataFrame:
    """Classifies numeric values into categories using Polars when/then/otherwise."""
    return df.with_columns(
        pl.when(pl.col(column_name) < 50)
        .then(pl.lit("Less than 50"))
        .when((pl.col(column_name) >= 50) & (pl.col(column_name) <= 100))
        .then(pl.lit("Between 50 and 100"))
        .when(pl.col(column_name) > 100)
        .then(pl.lit("Greater than 100"))
        .otherwise(pl.lit("Invalid Input"))
        .alias("category")
    )


# --- Main Execution Flow ---

def main():
    # 1. Load Credentials
    if not CREDS_PATH.exists():
        print(f"Credentials file not found at {CREDS_PATH}")
        return

    with open(CREDS_PATH, "r") as file:
        creds = yaml.safe_load(file)

    # 2. Initialize Database Engine
    db_engine = get_db_engine(creds["data_warehouse"])

    # 3. Load SQL Queries
    print("Loading SQL queries...")
    invoices_sql = load_sql_file(SQL_DIR / "invoices_main.sql")
    contracts_sql = load_sql_file(SQL_DIR / "contracts_main.sql")

    # 4. Extract Data
    print("Extracting data from database...")
    invoices_df = read_from_database_pl(db_engine, invoices_sql)
    contracts_df = read_from_database_pl(db_engine, contracts_sql)

    # 5. Transform Data
    print("Transforming data...")
    # Deduplicate contracts
    contracts_df = contracts_df.unique()
    # Classify invoices
    invoices_df = classify_invoices_pl(invoices_df, "total_import_euros")
    # Cast join keys to numeric (Int64)
    invoices_df = invoices_df.with_columns(pl.col("contract_id").cast(pl.Int64, strict=False))
    contracts_df = contracts_df.with_columns(pl.col("contract_id").cast(pl.Int64, strict=False))
    # Merge
    merged_df = invoices_df.join(contracts_df, on="contract_id", how="left")

    # 6. Create Indicators (KPIs)

    # KPI 1: Amount by Category & Client Type
    kpi_category_df = (
        merged_df
        .group_by(["category", "client_type_description"])
        .agg(pl.col("total_import_euros").sum().alias("kpi_value"))
        .with_columns(
            (
                pl.lit("Total amount in euros of the customers with invoices ")
                + pl.col("category")
                + pl.lit(" euros and ")
                + pl.col("client_type_description")
            ).alias("kpi_name")
        )
        .select(["kpi_name", "kpi_value"])
    )

    # KPI 2: Document Type Count
    kpi_doctype_df = (
        merged_df
        .group_by("document_type_description")
        .agg(pl.col("total_import_euros").count().cast(pl.Float64).alias("kpi_value"))
        .with_columns(
            (
                pl.lit("Number of invoices of invoice type ")
                + pl.col("document_type_description")
            ).alias("kpi_name")
        )
        .select(["kpi_name", "kpi_value"])
    )

    # 7. Aggregate and Load
    print("Loading results to database...")
    main_df = pl.concat([kpi_category_df, kpi_doctype_df])
    main_df = main_df.with_columns(pl.lit(date.today()).alias("kpi_date"))

    # Ensure column order matches target table
    output_df = main_df.select(["kpi_date", "kpi_name", "kpi_value"])

    write_to_database_pl(db_engine, output_df, TABLE_NAME)
    print("ETL Process completed successfully.")


if __name__ == "__main__":
    tracemalloc.start()
    start_time = time.time()
    main()
    elapsed = time.time() - start_time
    _, peak_memory = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    print(f"Polars pipeline executed in {elapsed:.2f} seconds.")
    print(f"Peak RAM usage: {peak_memory / (1024 * 1024):.2f} MB")
