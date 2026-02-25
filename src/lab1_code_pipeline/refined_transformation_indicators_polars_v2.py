"""
=============================================================================
 Polars ETL Pipeline — Didactic Version
=============================================================================
 This script demonstrates key Polars features that make it faster and more
 expressive than pandas:

 1. LAZY EVALUATION (LazyFrame)
    - Polars builds a query plan instead of executing immediately.
    - The engine optimizes the plan (predicate pushdown, projection pushdown,
      common subexpression elimination) before running anything.
    - You call .collect() only when you need the final result.

 2. EXPRESSION API
    - Instead of chaining Python methods row-by-row, Polars uses composable
      expressions (pl.col, pl.when, pl.lit) that are compiled to fast Rust code.
    - Multiple columns can be created/transformed in a single .with_columns().

 3. PARALLEL EXECUTION
    - Polars automatically parallelizes operations across CPU cores.
    - No GIL limitation — the heavy lifting runs in Rust threads.

 4. METHOD CHAINING
    - The API is designed for fluent, readable pipelines where each step
      produces a new (Lazy)Frame without mutating the original.
=============================================================================
"""

import time
import tracemalloc
import yaml
import polars as pl
from datetime import date
from pathlib import Path

from utils import get_db_engine, load_sql_file

# --- Configuration ---
BASE_DIR = Path(__file__).parent if "__file__" in locals() else Path.cwd()
CREDS_PATH = Path("C:/Users/roger.lloret/Documents/creds/creds_dmbi.yml")
SQL_DIR = BASE_DIR / "sql"
TABLE_NAME = "gen_kpi_ft"


# =====================================================================
#  HELPER FUNCTIONS
# =====================================================================

def read_from_database_pl(engine, query: str) -> pl.LazyFrame:
    """
    Reads from MySQL and returns a **LazyFrame**.
    
    Why LazyFrame?
    - No computation happens yet — Polars just records what to do.
    - Subsequent filters, selects, and joins are optimized together.
    """
    uri = engine.url.render_as_string(hide_password=False)
    # .lazy() converts the eager DataFrame to a LazyFrame for optimization
    return pl.read_database_uri(query, uri=uri).lazy()


def write_to_database_pl(engine, df: pl.DataFrame, table_name: str, if_exists: str = "append"):
    """Writes a Polars DataFrame to MySQL (converts to pandas for to_sql)."""
    with engine.begin() as connection:
        df.to_pandas().to_sql(table_name, con=connection, if_exists=if_exists, index=False)


# =====================================================================
#  REUSABLE POLARS EXPRESSIONS
# =====================================================================
# Expressions are first-class objects in Polars.  You can define them once
# and reuse them across different pipelines — like SQL column definitions.

# Expression: classify invoice amounts into categories
CATEGORY_EXPR = (
    pl.when(pl.col("total_import_euros") < 50)
      .then(pl.lit("Less than 50"))
      .when(pl.col("total_import_euros").is_between(50, 100))
      .then(pl.lit("Between 50 and 100"))
      .when(pl.col("total_import_euros") > 100)
      .then(pl.lit("Greater than 100"))
      .otherwise(pl.lit("Invalid Input"))
      .alias("category")
)

# Expression: cast contract_id to Int64 for safe joins
CAST_CONTRACT_ID = pl.col("contract_id").cast(pl.Int64, strict=False)


# =====================================================================
#  MAIN PIPELINE
# =====================================================================

def main():
    # -----------------------------------------------------------------
    # 1. CREDENTIALS
    # -----------------------------------------------------------------
    if not CREDS_PATH.exists():
        print(f"Credentials file not found at {CREDS_PATH}")
        return

    with open(CREDS_PATH, "r") as f:
        creds = yaml.safe_load(f)

    db_engine = get_db_engine(creds["data_warehouse"])

    # -----------------------------------------------------------------
    # 2. EXTRACT — returns LazyFrames (nothing is computed yet)
    # -----------------------------------------------------------------
    print("[Extract] Loading SQL queries and creating lazy readers...")
    invoices_sql = load_sql_file(SQL_DIR / "invoices_main.sql")
    contracts_sql = load_sql_file(SQL_DIR / "contracts_main.sql")

    invoices_lf = read_from_database_pl(db_engine, invoices_sql)
    contracts_lf = read_from_database_pl(db_engine, contracts_sql)

    # -----------------------------------------------------------------
    # 3. TRANSFORM — build the query plan lazily
    # -----------------------------------------------------------------
    print("[Transform] Building lazy query plan...")

    # Polars Power #1: MULTIPLE TRANSFORMS IN ONE .with_columns()
    # Instead of mutating the DataFrame step-by-step, we declare all new
    # columns at once.  Polars can execute them in parallel.
    invoices_lf = invoices_lf.with_columns(
        CAST_CONTRACT_ID,     # cast contract_id
        CATEGORY_EXPR,        # classify invoices — reusable expression!
    )

    # Polars Power #2: CHAINED LAZY OPERATIONS
    # unique() + with_columns() are recorded in the plan, not executed yet.
    contracts_lf = (
        contracts_lf
        .unique()                                  # deduplicate
        .with_columns(CAST_CONTRACT_ID)            # cast contract_id
    )

    # Polars Power #3: LAZY JOIN
    # The optimizer may reorder or push filters before the join.
    merged_lf = invoices_lf.join(contracts_lf, on="contract_id", how="left")

    # -----------------------------------------------------------------
    # 4. CREATE KPIs — still lazy, still optimizable
    # -----------------------------------------------------------------

    # KPI 1: Total amount by category & client type
    kpi_category_lf = (
        merged_lf
        .group_by("category", "client_type_description")
        .agg(pl.col("total_import_euros").sum().alias("kpi_value"))
        .with_columns(
            (
                pl.lit("Total amount in euros of the customers with invoices ")
                + pl.col("category")
                + pl.lit(" euros and ")
                + pl.col("client_type_description")
            ).alias("kpi_name")
        )
        .select("kpi_name", "kpi_value")
    )

    # KPI 2: Invoice count by document type
    kpi_doctype_lf = (
        merged_lf
        .group_by("document_type_description")
        .agg(pl.col("total_import_euros").count().cast(pl.Float64).alias("kpi_value"))
        .with_columns(
            (
                pl.lit("Number of invoices of invoice type ")
                + pl.col("document_type_description")
            ).alias("kpi_name")
        )
        .select("kpi_name", "kpi_value")
    )

    # -----------------------------------------------------------------
    # 5. COLLECT — this is where Polars actually executes everything
    # -----------------------------------------------------------------
    # Polars Power #4: SINGLE .collect() TRIGGERS THE OPTIMIZED PLAN
    # The engine sees the full pipeline and optimizes before running.

    print("[Collect] Executing optimized query plan...")
    kpi_category_df = kpi_category_lf.collect()
    kpi_doctype_df = kpi_doctype_lf.collect()

    # -----------------------------------------------------------------
    # 6. FINAL ASSEMBLY & LOAD
    # -----------------------------------------------------------------
    output_df = (
        pl.concat([kpi_category_df, kpi_doctype_df])
        .with_columns(pl.lit(date.today()).alias("kpi_date"))
        .select("kpi_date", "kpi_name", "kpi_value")
    )

    # Quick preview of the results
    print("\n--- Output Preview ---")
    print(output_df)
    print(f"\nSchema: {output_df.schema}")
    print(f"Rows:   {output_df.height}")

    print("\n[Load] Writing results to database...")
    write_to_database_pl(db_engine, output_df, TABLE_NAME)
    print("[Done] ETL Process completed successfully.")


if __name__ == "__main__":
    tracemalloc.start()
    start_time = time.time()
    main()
    elapsed = time.time() - start_time
    _, peak_memory = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    print(f"\nPolars (lazy) pipeline executed in {elapsed:.2f} seconds.")
    print(f"Peak RAM usage: {peak_memory / (1024 * 1024):.2f} MB")
