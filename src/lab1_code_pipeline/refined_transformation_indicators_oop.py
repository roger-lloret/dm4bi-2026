"""
Reusable Object-Oriented ETL Framework using Pandas.

The design follows three principles that make it easy to create new pipelines:

  1. ABSTRACT BASE CLASSES  — define the contract every pipeline must follow.
  2. DEPENDENCY INJECTION   — pass a DatabaseManager in; don't create it inside.
  3. PLUG-IN KPIs           — register KPI functions in a list; add new ones
                              without touching the orchestrator.

Class hierarchy
───────────────
  DatabaseManager          — reusable DB I/O (engine, read, write, load_sql)
  BaseTransformer (ABC)    — override transform() for your domain logic
  BaseKPI (ABC)            — override build() for each KPI calculation
  BasePipeline (ABC)       — override extract / get_transformer / get_kpis / ...

Concrete implementations for the invoices use-case
──────────────────────────────────────────────────
  InvoiceTransformer       — classify, cast, merge invoices + contracts
  AmountByCategoryKPI      — KPI 1
  CountByDocTypeKPI        — KPI 2
  InvoiceKPIPipeline       — wires everything together
"""

import time
import tracemalloc
import yaml
import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
from datetime import date
from pathlib import Path
from typing import Callable
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


# =====================================================================
#  REUSABLE: DATABASE MANAGER
# =====================================================================

class DatabaseManager:
    """
    Generic database helper.  Inject credentials at construction time;
    reuse the same instance across any number of pipelines.
    """

    def __init__(self, creds: dict):
        conn_str = (
            f"mysql://{creds['username']}:{creds['password']}"
            f"@{creds['host']}:3306/{creds['database']}"
        )
        self.engine: Engine = create_engine(conn_str)

    def read(self, query: str) -> pd.DataFrame:
        return pd.read_sql(query, self.engine)

    def write(self, df: pd.DataFrame, table_name: str, if_exists: str = "append"):
        with self.engine.begin() as conn:
            df.to_sql(table_name, con=conn, if_exists=if_exists, index=False)

    @staticmethod
    def load_sql(filepath: Path) -> str:
        with open(filepath, "r", encoding="utf-8") as f:
            return f.read()

    @staticmethod
    def load_credentials(path: Path) -> dict:
        if not path.exists():
            raise FileNotFoundError(f"Credentials file not found at {path}")
        with open(path, "r") as f:
            return yaml.safe_load(f)


# =====================================================================
#  REUSABLE: ABSTRACT BASE CLASSES
# =====================================================================

class BaseTransformer(ABC):
    """
    Override transform() with your domain-specific logic.
    Any pipeline can plug in a different transformer.
    """

    @abstractmethod
    def transform(self, **dataframes) -> pd.DataFrame:
        """Receive named raw DataFrames, return a single transformed DataFrame."""
        ...


class BaseKPI(ABC):
    """
    Each KPI is its own class.  To add a new metric:
      1. Subclass BaseKPI
      2. Implement build()
      3. Register it in the pipeline's get_kpis() list
    """

    @abstractmethod
    def build(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return a DataFrame with columns [kpi_name, kpi_value]."""
        ...


class BasePipeline(ABC):
    """
    Template-method pattern: run() calls extract → transform → build_kpis → load.
    Subclasses only override the parts that change.
    """

    def __init__(self, db: DatabaseManager, table_name: str):
        self.db = db
        self.table_name = table_name

    # --- Override these in each concrete pipeline ---

    @abstractmethod
    def extract(self) -> dict[str, pd.DataFrame]:
        """Return a dict of named DataFrames (e.g. {'invoices': df, 'contracts': df})."""
        ...

    @abstractmethod
    def get_transformer(self) -> BaseTransformer:
        """Return the transformer instance for this pipeline."""
        ...

    @abstractmethod
    def get_kpis(self) -> list[BaseKPI]:
        """Return the list of KPI builders to apply."""
        ...

    # --- Shared logic (no need to override) --------------------------

    def build_kpis(self, merged: pd.DataFrame) -> pd.DataFrame:
        """Runs every registered KPI and concatenates results."""
        frames = [kpi.build(merged) for kpi in self.get_kpis()]
        result = pd.concat(frames)
        result["kpi_date"] = date.today()
        return result[["kpi_date", "kpi_name", "kpi_value"]]

    def load(self, df: pd.DataFrame):
        print(f"Loading {len(df)} rows to '{self.table_name}'...")
        self.db.write(df, self.table_name)

    def run(self):
        """Template method — orchestrates the full pipeline."""
        raw = self.extract()
        merged = self.get_transformer().transform(**raw)
        output = self.build_kpis(merged)
        self.load(output)
        print("Pipeline completed successfully.")


# =====================================================================
#  CONCRETE: TRANSFORMER FOR INVOICES
# =====================================================================

class InvoiceTransformer(BaseTransformer):
    """Deduplicates, classifies, casts, and merges invoices + contracts."""

    @staticmethod
    def classify(df: pd.DataFrame, column: str) -> pd.DataFrame:
        conditions = [
            df[column] < 50,
            (df[column] >= 50) & (df[column] <= 100),
            df[column] > 100,
        ]
        choices = ["Less than 50", "Between 50 and 100", "Greater than 100"]
        df["category"] = np.select(conditions, choices, default="Invalid Input")
        return df

    def transform(self, *, invoices: pd.DataFrame, contracts: pd.DataFrame) -> pd.DataFrame:
        print("Transforming data...")
        contracts = contracts.drop_duplicates()
        invoices = self.classify(invoices, "total_import_euros")
        invoices["contract_id"] = pd.to_numeric(invoices["contract_id"], errors="coerce")
        contracts["contract_id"] = pd.to_numeric(contracts["contract_id"], errors="coerce")
        return invoices.merge(contracts, on="contract_id", how="left")


# =====================================================================
#  CONCRETE: KPI DEFINITIONS
# =====================================================================

class AmountByCategoryKPI(BaseKPI):
    """KPI 1: Total amount by invoice category and client type."""

    def build(self, df: pd.DataFrame) -> pd.DataFrame:
        result = (
            df
            .groupby(["category", "client_type_description"])["total_import_euros"]
            .sum()
            .reset_index()
        )
        result["kpi_name"] = (
            "Total amount in euros of the customers with invoices "
            + result["category"]
            + " euros and "
            + result["client_type_description"]
        )
        return result.rename(columns={"total_import_euros": "kpi_value"})[["kpi_name", "kpi_value"]]


class CountByDocTypeKPI(BaseKPI):
    """KPI 2: Invoice count by document type."""

    def build(self, df: pd.DataFrame) -> pd.DataFrame:
        result = (
            df
            .groupby("document_type_description")["total_import_euros"]
            .count()
            .reset_index()
        )
        result["kpi_name"] = "Number of invoices of invoice type " + result["document_type_description"]
        return result.rename(columns={"total_import_euros": "kpi_value"})[["kpi_name", "kpi_value"]]


# =====================================================================
#  CONCRETE: INVOICE KPI PIPELINE
# =====================================================================

class InvoiceKPIPipeline(BasePipeline):
    """
    Concrete pipeline for invoice KPIs.
    To create a different pipeline (e.g. consumption KPIs):
      1. Subclass BasePipeline
      2. Override extract(), get_transformer(), get_kpis()
      3. Call .run()
    """

    def __init__(self, db: DatabaseManager, sql_dir: Path, table_name: str = "gen_kpi_ft"):
        super().__init__(db, table_name)
        self.sql_dir = sql_dir

    def extract(self) -> dict[str, pd.DataFrame]:
        print("Extracting data from database...")
        invoices_sql = DatabaseManager.load_sql(self.sql_dir / "invoices_main.sql")
        contracts_sql = DatabaseManager.load_sql(self.sql_dir / "contracts_main.sql")
        return {
            "invoices": self.db.read(invoices_sql),
            "contracts": self.db.read(contracts_sql),
        }

    def get_transformer(self) -> BaseTransformer:
        return InvoiceTransformer()

    def get_kpis(self) -> list[BaseKPI]:
        return [
            AmountByCategoryKPI(),
            CountByDocTypeKPI(),
        ]


# =====================================================================
#  ENTRY POINT
# =====================================================================

# Configuration — change these to wire up a different pipeline
BASE_DIR = Path(__file__).parent if "__file__" in locals() else Path.cwd()
CREDS_PATH = Path("C:/Users/roger.lloret/Documents/creds/creds_dmbi.yml")
SQL_DIR = BASE_DIR / "sql"

if __name__ == "__main__":
    tracemalloc.start()
    start_time = time.time()

    # 1. Load credentials & create a reusable DatabaseManager
    creds = DatabaseManager.load_credentials(CREDS_PATH)
    db = DatabaseManager(creds["data_warehouse"])

    # 2. Create and run the invoice KPI pipeline
    pipeline = InvoiceKPIPipeline(db, sql_dir=SQL_DIR)
    pipeline.run()

    # 3. Report metrics
    elapsed = time.time() - start_time
    _, peak_memory = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    print(f"Pandas (OOP) pipeline executed in {elapsed:.2f} seconds.")
    print(f"Peak RAM usage: {peak_memory / (1024 * 1024):.2f} MB")
