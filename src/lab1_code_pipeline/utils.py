import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from pathlib import Path


# --- Database Helper Functions ---

def get_db_engine(creds: dict) -> Engine:
    """
    Creates a SQLAlchemy engine from a credentials dictionary.
    """
    # Construct connection string safely
    # Note: Consider using a specific driver like mysql+pymysql if needed
    conn_str = (
        f"mysql://{creds['username']}:{creds['password']}"
        f"@{creds['host']}:3306/{creds['database']}"
    )
    engine = create_engine(conn_str)
    return engine

def read_from_database(engine: Engine, query: str) -> pd.DataFrame:
    """
    Executes a SQL query and returns the result as a pandas DataFrame.
    """
    return pd.read_sql(query, engine)

def write_to_database(engine: Engine, df: pd.DataFrame, table_name: str, if_exists: str = 'append'):
    """
    Writes a pandas DataFrame to a SQL database table.
    
    Args:
        engine (Engine): The SQLAlchemy engine object.
        df (pd.DataFrame): The data to write.
        table_name (str): Name of the destination table.
        if_exists (str): Action if table exists ('fail', 'replace', 'append').
    """
    with engine.begin() as connection:
        df.to_sql(table_name, con=connection, if_exists=if_exists, index=False)

def load_sql_file(filepath: Path) -> str:
    """Reads a SQL file and returns its content as a string."""
    with open(filepath, "r", encoding="utf-8") as file:
        return file.read()

# --- Data Processing Functions ---

def classify_invoices(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """
    Classifies numeric values into categories using vectorized NumPy operations 
    for performance, replacing slower .apply() methods.
    """
    conditions = [
        (df[column_name] < 50),
        (df[column_name] >= 50) & (df[column_name] <= 100),
        (df[column_name] > 100)
    ]
    choices = ["Less than 50", "Between 50 and 100", "Greater than 100"]
    
    df['category'] = np.select(conditions, choices, default="Invalid Input")
    return df