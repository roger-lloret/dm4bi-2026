import yaml
import pandas as pd
from pathlib import Path

from utils import get_db_engine, write_to_database

# --- Configuration ---
CREDS_PATH = Path("C:/Users/roger.lloret/Documents/creds/creds_dmbi.yml")
DATA_DIR = Path("C:/Users/roger.lloret/Documents/EAE/data")

# Mapping: CSV filename -> MySQL table name
CSV_TO_TABLE = {
    "client_type_eae.csv": "con_client_type_dim",
    "invoices_eae.csv": "inv_invoice_ft",
    "contract_eae.csv": "con_contract_dim",
    "doctype_eae.csv": "inv_doc_type_dim",
}


def main():
    # 1. Load credentials
    with open(CREDS_PATH, "r") as f:
        creds = yaml.safe_load(f)

    # 2. Create engine targeting the 'eae' schema
    db_engine = get_db_engine(creds["data_warehouse"])

    # 3. Load each CSV and write to MySQL (truncate first via 'replace')
    for csv_file, table_name in CSV_TO_TABLE.items():
        csv_path = DATA_DIR / csv_file
        print(f"Loading {csv_path} -> {table_name} ...")
        df = pd.read_csv(csv_path, sep=';')
        write_to_database(db_engine, df, table_name, if_exists="replace")
        print(f"  {len(df)} rows written to {table_name}.")
    print("All tables loaded successfully.")


if __name__ == "__main__":
    main()
