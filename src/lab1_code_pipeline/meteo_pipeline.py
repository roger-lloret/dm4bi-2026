"""
Meteo ETL Pipeline
==================
1. Reads lat/lon for Madrid and Barcelona from the zipcode_eae table.
2. Calls the Open-Meteo API to fetch hourly temperature forecasts.
3. Stores the raw forecast data in the meteo_eae table.
4. Computes the average forecast temperature per city as a KPI.
5. Writes the KPI to the gen_kpi_ft table.
"""

import time
import tracemalloc
import yaml
import requests
import pandas as pd
from datetime import date
from pathlib import Path

from utils import get_db_engine, read_from_database, write_to_database

# --- Configuration & Constants ---
BASE_DIR = Path(__file__).parent if "__file__" in locals() else Path.cwd()
CREDS_PATH = Path("C:/Users/roger.lloret/Documents/creds/creds_dmbi.yml")
KPI_TABLE = "gen_kpi_ft"
METEO_TABLE = "meteo_eae"

# Cities to query — must match province names in zipcode_eae
CITIES = ["Madrid", "Barcelona"]

# Open-Meteo API base URL
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


# --- Helper Functions ---

def get_city_coordinates(engine, cities: list[str]) -> pd.DataFrame:
    """
    Reads lat/lon from zipcode_eae for the given cities (matched by province).
    Returns one row per city with columns: city, latitude, longitude.
    """
    placeholders = ", ".join(f"'{c}'" for c in cities)
    query = f"""
        SELECT province AS city,
               AVG(zipcode_latitude)  AS latitude,
               AVG(zipcode_longitude) AS longitude
        FROM eae.gen_zipcode_dim
        WHERE province IN ({placeholders})
        GROUP BY province
    """
    return read_from_database(engine, query)


def fetch_forecast(city: str, latitude: float, longitude: float) -> pd.DataFrame:
    """
    Calls the Open-Meteo API for a single location and returns a DataFrame
    with columns: city, datetime, temperature_2m.
    """
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": "temperature_2m",
    }
    response = requests.get(OPEN_METEO_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    hourly = data["hourly"]
    df = pd.DataFrame({
        "city": city,
        "datetime": pd.to_datetime(hourly["time"]),
        "temperature_2m": hourly["temperature_2m"],
    })
    return df


# --- Main Execution Flow ---

def main():
    # 1. Load Credentials
    with open(CREDS_PATH, "r") as f:
        creds = yaml.safe_load(f)

    db_engine = get_db_engine(creds["data_warehouse"])

    # 2. Get coordinates for Madrid & Barcelona from the database
    print("Reading city coordinates from gen_zipcode_dim...")
    coords_df = get_city_coordinates(db_engine, CITIES)
    print(coords_df.to_string(index=False))

    # 3. Fetch temperature forecasts from Open-Meteo
    print("Fetching temperature forecasts from Open-Meteo API...")
    forecast_frames = []
    for _, row in coords_df.iterrows():
        city_forecast = fetch_forecast(row["city"], row["latitude"], row["longitude"])
        forecast_frames.append(city_forecast)
        print(f"  {row['city']}: {len(city_forecast)} hourly records retrieved.")

    meteo_df = pd.concat(forecast_frames, ignore_index=True)

    # 4. Store raw forecast in meteo_eae (truncate + reload)
    print(f"Writing {len(meteo_df)} rows to {METEO_TABLE}...")
    write_to_database(db_engine, meteo_df, METEO_TABLE, if_exists="replace")

    # 5. Build KPI: average forecast temperature per city
    print("Building average temperature KPIs...")
    kpi_df = (
        meteo_df
        .groupby("city")["temperature_2m"]
        .mean()
        .reset_index()
    )
    kpi_df["kpi_name"] = "Average forecast temperature (°C) for " + kpi_df["city"]
    kpi_df = kpi_df.rename(columns={"temperature_2m": "kpi_value"})
    kpi_df["kpi_date"] = date.today()

    output_df = kpi_df[["kpi_date", "kpi_name", "kpi_value"]]
    print(output_df.to_string(index=False))

    # 6. Write KPI to gen_kpi_ft
    print(f"Writing KPIs to {KPI_TABLE}...")
    write_to_database(db_engine, output_df, KPI_TABLE)
    print("Meteo ETL completed successfully.")


if __name__ == "__main__":
    tracemalloc.start()
    start_time = time.time()
    main()
    elapsed = time.time() - start_time
    _, peak_memory = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    print(f"Meteo pipeline executed in {elapsed:.2f} seconds.")
    print(f"Peak RAM usage: {peak_memory / (1024 * 1024):.2f} MB")
