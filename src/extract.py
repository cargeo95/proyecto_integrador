from typing import Dict
from pathlib import Path

import requests
from pandas import DataFrame, read_csv, read_json, to_datetime


def temp() -> DataFrame:
    """Get the temperature data."""
    return read_csv("data/temperature.csv")


def get_public_holidays(public_holidays_url: str, year: str) -> DataFrame:
    """Get the public holidays for the given year for Brazil."""
    url = f"{public_holidays_url}/{year}/BR"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise SystemExit(f"Error fetching public holidays from {url}: {e}")

    df = read_json(resp.text)
    for col in ("types", "counties"):
        if col in df.columns:
            df = df.drop(columns=[col])
    if "date" in df.columns:
        df["date"] = to_datetime(df["date"])
    return df


def extract(
    csv_folder: str, csv_table_mapping: Dict[str, str], public_holidays_url: str
) -> Dict[str, DataFrame]:
    """Extract the data from the csv files and load them into the dataframes."""
    csv_folder_path = Path(csv_folder)  
    dataframes = {
        table_name: read_csv(csv_folder_path / csv_file)  
        for csv_file, table_name in csv_table_mapping.items()
    }

    holidays = get_public_holidays(public_holidays_url, "2017")
    dataframes["public_holidays"] = holidays
    return dataframes
