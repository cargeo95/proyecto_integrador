from typing import Dict
from pathlib import Path

import requests
from pandas import DataFrame, read_csv, read_json, to_datetime
from src.config import DATASET_ROOT_PATH, PUBLIC_HOLIDAYS_URL, get_csv_to_table_mapping


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


def run_all():
    """Ejecuta la fase de extracción de datos"""
    from pandas import DataFrame

    print("🔹 [EXTRACT] Iniciando extracción de datos...")
    try:
        # Usa las funciones ya definidas en tu módulo
        mapping = get_csv_to_table_mapping()
        data_frames = extract(DATASET_ROOT_PATH, mapping, PUBLIC_HOLIDAYS_URL)

        # Validación simple
        if isinstance(data_frames, dict) and all(isinstance(v, DataFrame) for v in data_frames.values()):
            print(f"✅ [EXTRACT] {len(data_frames)} tablas extraídas correctamente.")
        else:
            print("⚠ [EXTRACT] No se devolvió un diccionario válido de DataFrames.")
        return data_frames

    except Exception as e:
        print(f"❌ [EXTRACT] Error en la extracción: {e}")
        raise