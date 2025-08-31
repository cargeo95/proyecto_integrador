# from typing import Dict

# from pandas import DataFrame
# from sqlalchemy.engine.base import Engine


# def load(data_frames: Dict[str, DataFrame], database: Engine):
#     """Load the dataframes into the sqlite database.

#     Args:
#         data_frames (Dict[str, DataFrame]): A dictionary with keys as the table names
#         and values as the dataframes.
#     """
#     # TODO: Implementa esta función. Por cada DataFrame en el diccionario, debes
#     # usar pandas.DataFrame.to_sql() para cargar el DataFrame en la base de datos
#     # como una tabla.
#     # Para el nombre de la tabla, utiliza las claves del diccionario `data_frames`.
#     raise NotImplementedError


# src/load.py
from typing import Dict
from pandas import DataFrame
from sqlalchemy.engine.base import Engine

# Si en extract() salen nombres "cortos" como olist_orders,
# este mapeo los convierte a los que usan los tests (*_dataset).
TABLE_NAME_MAPPING = {
    "olist_orders": "olist_orders_dataset",
    "olist_customers": "olist_customers_dataset",
    "olist_order_items": "olist_order_items_dataset",
    "olist_order_payments": "olist_order_payments_dataset",
    "olist_order_reviews": "olist_order_reviews_dataset",
    "olist_products": "olist_products_dataset",
    "olist_sellers": "olist_sellers_dataset",
    "olist_geolocation": "olist_geolocation_dataset",
}

def load(data_frames: Dict[str, DataFrame], database: Engine) -> None:
    """
    Carga los DataFrames en la base SQLite.
    - Usa la clave del dict como nombre de tabla (o la traduce con TABLE_NAME_MAPPING).
    - Reemplaza si ya existe.
    """
    for table_name, df in data_frames.items():
        if not isinstance(df, DataFrame):
            raise TypeError(f"El valor para '{table_name}' no es un DataFrame")

        final_name = TABLE_NAME_MAPPING.get(table_name, table_name)
        df.to_sql(name=final_name, con=database, if_exists="replace", index=False)

# --------------------------------------------------------------------
# Bloque de utilidad: crear un .db en disco para inspección en DBeaver
# --------------------------------------------------------------------
if __name__ == "__main__":
    # 1) Imports locales para no romper los tests
    from sqlalchemy import create_engine
    from pathlib import Path
    from src.extract import extract, get_public_holidays  # usamos extract()
    import pandas as pd

    # 2) Parámetros de entrada
    DATASET_ROOT_PATH = "dataset"  # carpeta donde están tus CSV
    PUBLIC_HOLIDAYS_URL = "https://date.nager.at/api/v3/PublicHolidays"

    # 3) Mapeo "archivo CSV" -> "nombre de tabla" EXACTO como lo usan las queries/tests
    csv_table_mapping = {
        "olist_orders_dataset.csv": "olist_orders_dataset",
        "olist_customers_dataset.csv": "olist_customers_dataset",
        "olist_order_items_dataset.csv": "olist_order_items_dataset",
        "olist_order_payments_dataset.csv": "olist_order_payments_dataset",
        "olist_order_reviews_dataset.csv": "olist_order_reviews_dataset",
        "olist_products_dataset.csv": "olist_products_dataset",
        "olist_sellers_dataset.csv": "olist_sellers_dataset",
        "olist_geolocation_dataset.csv": "olist_geolocation_dataset",
        "product_category_name_translation.csv": "product_category_name_translation",
    }

    # 4) Crear un engine a un archivo físico
    db_path = Path("olist_dw_debug.db").absolute()
    engine = create_engine(f"sqlite:///{db_path}")

    # 5) Extraer dataframes y cargar
    dfs = extract(
        csv_folder=DATASET_ROOT_PATH,
        csv_table_mapping=csv_table_mapping,
        public_holidays_url=PUBLIC_HOLIDAYS_URL,
    )
    load(dfs, engine)

    # 6) Mostrar las tablas resultantes (para confirmar en consola)
    with engine.connect() as conn:
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    print("✅ DataWarehouse creado en:", db_path)
    print("🔎 Tablas creadas:")
    for (t,) in tables:
        print("   -", t)

    print("\nCómo verlo en DBeaver:")
    print("1) Database > New Connection > SQLite.")
    print(f"2) En 'Database file' selecciona: {db_path}")
    print("3) Test Connection > Finish. Expande y abre las tablas.")
