# src/load.py
from typing import Dict
from pandas import DataFrame
from sqlalchemy.engine.base import Engine
from sqlalchemy import create_engine
from src.config import SQLITE_BD_ABSOLUTE_PATH

# Mapeo opcional por si te llegan nombres "cortos" desde extract()
TABLE_NAME_MAPPING = {
    "olist_orders": "olist_orders_dataset",
    "olist_customers": "olist_customers_dataset",
    "olist_order_items": "olist_order_items_dataset",
    "olist_order_payments": "olist_order_payments_dataset",
    "olist_order_reviews": "olist_order_reviews_dataset",
    "olist_products": "olist_products_dataset",
    "olist_sellers": "olist_sellers_dataset",
    "olist_geolocation": "olist_geolocation_dataset",
    # Si ya llegan con *_dataset, se respetan tal cual
}

def load(data_frames: Dict[str, DataFrame], database: Engine) -> None:
    """
    Carga los DataFrames en SQLite usando las claves del dict como nombres de tabla
    (o las traduce a *_dataset con TABLE_NAME_MAPPING). Reemplaza si ya existen.
    Luego crea vistas de compatibilidad para transform.py:
      - olist_orders -> olist_orders_dataset
      - olist_order_items -> olist_order_items_dataset
      - olist_products -> olist_products_dataset
    """
    # 1) Cargar tablas
    for table_name, df in data_frames.items():
        if not isinstance(df, DataFrame):
            raise TypeError(f"El valor para '{table_name}' no es un DataFrame")
        final_name = TABLE_NAME_MAPPING.get(table_name, table_name)
        df.to_sql(name=final_name, con=database, if_exists="replace", index=False)

    # 2) Vistas de compatibilidad para no tocar transform.py
    with database.connect() as conn:
        conn.exec_driver_sql("DROP VIEW IF EXISTS olist_orders;")
        conn.exec_driver_sql("DROP VIEW IF EXISTS olist_order_items;")
        conn.exec_driver_sql("DROP VIEW IF EXISTS olist_products;")

        conn.exec_driver_sql("""
            CREATE VIEW olist_orders AS
            SELECT * FROM olist_orders_dataset;
        """)
        conn.exec_driver_sql("""
            CREATE VIEW olist_order_items AS
            SELECT * FROM olist_order_items_dataset;
        """)
        conn.exec_driver_sql("""
            CREATE VIEW olist_products AS
            SELECT * FROM olist_products_dataset;
        """)

def run_all(data_frames=None):
    """Ejecuta la fase de carga de datos en SQLite"""
    print("🔹 [LOAD] Iniciando carga de datos en la base de datos...")

    try:
        engine = create_engine(f"sqlite:///{SQLITE_BD_ABSOLUTE_PATH}")

        # Si se llamó sin data_frames (por prueba), no hace nada
        if data_frames:
            load(data_frames, engine)
            print(f"✅ [LOAD] Datos cargados exitosamente en {SQLITE_BD_ABSOLUTE_PATH}")
        else:
            print("ℹ [LOAD] No se recibieron dataframes, solo se conectó la base de datos.")
        return engine

    except Exception as e:
        print(f"❌ [LOAD] Error al cargar los datos: {e}")
        raise