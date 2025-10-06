from collections import namedtuple
from enum import Enum
from typing import Callable, Dict, List
import pandas as pd
from pandas import DataFrame, read_sql
from sqlalchemy import text, create_engine
from sqlalchemy.engine.base import Engine
from src.config import QUERIES_ROOT_PATH, SQLITE_BD_ABSOLUTE_PATH

# ---------------------------------------------------------------
# Definiciones generales
# ---------------------------------------------------------------
QueryResult = namedtuple("QueryResult", ["query", "result"])

class QueryEnum(Enum):
    """Enumeración de las consultas disponibles"""
    DELIVERY_DATE_DIFFERECE = "delivery_date_difference"
    GLOBAL_AMMOUNT_ORDER_STATUS = "global_ammount_order_status"
    REVENUE_BY_MONTH_YEAR = "revenue_by_month_year"
    REVENUE_PER_STATE = "revenue_per_state"
    TOP_10_LEAST_REVENUE_CATEGORIES = "top_10_least_revenue_categories"
    TOP_10_REVENUE_CATEGORIES = "top_10_revenue_categories"
    REAL_VS_ESTIMATED_DELIVERED_TIME = "real_vs_estimated_delivered_time"
    ORDERS_PER_DAY_AND_HOLIDAYS_2017 = "orders_per_day_and_holidays_2017"
    GET_FREIGHT_VALUE_WEIGHT_RELATIONSHIP = "get_freight_value_weight_relationship"

# ---------------------------------------------------------------
# Función de lectura de archivos SQL
# ---------------------------------------------------------------
def read_query(query_name: str) -> str:
    """Lee el contenido de una consulta SQL desde /queries"""
    with open(f"{QUERIES_ROOT_PATH}/{query_name}.sql", "r", encoding="utf-8") as f:
        sql_file = f.read()
        sql = text(sql_file)
    return sql

# ---------------------------------------------------------------
# Consultas principales
# ---------------------------------------------------------------
def query_delivery_date_difference(database: Engine) -> QueryResult:
    query_name = QueryEnum.DELIVERY_DATE_DIFFERECE.value
    query = read_query(query_name)
    return QueryResult(query=query_name, result=read_sql(query, database))

def query_global_ammount_order_status(database: Engine) -> QueryResult:
    query_name = QueryEnum.GLOBAL_AMMOUNT_ORDER_STATUS.value
    query = read_query(query_name)
    return QueryResult(query=query_name, result=read_sql(query, database))

def query_revenue_by_month_year(database: Engine) -> QueryResult:
    query_name = QueryEnum.REVENUE_BY_MONTH_YEAR.value
    query = read_query(query_name)
    return QueryResult(query=query_name, result=read_sql(query, database))

def query_revenue_per_state(database: Engine) -> QueryResult:
    query_name = QueryEnum.REVENUE_PER_STATE.value
    query = read_query(query_name)
    return QueryResult(query=query_name, result=read_sql(query, database))

def query_top_10_least_revenue_categories(database: Engine) -> QueryResult:
    query_name = QueryEnum.TOP_10_LEAST_REVENUE_CATEGORIES.value
    query = read_query(query_name)
    return QueryResult(query=query_name, result=read_sql(query, database))

def query_top_10_revenue_categories(database: Engine) -> QueryResult:
    query_name = QueryEnum.TOP_10_REVENUE_CATEGORIES.value
    query = read_query(query_name)
    return QueryResult(query=query_name, result=read_sql(query, database))

def query_real_vs_estimated_delivered_time(database: Engine) -> QueryResult:
    query_name = QueryEnum.REAL_VS_ESTIMATED_DELIVERED_TIME.value
    query = read_query(query_name)
    return QueryResult(query=query_name, result=read_sql(query, database))

# ---------------------------------------------------------------
# Consultas pandas avanzadas (todavía pendientes)
# ---------------------------------------------------------------
def query_freight_value_weight_relationship(database: Engine) -> QueryResult:
    query_name = QueryEnum.GET_FREIGHT_VALUE_WEIGHT_RELATIONSHIP.value
    # Tablas de referencia (vistas creadas en load.py)
    orders = read_sql("SELECT * FROM olist_orders", database)
    items = read_sql("SELECT * FROM olist_order_items", database)
    products = read_sql("SELECT * FROM olist_products", database)

    # TODO: implementar correctamente las fusiones según tu práctica
    aggregations = pd.DataFrame()
    return QueryResult(query=query_name, result=aggregations)

def query_orders_per_day_and_holidays_2017(database: Engine) -> QueryResult:
    query_name = QueryEnum.ORDERS_PER_DAY_AND_HOLIDAYS_2017.value
    holidays = read_sql("SELECT * FROM public_holidays", database)
    orders = read_sql("SELECT * FROM olist_orders", database)

    # TODO: implementar correctamente los pasos indicados
    result_df = pd.DataFrame()
    return QueryResult(query=query_name, result=result_df)

# ---------------------------------------------------------------
# Ejecutor general de queries
# ---------------------------------------------------------------
def get_all_queries() -> List[Callable[[Engine], QueryResult]]:
    return [
        query_delivery_date_difference,
        query_global_ammount_order_status,
        query_revenue_by_month_year,
        query_revenue_per_state,
        query_top_10_least_revenue_categories,
        query_top_10_revenue_categories,
        query_real_vs_estimated_delivered_time,
        query_orders_per_day_and_holidays_2017,
        query_freight_value_weight_relationship,
    ]

def run_queries(database: Engine) -> Dict[str, DataFrame]:
    """Ejecuta todas las consultas y devuelve los resultados en un diccionario."""
    query_results = {}
    for query in get_all_queries():
        query_result = query(database)
        query_results[query_result.query] = query_result.result
    return query_results

# ---------------------------------------------------------------
# Orquestador local (para el pipeline)
# ---------------------------------------------------------------
def run_all():
    """Ejecuta todas las transformaciones SQL definidas en /queries"""
    print("🔹 [TRANSFORM] Ejecutando transformaciones y consultas SQL...")

    try:
        # Evita importación circular
        engine = create_engine(f"sqlite:///{SQLITE_BD_ABSOLUTE_PATH}")
        results = run_queries(engine)

        print(f"✅ [TRANSFORM] {len(results)} transformaciones ejecutadas exitosamente.")
        return results

    except Exception as e:
        print(f"❌ [TRANSFORM] Error en las transformaciones: {e}")
        raise
