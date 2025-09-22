# dashboard/app.py
from pathlib import Path
from typing import List, Tuple

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from dash import Dash, dcc, html
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
QUERIES_DIR = PROJECT_ROOT / "queries"
DEFAULT_DB_PATH = PROJECT_ROOT / "olist_dw_debug.db"  # DB fija en disco

SQL_FILES = {
    "revenue_by_month_year": "revenue_by_month_year.sql",
    "top_10_revenue_categories": "top_10_revenue_categories.sql",
    "top_10_least_revenue_categories": "top_10_least_revenue_categories.sql",
    "delivery_date_difference": "delivery_date_difference.sql",
    "real_vs_estimated_delivered_time": "real_vs_estimated_delivered_time.sql",
}

# ------------------------------------------------------------------------------
# Data helpers
# ------------------------------------------------------------------------------
def get_engine(db_path: Path = DEFAULT_DB_PATH) -> Engine:
    if not db_path.exists():
        raise FileNotFoundError(
            f"No se encontró la base de datos en {db_path}.\n"
            "Genera olist_dw_debug.db antes de abrir el dashboard."
        )
    return create_engine(f"sqlite:///{db_path}")

def run_sql_file(engine: Engine, sql_filename: str) -> pd.DataFrame:
    file_path = QUERIES_DIR / sql_filename
    if not file_path.exists():
        raise FileNotFoundError(f"No existe el archivo SQL: {file_path}")
    with open(file_path, "r", encoding="utf-8") as f:
        sql_text = f.read()
    return pd.read_sql_query(text(sql_text), engine)

def safe_df(fn, *args, **kwargs) -> Tuple[pd.DataFrame, str]:
    try:
        df = fn(*args, **kwargs)
        return df, ""
    except Exception as e:
        return pd.DataFrame(), f"{type(e).__name__}: {e}"

def empty_figure_with_message(title: str, msg: str) -> go.Figure:
    fig = go.Figure()
    fig.add_annotation(
        text=f"<b>{title}</b><br>{msg}",
        showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper", align="center"
    )
    fig.update_layout(margin=dict(l=40, r=40, t=60, b=40), template="simple_white")
    return fig

# ------------------------------------------------------------------------------
# Figures
# ------------------------------------------------------------------------------
def figure_revenue_heatmap(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return empty_figure_with_message("Revenue por mes/año", "Sin datos")
    if "month_no" in df.columns:
        df = df.sort_values("month_no")
    year_cols = [c for c in df.columns if c.startswith("Year")] or \
                [c for c in df.columns if any(y in c for y in ["2016","2017","2018"])]
    months = df["month"].tolist() if "month" in df.columns else df["month_no"].tolist()
    z = df[year_cols].T.values
    fig = go.Figure(data=go.Heatmap(
        z=z, x=months, y=year_cols,
        colorbar=dict(title="Revenue"),
        hovertemplate="Mes: %{x}<br>Año: %{y}<br>Revenue: %{z:.2f}<extra></extra>"
    ))
    fig.update_layout(
        title="Revenue por mes (2016–2018)",
        xaxis_title="Mes", yaxis_title="Año",
        margin=dict(l=24, r=16, t=50, b=24), template="simple_white",
    )
    return fig

def figure_top_categories(df: pd.DataFrame, title: str) -> go.Figure:
    if df.empty:
        return empty_figure_with_message(title, "Sin datos")
    sort_col = "Revenue" if "Revenue" in df.columns else df.columns[-1]
    df = df.sort_values(sort_col, ascending=False)
    fig = px.bar(
        df, x="Revenue", y="Category", orientation="h",
        hover_data=[c for c in df.columns if c not in ["Category","Revenue"]],
        template="simple_white",
    )
    fig.update_layout(
        title=title, xaxis_title="Revenue", yaxis_title="Categoría",
        margin=dict(l=24, r=16, t=50, b=24), bargap=0.15,
    )
    return fig

def figure_bottom_categories(df: pd.DataFrame, title: str) -> go.Figure:
    if df.empty:
        return empty_figure_with_message(title, "Sin datos")
    sort_col = "Revenue" if "Revenue" in df.columns else df.columns[-1]
    df = df.sort_values(sort_col, ascending=True)
    fig = px.bar(
        df, x="Revenue", y="Category", orientation="h",
        hover_data=[c for c in df.columns if c not in ["Category","Revenue"]],
        template="simple_white",
    )
    fig.update_layout(
        title=title, xaxis_title="Revenue", yaxis_title="Categoría",
        margin=dict(l=24, r=16, t=50, b=24), bargap=0.15,
    )
    return fig

def figure_delivery_diff(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return empty_figure_with_message("Diferencia estimado vs real por estado", "Sin datos")
    df = df.sort_values(["Delivery_Difference","State"], ascending=[True, True])
    fig = px.bar(df, x="Delivery_Difference", y="State", orientation="h", template="simple_white")
    fig.update_layout(
        title="Diferencia entre fecha estimada y entrega real (días) por estado",
        xaxis_title="(+) Antes de lo estimado | (−) Después",
        yaxis_title="Estado",
        margin=dict(l=24, r=16, t=50, b=24), bargap=0.15,
    )
    return fig

def figure_real_vs_estimated(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return empty_figure_with_message("Tiempo real vs estimado", "Sin datos")
    def find_col(options: List[str]) -> str:
        for opt in options:
            for c in df.columns:
                if opt == c.lower():
                    return c
        return ""
    month_col = find_col(["month","mes","month_name"]) or \
                next((c for c in df.columns if "month" in c.lower() or "mes" in c.lower()), df.columns[0])
    real_col  = find_col(["real","real_days","real_time","real_delivery_time"]) or \
                next((c for c in df.columns if "real" in c.lower()), df.columns[1])
    est_col   = find_col(["estimated","estimated_days","estimated_time","estimated_delivery_time"]) or \
                next((c for c in df.columns if "estim" in c.lower()), df.columns[2 if len(df.columns)>2 else 1])

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df[month_col], y=df[real_col], mode="lines+markers", name="Real"))
    fig.add_trace(go.Scatter(x=df[month_col], y=df[est_col],  mode="lines+markers", name="Estimado"))
    fig.update_layout(
        title="Tiempo de entrega: Real vs Estimado",
        xaxis_title="Mes", yaxis_title="Días",
        margin=dict(l=24, r=16, t=50, b=24), template="simple_white",
    )
    return fig

# ------------------------------------------------------------------------------
# Carga una vez y construye figuras
# ------------------------------------------------------------------------------
engine = None
try:
    engine = get_engine(DEFAULT_DB_PATH)
except Exception as e:
    err = f"{type(e).__name__}: {e}"
    fig1 = empty_figure_with_message("Revenue por mes/año", err)
    fig2 = empty_figure_with_message("Top 10 categorías por Revenue", err)
    fig3 = empty_figure_with_message("Bottom 10 categorías por Revenue", err)
    fig4 = empty_figure_with_message("Diferencia estimado vs real por estado", err)
    fig5 = empty_figure_with_message("Tiempo real vs estimado", err)
else:
    df_rev_heat, err1 = safe_df(run_sql_file, engine, SQL_FILES["revenue_by_month_year"])
    df_top10,    err2 = safe_df(run_sql_file, engine, SQL_FILES["top_10_revenue_categories"])
    df_bottom10, err3 = safe_df(run_sql_file, engine, SQL_FILES["top_10_least_revenue_categories"])
    df_deliv,    err4 = safe_df(run_sql_file, engine, SQL_FILES["delivery_date_difference"])
    df_realest,  err5 = safe_df(run_sql_file, engine, SQL_FILES["real_vs_estimated_delivered_time"])

    fig1 = figure_revenue_heatmap(df_rev_heat) if not err1 else empty_figure_with_message("Revenue por mes/año", err1)
    fig2 = figure_top_categories(df_top10, "Top 10 categorías por Revenue") if not err2 else empty_figure_with_message("Top 10 categorías por Revenue", err2)
    fig3 = figure_bottom_categories(df_bottom10, "Bottom 10 categorías por Revenue") if not err3 else empty_figure_with_message("Bottom 10 categorías por Revenue", err3)
    fig4 = figure_delivery_diff(df_deliv) if not err4 else empty_figure_with_message("Diferencia estimado vs real por estado", err4)
    fig5 = figure_real_vs_estimated(df_realest) if not err5 else empty_figure_with_message("Tiempo real vs estimado", err5)

# ------------------------------------------------------------------------------
# App (Bootstrap)
# ------------------------------------------------------------------------------
app = Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.FLATLY,  # tema bootstrap
        "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.css",
    ],
)
app.title = "Olist — Ingresos y Entregas"
server = app.server

# Navbar
navbar = dbc.Navbar(
    dbc.Container(
        [
            html.A(
                dbc.Row(
                    [
                        dbc.Col(html.I(className="bi bi-cart3 me-2", style={"fontSize": "1.25rem"})),
                        dbc.Col(dbc.NavbarBrand("Olist Dashboard", className="ms-2")),
                    ],
                    align="center",
                    className="g-0",
                ),
                href="#",
                style={"textDecoration": "none"},
            ),
            dbc.NavbarToggler(id="navbar-toggle"),
        ]
    ),
    color="primary",
    dark=True,
    class_name="mb-3 shadow-sm",
)

# Cards helper
def graph_card(title: str, figure: go.Figure, icon: str = "bi-bar-chart"):
    return dbc.Card(
        [
            dbc.CardHeader(
                html.Span([html.I(className=f"{icon} me-2"), title]),
                className="fw-semibold",
            ),
            dbc.CardBody(dcc.Graph(figure=figure, config={"displayModeBar": False})),
        ],
        class_name="shadow-sm h-100",
    )

# Layout con grid Bootstrap
app.layout = dbc.Container(
    [
        navbar,
        dbc.Row(
            dbc.Col(
                html.P(
                    "Análisis ejecutivo de ingresos por categoría/estado y desempeño de entregas (2016–2018).",
                    className="text-muted"
                ),
                width=12,
            ),
            class_name="mb-2",
        ),
        # Fila 1: heatmap ancho
        dbc.Row(
            [
                dbc.Col(graph_card("Revenue por mes (2016–2018)", fig1, "bi-grid-1x2"), width=12),
            ],
            class_name="mb-3",
        ),
        # Fila 2: Top / Bottom categorías
        dbc.Row(
            [
                dbc.Col(graph_card("Top 10 categorías por Revenue", fig2, "bi-trophy"), width=6),
                dbc.Col(graph_card("Bottom 10 categorías por Revenue", fig3, "bi-arrow-down"), width=6),
            ],
            class_name="mb-3",
        ),
        # Fila 3: Entregas (dos gráficos)
        dbc.Row(
            [
                dbc.Col(graph_card("Diferencia estimado vs real por estado", fig4, "bi-truck"), width=6),
                dbc.Col(graph_card("Tiempo de entrega: Real vs Estimado", fig5, "bi-stopwatch"), width=6),
            ],
            class_name="mb-4",
        ),
        dbc.Row(
            dbc.Col(
                html.Footer(
                    html.Small("Fuente: Olist (SQLite) · Dashboard estático — regenerar DB para actualizar."),
                    className="text-muted",
                ),
                width=12,
            )
        ),
    ],
    fluid=True,
    class_name="py-2",
)

if __name__ == "__main__":
    app.run_server(debug=True)
