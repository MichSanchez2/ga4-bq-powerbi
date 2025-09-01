from fastapi import FastAPI, HTTPException, Query
from datetime import date, timedelta
import os

# GA4 -> BigQuery (tu pipeline real)
from ga4_to_bq import run_range

# --- DIAGNÓSTICO GA4 (API) ---
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange, Dimension, Metric, RunReportRequest
)

# --- DIAGNÓSTICO BQ ---
from google.cloud import bigquery


app = FastAPI(title="GA4 → BigQuery Ingest", version="1.0.0")


@app.get("/")
def root():
    """Ping simple para ver endpoints disponibles."""
    return {
        "ok": True,
        "endpoints": ["/health", "/ingest", "/debug_ga", "/debug_bq", "/docs"]
    }


@app.get("/health")
def health():
    """Healthcheck para Render."""
    return {"status": "ok"}


def _parse_date(x: str) -> date:
    """Acepta 'yesterday' o YYYY-MM-DD."""
    if x == "yesterday":
        return date.today() - timedelta(days=1)
    return date.fromisoformat(x)


@app.api_route("/ingest", methods=["GET", "POST"])
def ingest(
    start: str = Query("yesterday", description="YYYY-MM-DD o 'yesterday'"),
    end: str = Query("yesterday", description="YYYY-MM-DD o 'yesterday'"),
    token: str = Query("", description="Debe coincidir con INGEST_TOKEN"),
):
    """
    Dispara la ingesta GA4 -> BigQuery para el rango [start, end].
    Se protege con el token definido en la variable de entorno INGEST_TOKEN.
    """
    expected = os.getenv("INGEST_TOKEN", "")
    if expected and token != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        s = _parse_date(start)
        e = _parse_date(end)
    except ValueError:
        raise HTTPException(status_code=400, detail="Usa 'yesterday' o fecha YYYY-MM-DD")

    if s > e:
        raise HTTPException(status_code=400, detail="start debe ser <= end")

    try:
        run_range(s, e)
    except Exception as exc:
        # mensaje breve para logs
        raise HTTPException(status_code=500, detail=f"ingest failed: {exc.__class__.__name__}") from exc

    return {"ok": True, "start": s.isoformat(), "end": e.isoformat()}


# =========================
#   ENDPOINT /debug_ga
# =========================
@app.get("/debug_ga")
def debug_ga(
    d1: str = Query("2024-07-15", description="Fecha 1 (YYYY-MM-DD)"),
    d2: str = Query("2025-07-15", description="Fecha 2 (YYYY-MM-DD)"),
):
    """
    Comprueba contra la GA4 Data API usando el property_id del backend.
    Devuelve rowCount para dos fechas para comparar 2024 vs 2025.
    """
    property_id = os.getenv("GA4_PROPERTY_ID")
    if not property_id:
        raise HTTPException(status_code=500, detail="GA4_PROPERTY_ID no está definido")

    client = BetaAnalyticsDataClient()

    def probe(date_str: str):
        req = RunReportRequest(
            property=f"properties/{property_id}",
            date_ranges=[DateRange(start_date=date_str, end_date=date_str)],
            dimensions=[Dimension(name="date")],
            metrics=[Metric(name="sessions")],
        )
        resp = client.run_report(req)
        return {
            "date": date_str,
            "rowCount": resp.row_count,
            "sampleRow": [c.value for c in resp.rows[0].dimension_values] if resp.row_count else [],
        }

    r1 = probe(d1)
    r2 = probe(d2)

    # Muestra también algunas variables no sensibles
    cfg = {
        "property_id": property_id,
        "gcp_project": os.getenv("GCP_PROJECT"),
        "bq_dataset": os.getenv("BQ_DATASET"),
        "bq_table_raw": os.getenv("BQ_TABLE_RAW"),
        # No exponemos secretos.
    }
    return {"config": cfg, "checks": [r1, r2]}


# =========================
#   ENDPOINT /debug_bq
# =========================
@app.get("/debug_bq")
def debug_bq():
    """
    Verifica que la tabla de destino exista y devuelve MIN/MAX/COUNT.
    """
    project = os.getenv("GCP_PROJECT")
    dataset = os.getenv("BQ_DATASET")
    table = os.getenv("BQ_TABLE_RAW")
    if not (project and dataset and table):
        raise HTTPException(
            status_code=500,
            detail="Faltan GCP_PROJECT / BQ_DATASET / BQ_TABLE_RAW en variables de entorno",
        )

    client = bigquery.Client(project=project)
    table_id = f"{project}.{dataset}.{table}"
    sql = f"""
        SELECT
          MIN(eventDate) AS min_date,
          MAX(eventDate) AS max_date,
          COUNT(*)       AS total_rows
        FROM `{table_id}`
    """
    job = client.query(sql)
    row = list(job.result())[0]
    return {
        "table": table_id,
        "min_date": row["min_date"],
        "max_date": row["max_date"],
        "total_rows": row["total_rows"],
    }
