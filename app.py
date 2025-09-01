# app.py
from fastapi import FastAPI, HTTPException, Query
from datetime import date, timedelta
import os

from ga4_to_bq import run_range
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.cloud import bigquery

app = FastAPI(title="GA4 → BigQuery Ingest", version="1.1.0")

@app.get("/")
def root():
    return {"ok": True, "endpoints": ["/health", "/ingest", "/backfill", "/debug_ga", "/debug_bq"]}

@app.get("/health")
def health():
    return {"status": "ok"}

def _parse_date(x: str) -> date:
    if x == "yesterday":
        return date.today() - timedelta(days=1)
    return date.fromisoformat(x)

# ---------------- Ingest / Backfill ----------------
@app.api_route("/ingest", methods=["GET", "POST"])
def ingest(
    start: str = Query("yesterday"),
    end: str = Query("yesterday"),
    token: str = Query(""),
    simple: int = Query(0, description="1=simple (solo fecha+metricas)"),
):
    expected = os.getenv("INGEST_TOKEN", "")
    if expected and token != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")
    s = _parse_date(start); e = _parse_date(end)
    if s > e:
        raise HTTPException(status_code=400, detail="start <= end")
    run_range(s, e, simple=bool(simple))
    return {"ok": True, "start": s.isoformat(), "end": e.isoformat(), "simple": bool(simple)}

@app.api_route("/backfill", methods=["GET", "POST"])
def backfill(
    start: str = Query(...),
    end: str = Query(...),
    token: str = Query(""),
    simple: int = Query(1, description="1=simple (recomendado para histórico)"),
):
    expected = os.getenv("INGEST_TOKEN", "")
    if expected and token != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")
    s = _parse_date(start); e = _parse_date(end)
    if s > e:
        raise HTTPException(status_code=400, detail="start <= end")
    run_range(s, e, simple=bool(simple))
    return {"ok": True, "reloaded": {"start": s.isoformat(), "end": e.isoformat()}, "simple": bool(simple)}

# ---------------- Debug helpers ----------------
@app.get("/debug_ga")
def debug_ga(
    d1: str = Query("2024-07-15"), d2: str = Query("2025-07-15")
):
    pid = os.getenv("GA4_PROPERTY_ID")
    if not pid:
        raise HTTPException(status_code=500, detail="GA4_PROPERTY_ID not set")
    client = BetaAnalyticsDataClient()

    def probe(ds: str):
        req = RunReportRequest(
            property=f"properties/{pid}",
            date_ranges=[DateRange(start_date=ds, end_date=ds)],
            dimensions=[Dimension(name="date")],
            metrics=[Metric(name="sessions")],
        )
        resp = client.run_report(req)
        return {"date": ds, "rowCount": resp.row_count,
                "sampleRow": [c.value for c in resp.rows[0].dimension_values] if resp.row_count else []}

    return {"config": {
                "property_id": pid,
                "gcp_project": os.getenv("GCP_PROJECT"),
                "bq_dataset": os.getenv("BQ_DATASET"),
                "bq_table_raw": os.getenv("BQ_TABLE_RAW"),
            },
            "checks": [probe(d1), probe(d2)]}

@app.get("/debug_bq")
def debug_bq():
    project = os.getenv("GCP_PROJECT")
    dataset = os.getenv("BQ_DATASET")
    table = os.getenv("BQ_TABLE_RAW")
    if not (project and dataset and table):
        raise HTTPException(status_code=500, detail="Missing GCP_PROJECT/BQ_DATASET/BQ_TABLE_RAW")
    client = bigquery.Client(project=project)
    sql = f"""
        SELECT MIN(eventDate) AS min_date,
               MAX(eventDate) AS max_date,
               COUNT(*)       AS total_rows
        FROM `{project}.{dataset}.{table}`
    """
    row = list(client.query(sql).result())[0]
    return {"table": f"{project}.{dataset}.{table}",
            "min_date": row["min_date"], "max_date": row["max_date"], "total_rows": row["total_rows"]}
