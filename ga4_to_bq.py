# ga4_to_bq.py
from __future__ import annotations
from dataclasses import dataclass
from datetime import date, timedelta, datetime, timezone
import os
from typing import Iterable, List, Dict, Any

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange, Dimension, Metric, RunReportRequest
)
from google.oauth2 import service_account

from google.cloud import bigquery

# ----------------------------- Config -----------------------------

GA4_PROPERTY_ID = os.environ.get("GA4_PROPERTY_ID") or os.environ.get("GA4_PROPERTY_ID".lower())
GCP_PROJECT     = os.environ.get("GCP_PROJECT")
BQ_DATASET      = os.environ.get("BQ_DATASET", "analytics_app")
BQ_TABLE_RAW    = os.environ.get("BQ_TABLE_RAW", "events_daily_raw")
CREDENTIALS_PATH= os.environ.get("GA4_CREDENTIALS_PATH") or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

TABLE_ID = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE_RAW}"

assert GA4_PROPERTY_ID,  "Falta GA4_PROPERTY_ID"
assert GCP_PROJECT,      "Falta GCP_PROJECT"

# ------------------------- Clientes -------------------------------

def _ga_client() -> BetaAnalyticsDataClient:
    if CREDENTIALS_PATH:
        creds = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
        return BetaAnalyticsDataClient(credentials=creds)
    return BetaAnalyticsDataClient()

def _bq_client() -> bigquery.Client:
    if CREDENTIALS_PATH:
        creds = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
        return bigquery.Client(project=GCP_PROJECT, credentials=creds)
    return bigquery.Client(project=GCP_PROJECT)

# --------------------- BigQuery helpers ---------------------------

def _ensure_table():
    bq = _bq_client()
    dataset_ref = bigquery.DatasetReference(GCP_PROJECT, BQ_DATASET)
    try:
        bq.get_dataset(dataset_ref)
    except Exception:
        bq.create_dataset(bigquery.Dataset(dataset_ref), exists_ok=True)

    schema = [
        bigquery.SchemaField("eventDate", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("sessionDefaultChannelGroup", "STRING"),
        bigquery.SchemaField("sourceMedium", "STRING"),
        bigquery.SchemaField("eventName", "STRING"),
        bigquery.SchemaField("totalUsers", "INT64"),
        bigquery.SchemaField("activeUsers", "INT64"),
        bigquery.SchemaField("sessions", "INT64"),
        bigquery.SchemaField("conversions", "INT64"),
        bigquery.SchemaField("purchaseRevenue", "FLOAT"),
        bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
    ]

    table_ref = dataset_ref.table(BQ_TABLE_RAW)
    try:
        _ = bq.get_table(table_ref)
        return  # ya existe
    except Exception:
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(field="eventDate")
        bq.create_table(table, exists_ok=True)

def _delete_day_from_bq(d: date):
    """Backfill seguro: borra la partición del día antes de cargarla."""
    bq = _bq_client()
    sql = f"""
    DELETE FROM `{TABLE_ID}`
    WHERE eventDate = @d
    """
    job = bq.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("d", "DATE", d)]
        ),
    )
    job.result()

def _append_rows(rows: List[Dict[str, Any]]):
    if not rows:
        return
    bq = _bq_client()
    _ensure_table()
    job = bq.load_table_from_json(
        rows,
        TABLE_ID,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        ),
    )
    job.result()

# ---------------------- GA4 helpers --------------------------------

DIMENSIONS = [
    "date",
    "country",
    "sessionDefaultChannelGroup",
    "sourceMedium",
    "eventName",
]
METRICS = [
    "totalUsers",
    "activeUsers",
    "sessions",
    "conversions",
    "purchaseRevenue",
]

def _fetch_day(d: date) -> List[Dict[str, Any]]:
    client = _ga_client()
    req = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        dimensions=[Dimension(name=n) for n in DIMENSIONS],
        metrics=[Metric(name=n) for n in METRICS],
        date_ranges=[DateRange(start_date=d.isoformat(), end_date=d.isoformat())],
        limit=100000,
    )
    resp = client.run_report(req)
    rows: List[Dict[str, Any]] = []
    for r in resp.rows:
        dim = [v.value for v in r.dimension_values]
        met = [v.value for v in r.metric_values]
        # GA devuelve fecha como 'YYYYMMDD'
        ymd = dim[0]
        rows.append({
            "eventDate": f"{ymd[0:4]}-{ymd[4:6]}-{ymd[6:8]}",
            "country": dim[1] or None,
            "sessionDefaultChannelGroup": dim[2] or None,
            "sourceMedium": dim[3] or None,
            "eventName": dim[4] or None,
            "totalUsers": int(met[0] or 0),
            "activeUsers": int(met[1] or 0),
            "sessions": int(met[2] or 0),
            "conversions": int(met[3] or 0),
            "purchaseRevenue": float(met[4] or 0.0),
            "_ingested_at": datetime.now(timezone.utc),  # <-- FIX: timestamp válido
        })
    return rows

# ---------------------- API pública --------------------------------

def run_range(start: date, end: date):
    """
    Carga cada día del rango [start, end] (inclusive).
    **Backfill idempotente**: antes de cargar un día, se borra su partición.
    """
    _ensure_table()

    d = start
    one = timedelta(days=1)
    while d <= end:
        rows = _fetch_day(d)
        if rows:
            _delete_day_from_bq(d)   # evita duplicados al recargar un día
            _append_rows(rows)
        d += one
