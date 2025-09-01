# ga4_to_bq.py
from __future__ import annotations
from datetime import date, timedelta, datetime, timezone
import os
from typing import List, Dict, Any

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.oauth2 import service_account
from google.cloud import bigquery

# ----------------------------- Config -----------------------------
GA4_PROPERTY_ID = os.environ.get("GA4_PROPERTY_ID") or os.environ.get("ga4_property_id")
GCP_PROJECT     = os.environ.get("GCP_PROJECT")
BQ_DATASET      = os.environ.get("BQ_DATASET", "analytics_app")
BQ_TABLE_RAW    = os.environ.get("BQ_TABLE_RAW", "events_daily_raw")
CREDENTIALS_PATH= os.environ.get("GA4_CREDENTIALS_PATH") or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

TABLE_ID = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE_RAW}"

assert GA4_PROPERTY_ID,  "Falta GA4_PROPERTY_ID"
assert GCP_PROJECT,      "Falta GCP_PROJECT"

# ------------------------- Clients -------------------------------
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
    """Create dataset/table if not exist. Table is DATE-partitioned on eventDate."""
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
        bigquery.SchemaField("conversions", "FLOAT64"),
        bigquery.SchemaField("purchaseRevenue", "FLOAT64"),
        bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
    ]

    table_ref = dataset_ref.table(BQ_TABLE_RAW)
    try:
        _ = bq.get_table(table_ref)
        return
    except Exception:
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(field="eventDate")
        bq.create_table(table, exists_ok=True)

def _append_rows_partition(rows: List[Dict[str, Any]], d: date):
    """
    Load JSON into a single daily partition using WRITE_TRUNCATE (no DML).
    """
    if not rows:
        return

    bq = _bq_client()
    _ensure_table()

    schema = [
        bigquery.SchemaField("eventDate", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("sessionDefaultChannelGroup", "STRING"),
        bigquery.SchemaField("sourceMedium", "STRING"),
        bigquery.SchemaField("eventName", "STRING"),
        bigquery.SchemaField("totalUsers", "INT64"),
        bigquery.SchemaField("activeUsers", "INT64"),
        bigquery.SchemaField("sessions", "INT64"),
        bigquery.SchemaField("conversions", "FLOAT64"),
        bigquery.SchemaField("purchaseRevenue", "FLOAT64"),
        bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
    ]

    dest = f"{TABLE_ID}${d.strftime('%Y%m%d')}"
    job = bq.load_table_from_json(
        rows,
        dest,
        job_config=bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
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

def _to_int(x: Any) -> int:
    if x in (None, ""):
        return 0
    return int(float(x))

def _to_float(x: Any) -> float:
    if x in (None, ""):
        return 0.0
    return float(x)

def _fetch_day(d: date) -> List[Dict[str, Any]]:
    """Detailed per-day pull with country/channel/source/event."""
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
        ymd = dim[0]  # 'YYYYMMDD'
        rows.append({
            "eventDate": f"{ymd[0:4]}-{ymd[4:6]}-{ymd[6:8]}",
            "country": dim[1] or None,
            "sessionDefaultChannelGroup": dim[2] or None,
            "sourceMedium": dim[3] or None,
            "eventName": dim[4] or None,
            "totalUsers": _to_int(met[0]),
            "activeUsers": _to_int(met[1]),
            "sessions": _to_int(met[2]),
            "conversions": _to_float(met[3]),
            "purchaseRevenue": _to_float(met[4]),
            "_ingested_at": datetime.now(timezone.utc).isoformat(),
        })
    return rows

def _fetch_range_simple(start: date, end: date) -> List[Dict[str, Any]]:
    """Simple & fast: one call for [start,end], only date + metrics (good for 14m backfill)."""
    client = _ga_client()
    req = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        dimensions=[Dimension(name="date")],
        metrics=[Metric(name=m) for m in METRICS],
        date_ranges=[DateRange(start_date=start.isoformat(), end_date=end.isoformat())],
        limit=2000,
    )
    resp = client.run_report(req)
    rows: List[Dict[str, Any]] = []
    for r in resp.rows:
        ymd = r.dimension_values[0].value  # 'YYYYMMDD'
        mv = [v.value for v in r.metric_values]
        rows.append({
            "eventDate": f"{ymd[0:4]}-{ymd[4:6]}-{ymd[6:8]}",
            "country": None,
            "sessionDefaultChannelGroup": None,
            "sourceMedium": None,
            "eventName": None,
            "totalUsers": _to_int(mv[0]),
            "activeUsers": _to_int(mv[1]),
            "sessions": _to_int(mv[2]),
            "conversions": _to_float(mv[3]),
            "purchaseRevenue": _to_float(mv[4]),
            "_ingested_at": datetime.now(timezone.utc).isoformat(),
        })
    return rows

# ---------------------- Public API --------------------------------
def run_range(start: date, end: date, *, simple: bool = False):
    """
    Load [start, end] inclusive.
    - simple=False -> detailed day-by-day (uses DIMENSIONS)
    - simple=True  -> fast range by date only; still writes per-day partitions
    """
    _ensure_table()

    if simple:
        all_rows = _fetch_range_simple(start, end)
        for r in all_rows:
            d = date.fromisoformat(r["eventDate"])
            _append_rows_partition([r], d)
        return

    d = start
    one = timedelta(days=1)
    while d <= end:
        rows = _fetch_day(d)
        if rows:
            _append_rows_partition(rows, d)
        d += one

