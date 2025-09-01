#!/usr/bin/env python3
import os, sys, time, argparse
from datetime import date, datetime, timedelta
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric
from google.cloud import bigquery

# --------- ENV / Defaults ----------
GA4_PROPERTY_ID = os.getenv("GA4_PROPERTY_ID", "").strip()
GCP_PROJECT = os.getenv("GCP_PROJECT", "").strip()
BQ_DATASET = os.getenv("BQ_DATASET", "analytics_app").strip()
BQ_TABLE_RAW = os.getenv("BQ_TABLE_RAW", "events_daily_raw").strip()
GA4_CREDENTIALS_PATH = os.getenv("GA4_CREDENTIALS_PATH", "/etc/secrets/ga4-credentials.json")

# Para Render Secret Files
if GA4_CREDENTIALS_PATH and os.path.exists(GA4_CREDENTIALS_PATH):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GA4_CREDENTIALS_PATH

if not GA4_PROPERTY_ID or not GCP_PROJECT:
    print("ERROR: set GA4_PROPERTY_ID and GCP_PROJECT env vars", file=sys.stderr)
    sys.exit(2)

DIMENSIONS = ["date","country","sessionDefaultChannelGroup","sourceMedium","eventName"]
METRICS    = ["totalUsers","activeUsers","sessions","conversions","purchaseRevenue"]
LIMIT = 100000  # GA4 Data API usa offset/limit

bq = bigquery.Client(project=GCP_PROJECT)
ga = BetaAnalyticsDataClient()
table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE_RAW}"

def ensure_bq():
    dataset_ref = bigquery.Dataset(f"{GCP_PROJECT}.{BQ_DATASET}")
    try:
        bq.get_dataset(dataset_ref)
    except Exception:
        bq.create_dataset(dataset_ref, exists_ok=True)
    schema=[bigquery.SchemaField("eventDate","DATE",mode="REQUIRED"),
            bigquery.SchemaField("country","STRING"),
            bigquery.SchemaField("sessionDefaultChannelGroup","STRING"),
            bigquery.SchemaField("sourceMedium","STRING"),
            bigquery.SchemaField("eventName","STRING"),
            bigquery.SchemaField("totalUsers","INTEGER"),
            bigquery.SchemaField("activeUsers","INTEGER"),
            bigquery.SchemaField("sessions","INTEGER"),
            bigquery.SchemaField("conversions","INTEGER"),
            bigquery.SchemaField("purchaseRevenue","FLOAT"),
            bigquery.SchemaField("_ingested_at","TIMESTAMP")]
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(field="eventDate")
    try:
        bq.get_table(table_id)
    except Exception:
        bq.create_table(table)

def run_day(d_iso: str):
    rows_out=[]; offset=0; total=None
    while True:
        req = RunReportRequest(
            property=f"properties/{GA4_PROPERTY_ID}" if not GA4_PROPERTY_ID.startswith("properties/") else GA4_PROPERTY_ID,
            dimensions=[Dimension(name=x) for x in DIMENSIONS],
            metrics=[Metric(name=x) for x in METRICS],
            date_ranges=[DateRange(start_date=d_iso, end_date=d_iso)],
            limit=LIMIT,
            offset=offset
        )
        resp = ga.run_report(req)
        if total is None:
            total = resp.row_count

        for r in resp.rows:
            dv = {DIMENSIONS[i]: r.dimension_values[i].value for i in range(len(DIMENSIONS))}
            mv = {METRICS[i]   : float(r.metric_values[i].value or 0) for i in range(len(METRICS))}
            rows_out.append({
                "eventDate": d_iso,
                "country": dv.get("country"),
                "sessionDefaultChannelGroup": dv.get("sessionDefaultChannelGroup"),
                "sourceMedium": dv.get("sourceMedium"),
                "eventName": dv.get("eventName"),
                "totalUsers": int(mv.get("totalUsers",0)),
                "activeUsers": int(mv.get("activeUsers",0)),
                "sessions": int(mv.get("sessions",0)),
                "conversions": int(mv.get("conversions",0)),
                "purchaseRevenue": float(mv.get("purchaseRevenue",0.0)),
                "_ingested_at": datetime.utcnow().isoformat()
            })

        fetched = offset + len(resp.rows)
        if fetched >= resp.row_count or len(resp.rows)==0:
            break
        offset += len(resp.rows)
        time.sleep(0.5)

    if rows_out:
        bq.load_table_from_json(rows_out, table_id).result()

def run_range(start_date: date, end_date: date):
    ensure_bq()
    d = start_date
    while d <= end_date:
        print(f"Ingesting {d} ...")
        run_day(d.isoformat())
        d += timedelta(days=1)
    print("DONE")

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--start", help="YYYY-MM-DD o 'yesterday'", default="yesterday")
    p.add_argument("--end",   help="YYYY-MM-DD o 'yesterday'", default="yesterday")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    def parse_one(x):
        if x == "yesterday": return date.today() - timedelta(days=1)
        return date.fromisoformat(x)
    s = parse_one(args.start)
    e = parse_one(args.end)
    run_range(s, e)
