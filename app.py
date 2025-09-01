from fastapi import FastAPI, HTTPException, Query
from datetime import date, timedelta
import os
from ga4_to_bq import run_range

app = FastAPI(title="GA4 → BigQuery Ingest")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/ingest")
def ingest(start: str = Query("yesterday"), end: str = Query("yesterday"), token: str = Query("")):
    expected = os.getenv("INGEST_TOKEN", "")
    if expected and token != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")
    def parse_one(x):
        if x == "yesterday": return date.today() - timedelta(days=1)
        return date.fromisoformat(x)
    s = parse_one(start)
    e = parse_one(end)
    run_range(s, e)
    return {"ok": True, "start": s.isoformat(), "end": e.isoformat()}
