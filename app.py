from fastapi import FastAPI, HTTPException, Query
from datetime import date, timedelta
import os

# Importa la función que hace la ingesta GA4 -> BigQuery
from ga4_to_bq import run_range

app = FastAPI(title="GA4 → BigQuery Ingest", version="1.0.0")


@app.get("/")
def root():
    """Ping simple para ver endpoints disponibles."""
    return {"ok": True, "endpoints": ["/health", "/ingest", "/docs"]}


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
