"""
Endpoints
─────────
GET /health                     → liveness probe
GET /data                       → paginated raw records
GET /vehicles                   → list of vehicle IDs
GET /vehicles/{veh_id}/trips    → trips for a vehicle
GET /trips/{trip_id}            → telemetry records for a trip
GET /stats/speed                → global speed statistics

GET /analytics/speeding         → excesso de velocidade (> threshold km/h)
GET /analytics/routes           → rotas mais utilizadas (lat/lon grid)
GET /analytics/stops            → paradas longas (velocidade = 0)
GET /analytics/fuel             → consumo estimado via MAF
GET /analytics/rpm-ranking      → ranking de eficiência por RPM médio
GET /anomalies                  → detecção de anomalias (z-score RPM/MAF)
"""

from fastapi import APIRouter, Depends, Query, Request, HTTPException
from fastapi.responses import JSONResponse

from spark import jobs

router = APIRouter()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _require_spark(request: Request):
    """Dependency: raises 503 if SparkSession is not ready yet."""
    if not getattr(request.app.state, "spark_ready", False):
        raise HTTPException(status_code=503, detail="SparkSession is still initialising, please retry shortly.")
    return request.app.state.spark


# ── Health ────────────────────────────────────────────────────────────────────

@router.get("/health", tags=["health"])
def health_check(request: Request):
    ready = getattr(request.app.state, "spark_ready", False)
    if ready:
        return {"status": "healthy"}
    return JSONResponse(status_code=503, content={"status": "starting"})


# ── Raw data (paginated) ──────────────────────────────────────────────────────

@router.get("/data", tags=["data"])
def get_data(
    spark=Depends(_require_spark),
    limit:  int = Query(default=50,  ge=1, le=500),
    offset: int = Query(default=0,   ge=0),
):
    """Return paginated rows from the processed HDFS Parquet sink."""
    return jobs.get_records(spark, limit=limit, offset=offset)


# ── Vehicles ──────────────────────────────────────────────────────────────────

@router.get("/vehicles", tags=["vehicles"])
def list_vehicles(spark=Depends(_require_spark)):
    """Return the list of distinct vehicle IDs present in the dataset."""
    return {"vehicle_ids": jobs.list_vehicles(spark)}


@router.get("/vehicles/{veh_id}/trips", tags=["vehicles"])
def get_vehicle_trips(veh_id: int, spark=Depends(_require_spark)):
    """Return distinct trip IDs and day numbers for a given vehicle."""
    trips = jobs.get_vehicle_trips(spark, veh_id)
    if not trips:
        raise HTTPException(status_code=404, detail=f"Vehicle {veh_id} not found.")
    return {"veh_id": veh_id, "trips": trips}


@router.get("/trips/{trip_id}", tags=["vehicles"])
def get_trip(
    trip_id: float,
    spark=Depends(_require_spark),
    limit: int = Query(default=200, ge=1, le=1000),
):
    """Return telemetry records for a specific trip, ordered by timestamp."""
    records = jobs.get_trip_records(spark, trip_id=trip_id, limit=limit)
    if not records:
        raise HTTPException(status_code=404, detail=f"Trip {trip_id} not found.")
    return {"trip_id": trip_id, "count": len(records), "records": records}


# ── Speed stats ───────────────────────────────────────────────────────────────

@router.get("/stats/speed", tags=["stats"])
def speed_stats(spark=Depends(_require_spark)):
    """Global vehicle speed statistics (avg, min, max)."""
    return jobs.speed_stats(spark)


# ── 1. Excesso de velocidade ──────────────────────────────────────────────────

@router.get("/analytics/speeding", tags=["analytics"])
def speeding_events(
    spark=Depends(_require_spark),
    threshold: float = Query(default=80.0, ge=0, description="Speed limit in km/h"),
):
    """
    Returns vehicles/trips where speed exceeded the threshold,
    sorted by number of speeding records (descending).
    """
    results = jobs.speeding_events(spark, threshold=threshold)
    return {
        "threshold_kmh": threshold,
        "count": len(results),
        "results": results,
    }


# ── 2. Rotas mais utilizadas ──────────────────────────────────────────────────

@router.get("/analytics/routes", tags=["analytics"])
def top_routes(
    spark=Depends(_require_spark),
    precision: int = Query(default=2, ge=1, le=4, description="Decimal places for lat/lon grid"),
    limit:     int = Query(default=20, ge=1, le=100),
):
    """
    Groups GPS coordinates into a lat/lon grid and ranks zones by record count.
    Higher precision = smaller grid cells (more granular routes).
    """
    results = jobs.top_routes(spark, precision=precision, limit=limit)
    return {"precision": precision, "count": len(results), "zones": results}


# ── 3. Paradas longas ─────────────────────────────────────────────────────────

@router.get("/analytics/stops", tags=["analytics"])
def long_stops(
    spark=Depends(_require_spark),
    min_consecutive: int = Query(
        default=10, ge=1,
        description="Minimum consecutive zero-speed samples to qualify as a long stop",
    ),
):
    """
    Detects sequences of consecutive zero-speed records within each trip.
    Returns stop events sorted by number of stopped samples (longest first).
    """
    results = jobs.long_stops(spark, min_consecutive=min_consecutive)
    return {
        "min_consecutive_samples": min_consecutive,
        "count": len(results),
        "stops": results,
    }


# ── 4. Consumo estimado de combustível ───────────────────────────────────────

@router.get("/analytics/fuel", tags=["analytics"])
def fuel_consumption(spark=Depends(_require_spark)):
    """
    Estimates fuel consumption per vehicle/trip using the MAF sensor.

    Formula (gasoline, ~1 s OBD sampling):
      fuel_liters ≈ Σ(MAF g/s) / AFR(14.7) / density(740 g/L)
    """
    results = jobs.fuel_consumption(spark)
    return {"count": len(results), "results": results}


# ── 5. Ranking de eficiência por RPM ─────────────────────────────────────────

@router.get("/analytics/rpm-ranking", tags=["analytics"])
def rpm_efficiency_ranking(spark=Depends(_require_spark)):
    """
    Ranks vehicles by average RPM while moving (ascending).
    Lower avg RPM at similar speeds indicates more efficient driving style.
    """
    results = jobs.rpm_efficiency_ranking(spark)
    return {"count": len(results), "ranking": results}


# ── 6. Detecção de anomalias ─────────────────────────────────────────────────

@router.get("/anomalies", tags=["analytics"])
def anomalies(
    spark=Depends(_require_spark),
    limit: int = Query(default=50, ge=1, le=200),
):
    """
    Detects records where RPM or MAF deviates more than 3 standard deviations
    from the mean (z-score > 3). Results include rpm_z and maf_z scores.
    """
    records = jobs.detect_anomalies(spark, limit=limit)
    return {"count": len(records), "records": records}
