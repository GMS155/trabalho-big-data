"""
Endpoints
─────────
GET /health                     → liveness probe
GET /data                       → paginated raw records

GET /vehicles/{veh_id}/summary → per-vehicle summary (trips, speed, rpm, fuel, anomalies)
GET /vehicles/{veh_id}/last-position → latest GPS position, speed and timestamp
GET /trips/{trip_id}            → trip summary (veh, timestamps, distance, avg speed)
GET /trips/{trip_id}/timeline  → time-series (timestamp, speed, rpm, maf)
GET /stats/summary              → summary (vehicles, trips, avg speed, fuel, top speeding vehicle)
GET /speeding                     → speeding events with filters (veh_id, day range, speed threshold)
GET /analytics/high-rpm             → aggressive driving events (sustained high RPM while moving)
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


# ── Vehicles ──────────────────────────────────────────────────────────────────


@router.get("/vehicles/{veh_id}/summary", tags=["vehicles"])
def get_vehicle_summary(veh_id: int, spark=Depends(_require_spark)):
    """Per-vehicle summary: total trips, avg speed, avg RPM, estimated fuel and anomaly count."""
    result = jobs.get_vehicle_summary(spark, veh_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Vehicle {veh_id} not found.")
    return result


@router.get("/vehicles/{veh_id}/last-position", tags=["vehicles"])
def get_vehicle_last_position(veh_id: int, spark=Depends(_require_spark)):
    """Return the latest GPS position, speed and timestamp for a vehicle."""
    result = jobs.get_vehicle_last_position(spark, veh_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Vehicle {veh_id} not found or has no GPS data.")
    return {"veh_id": veh_id, **result}


@router.get("/trips/{trip_id}", tags=["vehicles"])
def get_trip(trip_id: float, spark=Depends(_require_spark)):
    """Trip summary: vehicle, start/end timestamps, GPS distance and avg speed."""
    result = jobs.get_trip_summary(spark, trip_id=trip_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Trip {trip_id} not found.")
    return result


@router.get("/trips/{trip_id}/timeline", tags=["vehicles"])
def get_trip_timeline(
    trip_id: float,
    spark=Depends(_require_spark),
    limit: int = Query(default=500, ge=1, le=5000),
):
    """Time-series of speed, RPM and MAF for a trip, ordered by timestamp."""
    result = jobs.get_trip_timeline(spark, trip_id=trip_id, limit=limit)
    if not result["count"]:
        raise HTTPException(status_code=404, detail=f"Trip {trip_id} not found.")
    return {"trip_id": trip_id, **result}


# ── Dashboard summary ─────────────────────────────────────────────────────────

@router.get("/stats/summary", tags=["stats"])
def get_summary(
    spark=Depends(_require_spark),
    speeding_threshold: float = Query(default=80.0, ge=0, description="Speed limit in km/h for speeding detection"),
):
    """
    Summary combining vehicles, trips, avg speed, estimated fuel consumption,
    and the top speeding vehicle.
    """
    return jobs.get_summary(spark, speeding_threshold=speeding_threshold)


# ── 1. Excesso de velocidade ──────────────────────────────────────────────────

@router.get("/speeding", tags=["analytics"])
def speeding(
    spark=Depends(_require_spark),
    threshold: float = Query(default=80.0, ge=0, description="Speed limit in km/h"),
    veh_id: int | None = Query(default=None, description="Filter by vehicle ID"),
    day_min: int | None = Query(default=None, description="First day (DayNum) of the period"),
    day_max: int | None = Query(default=None, description="Last day (DayNum) of the period"),
    limit: int = Query(default=200, ge=1, le=1000),
):
    """Speeding events filtered by vehicle, day range and speed threshold."""
    return jobs.speeding_events(
        spark,
        threshold=threshold,
        veh_id=veh_id,
        day_min=day_min,
        day_max=day_max,
        limit=limit,
    )


# ── 2. Alta rotação ───────────────────────────────────────────────────────────

@router.get("/analytics/high-rpm", tags=["analytics"])
def high_rpm(
    spark=Depends(_require_spark),
    rpm_threshold: float = Query(default=3500.0, ge=0, description="RPM threshold for aggressive driving"),
    veh_id: int | None = Query(default=None, description="Filter by vehicle ID"),
    limit: int = Query(default=200, ge=1, le=1000),
):
    """Detects aggressive driving: trips/vehicles with sustained high RPM while moving."""
    return jobs.high_rpm_driving(spark, rpm_threshold=rpm_threshold, veh_id=veh_id, limit=limit)


