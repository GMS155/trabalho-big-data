"""
Batch query functions executed by FastAPI route handlers.

All functions read from the processed Parquet sink written by the streaming
job.  When no processed data exists yet they fall back to the raw CSV stored
in HDFS.

Dataset columns:
  DayNum, VehId, Trip, Timestamp(ms), Latitude[deg], Longitude[deg],
  Vehicle Speed[km/h], MAF[g/sec], Engine RPM[RPM], Absolute Load[%]
"""

import math
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

HDFS_RAW       = "hdfs://namenode:9000/vehicle/raw/vehicle_data.csv"
HDFS_PROCESSED = "hdfs://namenode:9000/processed"

# Column names
COL_SPEED = "Vehicle Speed[km/h]"
COL_MAF   = "MAF[g/sec]"
COL_RPM   = "Engine RPM[RPM]"
COL_LOAD  = "Absolute Load[%]"
COL_LAT   = "Latitude[deg]"
COL_LON   = "Longitude[deg]"
COL_TS    = "Timestamp(ms)"
COL_VEH   = "VehId"
COL_TRIP  = "Trip"
COL_DAY   = "DayNum"

# Fuel estimation constants (gasoline)
AIR_FUEL_RATIO   = 14.7   # stoichiometric AFR
FUEL_DENSITY_G_L = 740.0  # g/L
SAMPLE_INTERVAL_S = 1.0   # assumed OBD sampling period in seconds


def _to_records(pdf) -> list:
    """Convert a pandas DataFrame to a list of dicts, replacing NaN/Inf with None."""
    records = pdf.to_dict(orient="records")
    return [
        {k: (None if isinstance(v, float) and not math.isfinite(v) else v)
         for k, v in row.items()}
        for row in records
    ]


def _read(spark: SparkSession):
    """Return a DataFrame from processed Parquet, falling back to raw CSV."""
    try:
        df = spark.read.parquet(HDFS_PROCESSED)
        if not df.take(1):
            raise ValueError("processed dir is empty")
        return df
    except Exception:
        pass

    try:
        return (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(HDFS_RAW)
        )
    except Exception as exc:
        raise RuntimeError(
            f"No data available"
        ) from exc


# ── Vehicles & trips ──────────────────────────────────────────────────────────

def get_vehicle_last_position(spark: SparkSession, veh_id: int) -> dict | None:
    row = (
        _read(spark)
        .filter(
            (F.col(COL_VEH) == veh_id)
            & F.col(COL_LAT).isNotNull()
            & F.col(COL_LON).isNotNull()
        )
        .orderBy(F.desc(COL_TS))
        .select(COL_TS, COL_LAT, COL_LON, COL_SPEED)
        .limit(1)
        .first()
    )
    if not row:
        return None
    return {
        "lat": row[COL_LAT],
        "lon": row[COL_LON],
        "speed": row[COL_SPEED],
        "timestamp": row[COL_TS],
    }


def get_vehicle_summary(spark: SparkSession, veh_id: int) -> dict | None:
    df = _read(spark).filter(F.col(COL_VEH) == veh_id)
    factor = SAMPLE_INTERVAL_S / AIR_FUEL_RATIO / FUEL_DENSITY_G_L

    base = df.agg(
        F.countDistinct(COL_TRIP).alias("total_trips"),
        F.round(F.avg(F.when(F.col(COL_SPEED).isNotNull(), F.col(COL_SPEED))), 2).alias("avg_speed"),
        F.round(F.avg(F.when(F.col(COL_RPM).isNotNull(), F.col(COL_RPM))), 1).alias("avg_rpm"),
        F.round(
            F.sum(F.when(F.col(COL_MAF).isNotNull() & (F.col(COL_MAF) >= 0), F.col(COL_MAF))) * factor,
            4,
        ).alias("estimated_fuel"),
        # Anomaly counts per physical rule
        F.count(F.when(
            F.col(COL_RPM).isNotNull() & (F.col(COL_RPM) > 6000)
            & F.col(COL_SPEED).isNotNull() & (F.col(COL_SPEED) == 0), True
        )).alias("a_high_rpm_stopped"),
        F.count(F.when(
            F.col(COL_SPEED).isNotNull() & (F.col(COL_SPEED) > 0) & F.col(COL_RPM).isNull(), True
        )).alias("a_speed_without_rpm"),
        F.count(F.when(
            F.col(COL_SPEED).isNotNull() & (F.col(COL_SPEED) > 0)
            & F.col(COL_RPM).isNotNull() & (F.col(COL_RPM) == 0), True
        )).alias("a_speed_with_zero_rpm"),
        F.count(F.when(F.col(COL_MAF).isNull(), True)).alias("a_missing_maf"),
    ).first()

    if not base or base["total_trips"] is None or int(base["total_trips"]) == 0:
        return None

    return {
        "veh_id": veh_id,
        "total_trips": int(base["total_trips"]),
        "avg_speed": base["avg_speed"],
        "avg_rpm": base["avg_rpm"],
        "estimated_fuel": base["estimated_fuel"],
        "anomalies": int(
            base["a_high_rpm_stopped"]
            + base["a_speed_without_rpm"]
            + base["a_speed_with_zero_rpm"]
            + base["a_missing_maf"]
        ),
    }


def get_summary(
    spark: SparkSession,
    speeding_threshold: float = 80.0
) -> dict:
    df = _read(spark)

    # Single pass for base aggregations + stats needed for anomaly detection
    base = df.agg(
        F.countDistinct(COL_VEH).alias("total_vehicles"),
        F.countDistinct(COL_TRIP).alias("total_trips"),
        F.round(F.avg(F.when(F.col(COL_SPEED).isNotNull(), F.col(COL_SPEED))), 2).alias("avg_speed"),
        F.round(
            F.sum(F.when(F.col(COL_MAF).isNotNull() & (F.col(COL_MAF) >= 0), F.col(COL_MAF)))
            * (SAMPLE_INTERVAL_S / AIR_FUEL_RATIO / FUEL_DENSITY_G_L),
            4,
        ).alias("total_fuel_estimated")
    ).first()

    # Top speeding vehicle
    top_row = (
        df.filter(F.col(COL_SPEED) > speeding_threshold)
        .groupBy(COL_VEH)
        .agg(F.count("*").alias("events"))
        .orderBy(F.desc("events"))
        .limit(1)
        .first()
    )
    top_speeding = (
        {"veh_id": int(top_row[COL_VEH]), "events": int(top_row["events"])}
        if top_row else None
    )

    return {
        "total_vehicles": int(base["total_vehicles"]),
        "total_trips": int(base["total_trips"]),
        "avg_speed": base["avg_speed"],
        "total_fuel_estimated": base["total_fuel_estimated"],
        "top_speeding_vehicle": top_speeding,
    }


def get_trip_timeline(spark: SparkSession, trip_id: float, limit: int = 500) -> dict:
    timestamps, speeds, rpms, mafs = [], [], [], []
    for row in (
        _read(spark)
        .filter(F.col(COL_TRIP) == trip_id)
        .select(COL_TS, COL_SPEED, COL_RPM, COL_MAF)
        .orderBy(COL_TS)
        .limit(limit)
        .toLocalIterator()
    ):
        timestamps.append(row[COL_TS])
        speeds.append(row[COL_SPEED])
        rpms.append(row[COL_RPM])
        mafs.append(row[COL_MAF])
    return {
        "count": len(timestamps),
        "timestamps": timestamps,
        "speed_kmh": speeds,
        "rpm": rpms,
        "maf_g_per_s": mafs,
    }


def get_trip_summary(spark: SparkSession, trip_id: float) -> dict | None:
    df = _read(spark).filter(F.col(COL_TRIP) == trip_id)

    w = Window.partitionBy(COL_TRIP).orderBy(COL_TS)
    R = 6371.0  # Earth radius in km

    enriched = (
        df
        .withColumn("prev_lat", F.lag(COL_LAT).over(w))
        .withColumn("prev_lon", F.lag(COL_LON).over(w))
        .withColumn("segment_km", F.when(
            F.col("prev_lat").isNotNull() & F.col("prev_lon").isNotNull()
            & F.col(COL_LAT).isNotNull() & F.col(COL_LON).isNotNull(),
            F.lit(2.0 * R) * F.asin(F.sqrt(F.least(
                F.lit(1.0),
                F.pow(F.sin(F.radians(F.col(COL_LAT) - F.col("prev_lat")) / 2), 2)
                + F.cos(F.radians(F.col("prev_lat")))
                * F.cos(F.radians(F.col(COL_LAT)))
                * F.pow(F.sin(F.radians(F.col(COL_LON) - F.col("prev_lon")) / 2), 2),
            )))
        ))
    )

    row = enriched.agg(
        F.count("*").alias("record_count"),
        F.first(COL_VEH).alias("veh_id"),
        F.min(COL_TS).alias("start_time"),
        F.max(COL_TS).alias("end_time"),
        F.round(F.avg(F.when(F.col(COL_SPEED).isNotNull(), F.col(COL_SPEED))), 2).alias("avg_speed"),
        F.round(F.sum("segment_km"), 3).alias("distance_km"),
    ).first()

    if not row or not row["record_count"]:
        return None

    return {
        "trip_id": trip_id,
        "veh_id": int(row["veh_id"]),
        "start_time": row["start_time"],
        "end_time": row["end_time"],
        "distance_km": row["distance_km"],
        "avg_speed": row["avg_speed"],
    }


# ── 1. Excesso de velocidade (> threshold km/h) ─────────────────────────────

def speeding_events(
    spark: SparkSession,
    threshold: float = 80.0,
    veh_id: int | None = None,
    day_min: int | None = None,
    day_max: int | None = None,
    limit: int = 200,
) -> dict:
    df = _read(spark)
    df = df.filter(F.col(COL_SPEED) > threshold)
    if veh_id is not None:
        df = df.filter(F.col(COL_VEH) == veh_id)
    if day_min is not None:
        df = df.filter(F.col(COL_DAY) >= day_min)
    if day_max is not None:
        df = df.filter(F.col(COL_DAY) <= day_max)
    events = [
        row.asDict()
        for row in (
            df.groupBy(COL_VEH, COL_TRIP, COL_DAY)
            .agg(
                F.count("*").alias("speeding_records"),
                F.round(F.max(COL_SPEED), 2).alias("max_speed_kmh"),
                F.round(F.avg(COL_SPEED), 2).alias("avg_speed_kmh"),
            )
            .orderBy(F.desc("speeding_records"))
            .limit(limit)
            .toLocalIterator()
        )
    ]
    return {
        "threshold_kmh": threshold,
        "filters": {"veh_id": veh_id, "day_min": day_min, "day_max": day_max},
        "count": len(events),
        "events": events,
    }


# ── 2. Alta rotação (condução agressiva) ─────────────────────────────────────

def high_rpm_driving(
    spark: SparkSession,
    rpm_threshold: float = 3500.0,
    veh_id: int | None = None,
    limit: int = 200,
) -> dict:
    df = _read(spark).filter(
        F.col(COL_RPM).isNotNull()
        & (F.col(COL_RPM) > rpm_threshold)
        & F.col(COL_SPEED).isNotNull()
        & (F.col(COL_SPEED) > 0)
    )
    if veh_id is not None:
        df = df.filter(F.col(COL_VEH) == veh_id)
    events = [
        row.asDict()
        for row in (
            df.groupBy(COL_VEH, COL_TRIP)
            .agg(
                F.count("*").alias("high_rpm_samples"),
                F.round(F.max(COL_RPM), 1).alias("max_rpm"),
                F.round(F.avg(COL_RPM), 1).alias("avg_rpm"),
                F.round(F.avg(COL_SPEED), 2).alias("avg_speed_kmh"),
            )
            .orderBy(F.desc("high_rpm_samples"))
            .limit(limit)
            .toLocalIterator()
        )
    ]
    return {
        "rpm_threshold": rpm_threshold,
        "filters": {"veh_id": veh_id},
        "count": len(events),
        "events": events,
    }
