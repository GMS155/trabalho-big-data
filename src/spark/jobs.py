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
        if df.rdd.isEmpty():
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
            f"No data available. Processed path '{HDFS_PROCESSED}' and raw CSV "
            f"'{HDFS_RAW}' are both inaccessible. "
            "Ensure hdfs-init has completed successfully."
        ) from exc


# ── Generic ───────────────────────────────────────────────────────────────────

def get_records(spark: SparkSession, limit: int = 50, offset: int = 0) -> list:
    df = _read(spark)
    return _to_records(
        df.limit(offset + limit)
        .toPandas()
        .iloc[offset:offset + limit]
    )


# ── Vehicles & trips ──────────────────────────────────────────────────────────

def list_vehicles(spark: SparkSession) -> list:
    df = _read(spark)
    return df.select(COL_VEH).distinct().orderBy(COL_VEH).toPandas()[COL_VEH].astype(int).tolist()


def get_vehicle_trips(spark: SparkSession, veh_id: int) -> list:
    df = _read(spark)
    return _to_records(
        df.filter(F.col(COL_VEH) == veh_id)
        .select(COL_TRIP, COL_DAY)
        .distinct()
        .orderBy(COL_DAY, COL_TRIP)
        .toPandas()
    )


def get_trip_records(spark: SparkSession, trip_id: float, limit: int = 200) -> list:
    df = _read(spark)
    return _to_records(
        df.filter(F.col(COL_TRIP) == trip_id)
        .orderBy(COL_TS)
        .limit(limit)
        .toPandas()
    )


# ── Speed stats ───────────────────────────────────────────────────────────────

def speed_stats(spark: SparkSession) -> dict:
    df = _read(spark)
    agg = (
        df.filter(F.col(COL_SPEED).isNotNull())
        .agg(
            F.round(F.avg(COL_SPEED), 2).alias("avg_kmh"),
            F.round(F.max(COL_SPEED), 2).alias("max_kmh"),
            F.round(F.min(COL_SPEED), 2).alias("min_kmh"),
        )
        .first()
    )
    return agg.asDict()


# ── 1. Excesso de velocidade (> threshold km/h) ───────────────────────────────

def speeding_events(spark: SparkSession, threshold: float = 80.0) -> list:
    """Records where speed exceeds threshold, aggregated by vehicle/trip/day."""
    df = _read(spark)
    return (
        df.filter(F.col(COL_SPEED) > threshold)
        .groupBy(COL_VEH, COL_TRIP, COL_DAY)
        .agg(
            F.count("*").alias("speeding_records"),
            F.round(F.max(COL_SPEED), 2).alias("max_speed_kmh"),
            F.round(F.avg(COL_SPEED), 2).alias("avg_speed_kmh"),
        )
        .orderBy(F.desc("speeding_records"))
        .toPandas()
        .pipe(_to_records)
    )


# ── 2. Rotas mais utilizadas (lat/lon grid) ───────────────────────────────────

def top_routes(spark: SparkSession, precision: int = 2, limit: int = 20) -> list:
    """
    Round lat/lon to `precision` decimal places and count records per cell.
    The most-visited grid cells represent the most-used road segments.
    """
    df = _read(spark)
    return (
        df.filter(F.col(COL_LAT).isNotNull() & F.col(COL_LON).isNotNull())
        .withColumn("lat_zone", F.round(F.col(COL_LAT), precision))
        .withColumn("lon_zone", F.round(F.col(COL_LON), precision))
        .groupBy("lat_zone", "lon_zone")
        .agg(
            F.count("*").alias("record_count"),
            F.countDistinct(COL_VEH).alias("distinct_vehicles"),
            F.countDistinct(COL_TRIP).alias("distinct_trips"),
        )
        .orderBy(F.desc("record_count"))
        .limit(limit)
        .toPandas()
        .pipe(_to_records)
    )


# ── 3. Paradas longas (velocidade = 0 por muitos timestamps) ─────────────────

def long_stops(spark: SparkSession, min_consecutive: int = 10) -> list:
    """
    Detect sequences of consecutive zero-speed samples within each trip.
    Only returns stop events with >= min_consecutive samples.
    """
    df = _read(spark)
    w = Window.partitionBy(COL_VEH, COL_TRIP).orderBy(COL_TS)

    # cumsum of "moving" flag creates a unique group id for each stop segment
    df = (
        df
        .withColumn("moving", (F.col(COL_SPEED) > 0).cast("int"))
        .withColumn("stop_group", F.sum("moving").over(w))
    )

    stops = (
        df.filter(F.col(COL_SPEED) == 0)
        .groupBy(COL_VEH, COL_TRIP, COL_DAY, "stop_group")
        .agg(
            F.count("*").alias("stopped_samples"),
            F.round(F.min(COL_TS) / 1000, 1).alias("start_timestamp_s"),
            F.round(F.max(COL_TS) / 1000, 1).alias("end_timestamp_s"),
            F.first(COL_LAT).alias("latitude"),
            F.first(COL_LON).alias("longitude"),
        )
        .filter(F.col("stopped_samples") >= min_consecutive)
        .withColumn("duration_s", F.col("end_timestamp_s") - F.col("start_timestamp_s"))
        .drop("stop_group")
        .orderBy(F.desc("stopped_samples"))
    )
    return stops.toPandas().pipe(_to_records)


# ── 4. Consumo estimado de combustível (via MAF) ──────────────────────────────

def fuel_consumption(spark: SparkSession) -> list:
    """
    Estimate fuel consumption per vehicle/trip using MAF sensor data.

    Formula (assuming ~1 s OBD sampling interval):
      fuel_mass_g  = sum(MAF_g/s) * SAMPLE_INTERVAL_S
      fuel_liters  = fuel_mass_g / AIR_FUEL_RATIO / FUEL_DENSITY_G_L
    """
    df = _read(spark)
    factor = SAMPLE_INTERVAL_S / AIR_FUEL_RATIO / FUEL_DENSITY_G_L
    return (
        df.filter(F.col(COL_MAF).isNotNull() & (F.col(COL_MAF) >= 0))
        .groupBy(COL_VEH, COL_TRIP, COL_DAY)
        .agg(
            F.round(F.sum(COL_MAF) * factor, 4).alias("fuel_liters_est"),
            F.round(F.avg(COL_MAF), 4).alias("avg_maf_g_per_s"),
            F.round(F.max(COL_MAF), 4).alias("max_maf_g_per_s"),
            F.count("*").alias("sample_count"),
        )
        .orderBy(F.desc("fuel_liters_est"))
        .toPandas()
        .pipe(_to_records)
    )


# ── 5. Ranking de eficiência por RPM médio ────────────────────────────────────

def rpm_efficiency_ranking(spark: SparkSession) -> list:
    """
    Rank vehicles by average RPM while moving (ascending).
    Lower avg RPM at comparable speeds indicates more efficient driving.
    """
    df = _read(spark)
    ranked = (
        df.filter(
            F.col(COL_RPM).isNotNull()
            & F.col(COL_MAF).isNotNull()
            & (F.col(COL_SPEED) > 0)
        )
        .groupBy(COL_VEH)
        .agg(
            F.round(F.avg(COL_RPM), 1).alias("avg_rpm"),
            F.round(F.avg(COL_MAF), 4).alias("avg_maf_g_per_s"),
            F.round(F.avg(COL_LOAD), 2).alias("avg_load_pct"),
            F.round(F.avg(COL_SPEED), 2).alias("avg_speed_kmh"),
            F.count("*").alias("moving_samples"),
        )
        .orderBy("avg_rpm")
        .toPandas()
    )
    ranked.insert(0, "rank", range(1, len(ranked) + 1))
    return _to_records(ranked)


# ── 6. Detecção de anomalias (z-score > 3 em RPM ou MAF) ─────────────────────

def detect_anomalies(spark: SparkSession, limit: int = 50) -> list:
   
    df = _read(spark).filter(
        F.col(COL_RPM).isNotNull() & F.col(COL_MAF).isNotNull()
    )

    stats = df.agg(
        F.avg(COL_RPM).alias("rpm_mean"),
        F.stddev(COL_RPM).alias("rpm_std"),
        F.avg(COL_MAF).alias("maf_mean"),
        F.stddev(COL_MAF).alias("maf_std"),
    ).first()

    rpm_mean, rpm_std = stats["rpm_mean"], stats["rpm_std"]
    maf_mean, maf_std = stats["maf_mean"], stats["maf_std"]

    if not rpm_std or not maf_std:
        return []

    return (
        df.withColumn("rpm_z", F.round(F.abs((F.col(COL_RPM) - rpm_mean) / rpm_std), 3))
        .withColumn("maf_z", F.round(F.abs((F.col(COL_MAF) - maf_mean) / maf_std), 3))
        .filter((F.col("rpm_z") > 3) | (F.col("maf_z") > 3))
        .orderBy(F.desc(F.greatest("rpm_z", "maf_z")))
        .limit(limit)
        .toPandas()
        .pipe(_to_records)
    )

