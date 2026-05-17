import logging
import threading
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from pyspark.sql import SparkSession

from api.routes import router
from spark.streaming import start_streaming

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
logger = logging.getLogger(__name__)


def _create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("VehicleTelemetryAPI")
        .master("spark://spark-master:7077")
        .config("spark.driver.host", "api")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def _init_spark(app: FastAPI) -> None:
    """Create SparkSession and start streaming in a background thread."""
    logger.info("Starting SparkSession (background)...")
    try:
        spark = _create_spark()
        app.state.spark = spark
        app.state.spark_ready = True
        logger.info("SparkSession ready.")

        logger.info("Launching Structured Streaming thread...")
        t = threading.Thread(
            target=start_streaming,
            args=(spark,),
            daemon=True,
            name="streaming-thread",
        )
        t.start()
    except Exception:
        logger.exception("Failed to initialise SparkSession")
        app.state.spark_ready = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.spark_ready = False
    app.state.spark = None

    t = threading.Thread(target=_init_spark, args=(app,), daemon=True, name="spark-init")
    t.start()

    yield

    if app.state.spark:
        logger.info("Shutting down SparkSession...")
        app.state.spark.stop()


app = FastAPI(
    title="Vehicle Telemetry API",
    description=(
        "REST API backed by Spark Structured Streaming reading vehicle "
        "telemetry data from HDFS.  New CSV files dropped into "
        "hdfs://namenode:9000/incoming/ are automatically processed."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
