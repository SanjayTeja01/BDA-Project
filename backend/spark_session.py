import os
import sys
import shutil

from pyspark.sql import SparkSession
from backend.logger import get_logger

logger = get_logger(__name__)

_spark: SparkSession = None


def _resolve_python_path() -> str:
    """
    Resolve the correct Python executable path for the current environment.
    Priority:
      1. PYSPARK_PYTHON env var (set in Dockerfile / docker-compose)
      2. sys.executable (the actual Python running this process)
      3. 'python3' / 'python' found on PATH
    Never hardcode a local machine path.
    """
    # 1. Honour explicit env override (Dockerfile sets this to 'python3')
    env_python = os.environ.get("PYSPARK_PYTHON") or os.environ.get("PYSPARK_DRIVER_PYTHON")
    if env_python and shutil.which(env_python):
        return env_python

    # 2. Use the same interpreter that is running the backend right now
    if sys.executable and os.path.isfile(sys.executable):
        return sys.executable

    # 3. Fallback: search PATH
    for candidate in ("python3", "python"):
        found = shutil.which(candidate)
        if found:
            return found

    raise RuntimeError(
        "Could not locate a Python executable for PySpark workers. "
        "Set the PYSPARK_PYTHON environment variable explicitly."
    )


def get_spark() -> SparkSession:
    global _spark
    if _spark and _spark.sparkContext._jvm is not None:
        return _spark

    python_path     = _resolve_python_path()
    partitions      = os.getenv("SPARK_DEFAULT_PARTITIONS", "8")
    app_name        = os.getenv("SPARK_APP_NAME", "SparkInsight")
    master          = os.getenv("SPARK_MASTER", "local[*]")
    driver_memory   = os.getenv("SPARK_DRIVER_MEMORY", "4g")
    executor_memory = os.getenv("SPARK_EXECUTOR_MEMORY", "4g")

    # Must be set as env vars BEFORE SparkSession is built
    os.environ["PYSPARK_PYTHON"]        = python_path
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

    logger.info(f"Initializing Spark session: master={master}, partitions={partitions}")
    logger.info(f"Using Python executable: {python_path}")

    _spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        # ── Tell Spark workers exactly which Python to use ──────────────────
        .config("spark.pyspark.python",        python_path)
        .config("spark.pyspark.driver.python", python_path)
        # ── Memory & performance ─────────────────────────────────────────────
        .config("spark.driver.memory",   driver_memory)
        .config("spark.executor.memory", executor_memory)
        .config("spark.sql.shuffle.partitions", partitions)
        # ── Adaptive query execution ─────────────────────────────────────────
        .config("spark.sql.adaptive.enabled",                    "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled",           "true")
        # ── Optimizations ────────────────────────────────────────────────────
        .config("spark.serializer",                              "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.parquet.mergeSchema",                 "false")
        .config("spark.sql.parquet.filterPushdown",              "true")
        .config("spark.sql.inMemoryColumnarStorage.compressed",  "true")
        .config("spark.sql.execution.arrow.pyspark.enabled",     "true")
        .config("spark.ui.enabled",                              "false")
        # ── Temp dirs (mapped in Dockerfile) ────────────────────────────────
        .config("spark.local.dir", os.getenv("SPARK_LOCAL_DIRS", "/tmp/spark-local"))
        .getOrCreate()
    )

    _spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Spark session created. Version: {_spark.version}")
    return _spark


def stop_spark():
    global _spark
    if _spark:
        _spark.stop()
        _spark = None
        logger.info("Spark session stopped.")