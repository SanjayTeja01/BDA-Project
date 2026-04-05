import time
from pyspark.sql import SparkSession
from backend.spark_session import get_spark
from backend.logger import get_logger

logger = get_logger(__name__)


class ExecutionTracker:
    def __init__(self):
        self._start: float = 0.0
        self._stage_times: dict = {}
        self._stage_start: float = 0.0
        self._current_stage: str = ""

    def start(self):
        self._start = time.perf_counter()

    def begin_stage(self, stage: str):
        self._current_stage = stage
        self._stage_start = time.perf_counter()

    def end_stage(self, stage: str = None):
        s = stage or self._current_stage
        elapsed = time.perf_counter() - self._stage_start
        self._stage_times[s] = round(elapsed, 3)
        logger.info(f"Stage '{s}' completed in {elapsed:.3f}s")

    def capture(self, df=None, row_count: int = 0) -> dict:
        total_time = round(time.perf_counter() - self._start, 3)
        spark = get_spark()

        spark_info = _get_spark_info(spark)

        dataset_size_bytes = 0
        partitions = 0
        if df is not None:
            try:
                partitions = df.rdd.getNumPartitions()
            except Exception:
                partitions = 0

        rps = round(row_count / total_time, 1) if total_time > 0 else 0

        return {
            "total_execution_time_sec": total_time,
            "stage_times": self._stage_times,
            "spark_version": spark_info.get("version"),
            "spark_app_id": spark_info.get("app_id"),
            "default_parallelism": spark_info.get("default_parallelism"),
            "partition_count": partitions,
            "records_processed": row_count,
            "records_per_second": rps,
            "adaptive_execution_enabled": True,
        }


def _get_spark_info(spark: SparkSession) -> dict:
    try:
        sc = spark.sparkContext
        return {
            "version": spark.version,
            "app_id": sc.applicationId,
            "default_parallelism": sc.defaultParallelism,
        }
    except Exception as e:
        logger.warning(f"Could not read Spark info: {e}")
        return {}
