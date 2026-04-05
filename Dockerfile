FROM python:3.10-slim

# ── System dependencies ────────────────────────────────────────────────────────
# Use default-jdk-headless which works across all Debian versions
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk-headless \
    curl \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ── Java environment ───────────────────────────────────────────────────────────
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ── CRITICAL: Tell PySpark exactly which Python to use ────────────────────────
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# ── Spark temp dirs ────────────────────────────────────────────────────────────
ENV SPARK_LOCAL_DIRS=/tmp/spark-local
RUN mkdir -p /tmp/spark-local

WORKDIR /app

# ── Install Python dependencies ────────────────────────────────────────────────
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# ── Copy full project ──────────────────────────────────────────────────────────
COPY . .

# ── Create dataset directories ────────────────────────────────────────────────
RUN mkdir -p datasets/finance datasets/geographical datasets/healthcare \
    datasets/education datasets/ecommerce datasets/climate \
    datasets/transportation datasets/logs datasets/public_data \
    datasets/social_media logs

# ── Environment ───────────────────────────────────────────────────────────────
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV SPARK_APP_NAME=SparkInsight
ENV SPARK_MASTER=local[*]
ENV SPARK_DRIVER_MEMORY=4g
ENV SPARK_DEFAULT_PARTITIONS=8
ENV LOG_LEVEL=INFO

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
