"""
dataset_loader.py
=================
SparkInsight Analytics Platform — Dataset Loader

ARCHITECTURE & PRIORITY:
─────────────────────────────────────────────────────────────────────────────
  PRIORITY 1 → Internet loading (real data, best possible approach)
               • Every dataset fetched from its authoritative public source
               • All I/O is fully parallel (ThreadPoolExecutor)
               • Streaming with chunked reads — never buffers full files
               • Hard 40-second wall-clock budget per loader
               • On any failure/timeout → falls to Priority 2

  PRIORITY 2 → Synthetic fallback (zero-risk, no driver RAM)
               • Data generated INSIDE Spark executors (not driver)
               • 2 million rows per dataset (safe for 15GB RAM device)
               • Written to /tmp Parquet → Spark reads → /tmp deleted
               • Spark sees real Parquet: real partitions, real shuffle,
                 real execution metrics — legitimately big data
─────────────────────────────────────────────────────────────────────────────

DEVICE SAFETY (15GB RAM device):
  OS + background apps  : ~2 GB
  Spark Driver + JVM    : ~2 GB
  Spark Executor memory : ~3 GB
  Safety buffer         : ~1 GB
  Available for data    : ~7 GB   ← we use at most ~1.5GB (2M rows)
─────────────────────────────────────────────────────────────────────────────
"""

import io
import os
import csv
import gzip
import json
import shutil
import random
import tempfile
import threading
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType,
)

from backend.spark_session import get_spark
from backend.logger import get_logger

logger = get_logger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

INTERNET_TIMEOUT_SECONDS = 40       # hard wall-clock budget per dataset loader
INTERNET_TARGET_ROWS     = 200_000  # stop fetching once we have this many rows
HTTP_CONNECT_TIMEOUT     = 10       # TCP connection timeout
HTTP_READ_TIMEOUT        = 25       # response body timeout
MAX_PARALLEL_WORKERS     = 8        # ThreadPoolExecutor max threads

SYNTHETIC_TOTAL_ROWS     = 2_000_000  # 2M rows — safe for 15GB device
SYNTHETIC_PARTITIONS     = 20         # Spark partitions for real distribution

_HEADERS = {"User-Agent": "SparkInsight-Analytics/1.0 (research)"}
_dataset_cache: Dict[str, DataFrame] = {}


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def load_dataset(meta: dict) -> DataFrame:
    """
    Main entry point. Returns a cached, analysis-ready Spark DataFrame.
    Flow:
      1. Return from cache if already loaded.
      2. Try internet loader with 40s timeout.
      3. If internet fails → generate synthetic data safely in Spark executors.
      4. Cache and return.
    """
    name = meta["dataset_name"]

    if name in _dataset_cache:
        logger.info(f"[CACHE HIT] '{name}' — returning cached DataFrame")
        return _dataset_cache[name]

    spark = get_spark()
    df: Optional[DataFrame] = None

    # ── PRIORITY 1: Internet ──────────────────────────────────────────────
    loader_fn = _INTERNET_LOADERS.get(name)
    if loader_fn:
        logger.info(
            f"[INTERNET] Loading '{name}' "
            f"(timeout={INTERNET_TIMEOUT_SECONDS}s, target={INTERNET_TARGET_ROWS:,} rows) ..."
        )
        df = _run_with_timeout(loader_fn, spark, meta, INTERNET_TIMEOUT_SECONDS)
        if df is not None:
            try:
                df.limit(1).collect()
                logger.info(f"[INTERNET] Successfully loaded '{name}' from internet")
            except Exception as e:
                logger.warning(f"[INTERNET] Validation failed for '{name}': {e}")
                df = None

    # ── PRIORITY 2: Synthetic fallback ───────────────────────────────────
    if df is None:
        logger.info(
            f"[SYNTHETIC] Internet unavailable for '{name}' — "
            f"generating {SYNTHETIC_TOTAL_ROWS:,} rows in Spark executors ..."
        )
        df = _synthetic_via_spark(spark, meta)
        logger.info(f"[SYNTHETIC] '{name}' synthetic data ready")

    df = _normalize_columns(df)
    df = df.cache()
    _dataset_cache[name] = df
    logger.info(
        f"[READY] '{name}' — "
        f"partitions={df.rdd.getNumPartitions()}, cols={len(df.columns)}"
    )
    return df


def get_cached(name: str) -> Optional[DataFrame]:
    return _dataset_cache.get(name)


def evict_cache(name: str):
    if name in _dataset_cache:
        _dataset_cache[name].unpersist()
        del _dataset_cache[name]
        logger.info(f"[CACHE] Evicted '{name}'")


# ─────────────────────────────────────────────────────────────────────────────
# TIMEOUT WRAPPER
# ─────────────────────────────────────────────────────────────────────────────

def _run_with_timeout(fn, spark, meta, timeout) -> Optional[DataFrame]:
    """
    Run fn(spark, meta) in a daemon thread.
    If it takes longer than timeout seconds, abandon and return None.
    """
    result: List = [None]
    error:  List = [None]

    def _target():
        try:
            result[0] = fn(spark, meta)
        except Exception as exc:
            error[0] = exc

    t = threading.Thread(target=_target, daemon=True)
    t.start()
    t.join(timeout=timeout)

    if t.is_alive():
        logger.warning(
            f"[TIMEOUT] Internet loader exceeded {timeout}s — "
            f"switching to synthetic data"
        )
        return None
    if error[0] is not None:
        logger.warning(f"[ERROR] Internet loader raised: {error[0]}")
        return None
    return result[0]


# ─────────────────────────────────────────────────────────────────────────────
# HTTP HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _get(url: str, params: dict = None,
         extra_headers: dict = None, stream: bool = False) -> requests.Response:
    h = {**_HEADERS, **(extra_headers or {})}
    return requests.get(
        url, params=params, headers=h,
        timeout=(HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT),
        stream=stream,
    )


def _post(url: str, data: dict = None) -> requests.Response:
    return requests.post(
        url, data=data, headers=_HEADERS,
        timeout=(HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT),
    )


def _stream_csv(url: str, row_limit: int,
                chunk_bytes: int = 512 * 1024) -> List[dict]:
    """Stream CSV over HTTP row-by-row. Never loads full file into memory."""
    resp = _get(url, stream=True)
    resp.raise_for_status()
    rows: List[dict] = []
    buf = io.StringIO()
    fieldnames: Optional[List[str]] = None
    header_done = False
    for raw in resp.iter_content(chunk_size=chunk_bytes, decode_unicode=True):
        buf.write(raw)
        buf.seek(0)
        lines = buf.readlines()
        if len(lines) < 2:
            buf.seek(0, 2)
            continue
        if not header_done:
            fieldnames = next(csv.reader([lines[0]]))
            header_done = True
            lines = lines[1:]
        for line in lines[:-1]:
            try:
                row = dict(zip(fieldnames, next(csv.reader([line]))))
                rows.append(row)
            except Exception:
                continue
            if len(rows) >= row_limit:
                resp.close()
                return rows
        buf = io.StringIO(lines[-1])
        buf.seek(0, 2)
    return rows


def _stream_jsonl_gz(url: str, row_limit: int,
                     max_bytes: int = 10 * 1024 * 1024) -> List[dict]:
    """Stream gzipped JSONL, decompress on the fly. Capped at max_bytes."""
    resp = _get(url, stream=True)
    resp.raise_for_status()
    buf = b""
    downloaded = 0
    for chunk in resp.iter_content(chunk_size=1024 * 1024):
        buf += chunk
        downloaded += len(chunk)
        if downloaded >= max_bytes:
            resp.close()
            break
    rows: List[dict] = []
    try:
        with gzip.open(io.BytesIO(buf)) as gz:
            for line in gz:
                try:
                    rows.append(json.loads(line))
                except Exception:
                    continue
                if len(rows) >= row_limit:
                    break
    except Exception as exc:
        logger.warning(f"JSONL-GZ decompress error: {exc}")
    return rows


def _parallel_collect(
    tasks: list,
    worker: Callable,
    target: int = INTERNET_TARGET_ROWS,
    workers: int = MAX_PARALLEL_WORKERS,
) -> List[dict]:
    """
    Run worker(task) for each task in parallel.
    Stops accumulating once target rows are collected.
    """
    all_rows: List[dict] = []
    lock = threading.Lock()
    enough = threading.Event()

    def _safe(task):
        if enough.is_set():
            return []
        try:
            return worker(task)
        except Exception as exc:
            logger.warning(f"Worker failed task={task}: {exc}")
            return []

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_safe, t): t for t in tasks}
        for fut in as_completed(futures):
            batch = fut.result()
            with lock:
                all_rows.extend(batch)
                if len(all_rows) >= target:
                    enough.set()

    return all_rows[:target]


def _rows_to_df(spark: SparkSession, rows: List[dict]) -> DataFrame:
    """Convert list-of-dicts to Spark DataFrame via pandas+Arrow when available."""
    if not rows:
        raise RuntimeError("No rows to convert.")
    try:
        import pandas as pd
        return spark.createDataFrame(pd.DataFrame(rows))
    except ImportError:
        return spark.createDataFrame(rows)


def _normalize_columns(df: DataFrame) -> DataFrame:
    for col in df.columns:
        clean = (
            col.strip()
               .replace(" ","_").replace("(","").replace(")","")
               .replace("/","_").replace("-","_")
               .replace(".","_").replace("%","pct")
        )
        if clean != col:
            df = df.withColumnRenamed(col, clean)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# INTERNET LOADERS  (Priority 1)
# ─────────────────────────────────────────────────────────────────────────────

def _load_nyc_taxi(spark: SparkSession, meta: dict) -> DataFrame:
    """
    Spark reads TLC parquet directly over HTTPS — no download to disk.
    Multiple months tried in parallel; first success wins.
    """
    months = ["2023-12","2023-11","2023-10","2023-09","2023-08","2023-07"]
    cdn    = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{m}.parquet"
    result: List = [None]
    found  = threading.Event()

    def _try_month(month):
        if found.is_set():
            return
        url = cdn.format(m=month)
        try:
            logger.info(f"[NYC Taxi] Spark direct-read: {url}")
            df = spark.read.parquet(url)
            df_lim = df.limit(INTERNET_TARGET_ROWS)
            df_lim.limit(1).collect()
            if not found.is_set():
                result[0] = df_lim
                found.set()
                logger.info(f"[NYC Taxi] {month} loaded via Spark direct-read")
        except Exception as exc:
            logger.warning(f"[NYC Taxi] {month} direct-read failed: {exc}")

    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = [ex.submit(_try_month, m) for m in months]
        for fut in as_completed(futs):
            if found.is_set():
                break

    if result[0] is not None:
        return result[0]

    # Fallback: stream 32MB max to tmp file
    for month in months[:3]:
        url = cdn.format(m=month)
        tmp_path = None
        try:
            resp = _get(url, stream=True)
            resp.raise_for_status()
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                downloaded = 0
                for chunk in resp.iter_content(chunk_size=4*1024*1024):
                    tmp.write(chunk)
                    downloaded += len(chunk)
                    if downloaded >= 32*1024*1024:
                        resp.close()
                        break
                tmp_path = tmp.name
            df = spark.read.parquet(tmp_path)
            df_lim = df.limit(INTERNET_TARGET_ROWS)
            df_lim.limit(1).collect()
            logger.info(f"[NYC Taxi] Fallback tmpfile succeeded ({month})")
            return df_lim
        except Exception as exc:
            logger.warning(f"[NYC Taxi] Tmpfile fallback failed ({month}): {exc}")
        finally:
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass

    raise RuntimeError("[NYC Taxi] All sources failed.")


def _load_stock_market(spark: SparkSession, meta: dict) -> DataFrame:
    """
    Parallel yfinance fetch for 30 major tickers.
    Falls back to Alpha Vantage demo if yfinance unavailable.
    """
    TICKERS = [
        "AAPL","MSFT","GOOGL","AMZN","NVDA","META","TSLA","JPM","V","XOM",
        "JNJ","PG","HD","CVX","MRK","KO","PEP","WMT","DIS","NFLX",
        "AMD","INTC","CSCO","ORCL","CRM","ADBE","PYPL","QCOM","TXN","IBM",
    ]
    SECTOR = {
        "AAPL":"Technology","MSFT":"Technology","GOOGL":"Technology",
        "AMZN":"Consumer Discretionary","NVDA":"Technology",
        "META":"Communication Services","TSLA":"Consumer Discretionary",
        "JPM":"Financials","V":"Financials","XOM":"Energy",
        "JNJ":"Health Care","PG":"Consumer Staples","HD":"Consumer Discretionary",
        "CVX":"Energy","MRK":"Health Care","KO":"Consumer Staples",
        "PEP":"Consumer Staples","WMT":"Consumer Staples",
        "DIS":"Communication Services","NFLX":"Communication Services",
    }
    try:
        import yfinance as yf
        all_rows: List[dict] = []
        lock   = threading.Lock()
        enough = threading.Event()

        def _fetch(ticker):
            if enough.is_set():
                return []
            try:
                hist = yf.Ticker(ticker).history(period="3y", interval="1d")
                rows = []
                for idx, row in hist.iterrows():
                    rows.append({
                        "ticker":        ticker,
                        "exchange":      "NASDAQ",
                        "security_name": f"{ticker} Inc.",
                        "trade_date":    str(idx.date()),
                        "open_price":    float(row.get("Open",  0) or 0),
                        "high_price":    float(row.get("High",  0) or 0),
                        "low_price":     float(row.get("Low",   0) or 0),
                        "close_price":   float(row.get("Close", 0) or 0),
                        "adj_close":     float(row.get("Close", 0) or 0),
                        "volume":        float(row.get("Volume",0) or 0),
                        "market_cap":    0.0,
                        "sector":        SECTOR.get(ticker,"Other"),
                        "industry":      SECTOR.get(ticker,"Other"),
                        "country":       "USA",
                    })
                return rows
            except Exception as exc:
                logger.warning(f"[Stock] yfinance {ticker} failed: {exc}")
                return []

        with ThreadPoolExecutor(max_workers=8) as ex:
            futures = {ex.submit(_fetch, t): t for t in TICKERS}
            for fut in as_completed(futures):
                batch = fut.result()
                with lock:
                    all_rows.extend(batch)
                    if len(all_rows) >= INTERNET_TARGET_ROWS:
                        enough.set()

        if all_rows:
            logger.info(f"[Stock] {len(all_rows):,} rows via yfinance")
            return _rows_to_df(spark, all_rows[:INTERNET_TARGET_ROWS])

    except ImportError:
        logger.warning("[Stock] yfinance not installed — trying Alpha Vantage demo")

    try:
        resp = _get("https://www.alphavantage.co/query", params={
            "function":"TIME_SERIES_DAILY","symbol":"IBM",
            "outputsize":"full","apikey":"demo",
        })
        resp.raise_for_status()
        ts   = resp.json().get("Time Series (Daily)", {})
        rows = []
        for date_str, v in ts.items():
            rows.append({
                "ticker":"IBM","exchange":"NYSE","security_name":"IBM",
                "trade_date":date_str,
                "open_price":float(v.get("1. open",0)),
                "high_price":float(v.get("2. high",0)),
                "low_price":float(v.get("3. low",0)),
                "close_price":float(v.get("4. close",0)),
                "adj_close":float(v.get("4. close",0)),
                "volume":float(v.get("5. volume",0)),
                "market_cap":0.0,"sector":"Technology",
                "industry":"Technology","country":"USA",
            })
        if rows:
            logger.info(f"[Stock] {len(rows)} rows via Alpha Vantage demo")
            return _rows_to_df(spark, rows[:INTERNET_TARGET_ROWS])
    except Exception as exc:
        logger.warning(f"[Stock] Alpha Vantage failed: {exc}")

    raise RuntimeError("[Stock] All sources failed.")


def _load_medicare(spark: SparkSession, meta: dict) -> DataFrame:
    """
    Probes CMS Socrata endpoints in parallel, paginates the winner.
    """
    ENDPOINTS = [
        "https://data.cms.gov/resource/3z4d-vmk2.json",
        "https://data.cms.gov/resource/znk5-bxst.json",
        "https://data.cms.gov/resource/f455-3pa5.json",
        "https://data.cms.gov/resource/efwk-h425.json",
    ]
    PAGE = 2000

    def _parse(rec, offset):
        def sf(k):
            try: return float(rec.get(k,0) or 0)
            except: return 0.0
        return {
            "beneficiary_code":            str(rec.get("npi",rec.get("rndrng_npi",f"B{offset}"))),
            "claim_id":                    str(rec.get("hcpcs_cd",rec.get("drg_cd","CLM"))),
            "provider_id":                 str(rec.get("npi",rec.get("rndrng_npi","PROV"))),
            "claim_from_date":             str(rec.get("year",rec.get("rfrg_yr","2022"))),
            "claim_thru_date":             "",
            "diagnosis_code_1":            str(rec.get("hcpcs_drug_ind","N")),
            "diagnosis_code_2":            "",
            "procedure_code":              "",
            "claim_payment_amount":        sf("avg_mdcr_pymt_amt") or sf("avg_tot_pymt_amt"),
            "deductible_amount":           sf("avg_sbmtd_cvrd_chrg") or sf("avg_mdcr_alowd_amt"),
            "carrier_line_allowed_charge": sf("avg_mdcr_alowd_amt") or sf("avg_mdcr_stdzd_amt"),
            "hcpcs_code":                  str(rec.get("hcpcs_cd","99213")),
            "claim_type":                  str(rec.get("rndrng_prvdr_type",rec.get("entity_cd","Carrier"))),
            "state_code":                  str(rec.get("rndrng_prvdr_state_abrvtn",rec.get("nppes_prvdr_state","CA"))),
        }

    def _probe(ep):
        try:
            resp = _get(ep, params={"$limit":PAGE,"$offset":0})
            if resp.status_code == 200:
                data = resp.json()
                if data:
                    return ep, data
        except Exception:
            pass
        return None

    working_ep, first_page = None, []
    with ThreadPoolExecutor(max_workers=len(ENDPOINTS)) as ex:
        futures = {ex.submit(_probe, ep): ep for ep in ENDPOINTS}
        for fut in as_completed(futures):
            res = fut.result()
            if res:
                working_ep, first_page = res
                break

    if not working_ep:
        raise RuntimeError("[Medicare] No CMS endpoint responded.")

    all_rows = [_parse(r,i) for i,r in enumerate(first_page)]
    offset   = PAGE
    while len(all_rows) < INTERNET_TARGET_ROWS:
        try:
            resp = _get(working_ep, params={"$limit":PAGE,"$offset":offset})
            if resp.status_code != 200:
                break
            page = resp.json()
            if not page:
                break
            for i,rec in enumerate(page):
                all_rows.append(_parse(rec, offset+i))
            offset += PAGE
        except Exception as exc:
            logger.warning(f"[Medicare] Pagination error offset={offset}: {exc}")
            break

    logger.info(f"[Medicare] {len(all_rows):,} rows from {working_ep}")
    return _rows_to_df(spark, all_rows[:INTERNET_TARGET_ROWS])


def _load_common_crawl(spark: SparkSession, meta: dict) -> DataFrame:
    """Queries CommonCrawl index for multiple domains in parallel."""
    INDEX   = "http://index.commoncrawl.org/CC-MAIN-2024-10-index"
    DOMAINS = [
        "wikipedia.org","reddit.com","github.com","stackoverflow.com",
        "medium.com","bbc.com","nytimes.com","techcrunch.com",
        "reuters.com","arxiv.org","cnn.com","theguardian.com",
    ]
    per_domain = max(500, INTERNET_TARGET_ROWS // len(DOMAINS))

    def _fetch(domain):
        try:
            resp = _get(INDEX, params={
                "url":f"*.{domain}","output":"json","limit":per_domain,
                "fl":"url,status,mime,length,timestamp,languages,filename",
            })
            if resp.status_code != 200:
                return []
            rows = []
            for line in resp.text.strip().split("\n"):
                if not line.strip():
                    continue
                try:
                    rec = json.loads(line)
                    ts  = str(rec.get("timestamp",""))
                    ts_fmt = (
                        f"{ts[:4]}-{ts[4:6]}-{ts[6:8]} {ts[8:10]}:{ts[10:12]}:{ts[12:14]}"
                        if len(ts) >= 14 else ts
                    )
                    rows.append({
                        "url":               str(rec.get("url","")),
                        "fetch_status":      str(rec.get("status","200")),
                        "content_type":      str(rec.get("mime","text/html")),
                        "crawl_timestamp":   ts_fmt,
                        "response_bytes":    float(rec.get("length",0) or 0),
                        "redirect_url":      "",
                        "detected_language": str(rec.get("languages","eng")),
                        "mime_type":         str(rec.get("mime","text/html")),
                        "warc_filename":     str(rec.get("filename","")),
                        "warc_record_offset":0.0,
                        "tld":               domain.split(".")[-1],
                        "domain":            domain,
                    })
                except Exception:
                    continue
            return rows
        except Exception as exc:
            logger.warning(f"[CommonCrawl] {domain} failed: {exc}")
            return []

    all_rows = _parallel_collect(DOMAINS, _fetch, target=INTERNET_TARGET_ROWS)
    if not all_rows:
        raise RuntimeError("[CommonCrawl] All domain queries failed.")
    logger.info(f"[CommonCrawl] {len(all_rows):,} rows")
    return _rows_to_df(spark, all_rows)


def _load_amazon_reviews(spark: SparkSession, meta: dict) -> DataFrame:
    """Parallel UCSD Amazon 2023 category fetches. 8MB cap per category."""
    BASES = [
        "https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/review_categories",
        "https://datarepo.eng.ucsd.edu/mcauley_group/data/amazon_2023/raw/review_categories",
    ]
    CATS = [
        "All_Beauty","Gift_Cards","Digital_Music","Musical_Instruments",
        "Industrial_and_Scientific","Toys_and_Games","Electronics",
        "Sports_and_Outdoors","Cell_Phones_and_Accessories","Office_Products",
    ]
    per_cat = max(200, INTERNET_TARGET_ROWS // len(CATS))

    def _fetch(cat):
        for base in BASES:
            url = f"{base}/{cat}.jsonl.gz"
            try:
                raw_rows = _stream_jsonl_gz(url, row_limit=per_cat,
                                            max_bytes=8*1024*1024)
                if not raw_rows:
                    continue
                rows = []
                for rec in raw_rows:
                    ts = rec.get("timestamp", rec.get("unixReviewTime",0))
                    if isinstance(ts,(int,float)) and ts > 1e9:
                        rd = datetime.utcfromtimestamp(
                            ts/1000 if ts > 1e12 else ts
                        ).strftime("%Y-%m-%d")
                    else:
                        rd = str(ts)[:10]
                    rows.append({
                        "reviewer_id":       str(rec.get("user_id",rec.get("reviewerID",""))),
                        "asin":              str(rec.get("asin","")),
                        "product_title":     str(rec.get("title",""))[:200],
                        "product_category":  cat.replace("_"," "),
                        "star_rating":       float(rec.get("rating",rec.get("overall",0)) or 0),
                        "helpful_votes":     float(rec.get("helpful_vote",0) or 0),
                        "total_votes":       0.0,
                        "vine":              "",
                        "verified_purchase": str(rec.get("verified_purchase",False)),
                        "review_headline":   str(rec.get("title",""))[:100],
                        "review_body":       "",
                        "review_date":       rd,
                        "marketplace":       "US",
                        "customer_id":       str(rec.get("user_id","")),
                    })
                logger.info(f"[Amazon] {cat}: {len(rows)} rows")
                return rows
            except Exception as exc:
                logger.warning(f"[Amazon] {cat}@{base} failed: {exc}")
        return []

    all_rows = _parallel_collect(CATS, _fetch,
                                 target=INTERNET_TARGET_ROWS, workers=4)
    if not all_rows:
        raise RuntimeError("[Amazon] All categories failed.")
    logger.info(f"[Amazon] {len(all_rows):,} rows")
    return _rows_to_df(spark, all_rows)


def _load_noaa_climate(spark: SparkSession, meta: dict) -> DataFrame:
    """Open-Meteo free API — 20 cities in parallel, no auth needed."""
    CITIES = [
        ("New York",40.71,-74.01),("London",51.51,-0.13),
        ("Tokyo",35.69,139.69),("Sydney",-33.87,151.21),
        ("Paris",48.85,2.35),("Beijing",39.91,116.39),
        ("Mumbai",19.08,72.88),("Cairo",30.06,31.25),
        ("Sao Paulo",-23.55,-46.63),("Lagos",6.52,3.38),
        ("Moscow",55.75,37.62),("Dubai",25.20,55.27),
        ("Chicago",41.88,-87.63),("Toronto",43.65,-79.38),
        ("Singapore",1.35,103.82),("Berlin",52.52,13.40),
        ("Mexico City",19.43,-99.13),("Buenos Aires",-34.60,-58.38),
        ("Johannesburg",-26.20,28.04),("Seoul",37.57,126.98),
    ]

    def _fetch(args):
        city, lat, lon = args
        try:
            resp = _get(
                "https://archive-api.open-meteo.com/v1/archive",
                params={
                    "latitude":lat,"longitude":lon,
                    "start_date":"2019-01-01","end_date":"2023-12-31",
                    "daily":"temperature_2m_max,temperature_2m_min,"
                            "precipitation_sum,windspeed_10m_max",
                    "timezone":"UTC",
                },
            )
            if resp.status_code != 200:
                return []
            daily = resp.json().get("daily",{})
            dates = daily.get("time",[])
            tmax  = daily.get("temperature_2m_max",[])
            tmin  = daily.get("temperature_2m_min",[])
            prcp  = daily.get("precipitation_sum",[])
            wind  = daily.get("windspeed_10m_max",[])
            rows  = []
            for i,d in enumerate(dates):
                rows.append({
                    "station_id":       f"{city.replace(' ','_')}_{lat}_{lon}",
                    "station_name":     city,
                    "latitude":         float(lat),
                    "longitude":        float(lon),
                    "elevation":        0.0,
                    "country":          city,
                    "state":            "",
                    "observation_date": str(d),
                    "temperature_max":  float(tmax[i]) if i<len(tmax) and tmax[i] is not None else 0.0,
                    "temperature_min":  float(tmin[i]) if i<len(tmin) and tmin[i] is not None else 0.0,
                    "precipitation":    float(prcp[i]) if i<len(prcp) and prcp[i] is not None else 0.0,
                    "snow_depth":       0.0,
                    "wind_speed":       float(wind[i]) if i<len(wind) and wind[i] is not None else 0.0,
                    "cloud_cover":      0.0,
                    "data_quality_flag":"",
                })
            logger.info(f"[NOAA] {city}: {len(rows)} days")
            return rows
        except Exception as exc:
            logger.warning(f"[NOAA] {city} failed: {exc}")
            return []

    all_rows = _parallel_collect(CITIES, _fetch,
                                 target=INTERNET_TARGET_ROWS, workers=8)
    if not all_rows:
        raise RuntimeError("[NOAA] All city sources failed.")
    logger.info(f"[NOAA] {len(all_rows):,} rows")
    return _rows_to_df(spark, all_rows)


def _load_osm_pois(spark: SparkSession, meta: dict) -> DataFrame:
    """Overpass API — all amenity types queried in parallel across mirrors."""
    MIRRORS = [
        "https://overpass-api.de/api/interpreter",
        "https://overpass.kumi.systems/api/interpreter",
    ]
    AMENITIES = [
        "restaurant","school","hospital","bank","pharmacy",
        "fuel","cafe","hotel","supermarket","library",
        "fast_food","bar","clinic","cinema","museum",
    ]
    per_amenity = max(200, INTERNET_TARGET_ROWS // len(AMENITIES))

    def _fetch(amenity):
        query = (
            f'[out:json][timeout:20];\n'
            f'node["amenity"="{amenity}"];\n'
            f'out body {per_amenity};'
        )
        for mirror in MIRRORS:
            try:
                resp = requests.post(
                    mirror, data={"data":query}, headers=_HEADERS,
                    timeout=(HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT),
                )
                if resp.status_code != 200:
                    continue
                rows = []
                for el in resp.json().get("elements",[]):
                    tags = el.get("tags",{})
                    rows.append({
                        "osm_id":       str(el.get("id","")),
                        "osm_type":     str(el.get("type","node")),
                        "name":         str(tags.get("name","")),
                        "amenity":      amenity,
                        "shop":         str(tags.get("shop","")),
                        "tourism":      str(tags.get("tourism","")),
                        "leisure":      str(tags.get("leisure","")),
                        "natural":      str(tags.get("natural","")),
                        "latitude":     float(el.get("lat",0)),
                        "longitude":    float(el.get("lon",0)),
                        "country":      str(tags.get("addr:country","")),
                        "city":         str(tags.get("addr:city","")),
                        "postcode":     str(tags.get("addr:postcode","")),
                        "last_updated": str(el.get("timestamp","")),
                        "tag_count":    float(len(tags)),
                    })
                if rows:
                    logger.info(f"[OSM] amenity={amenity}: {len(rows)} rows")
                    return rows
            except Exception as exc:
                logger.warning(f"[OSM] {mirror} amenity={amenity} failed: {exc}")
        return []

    all_rows = _parallel_collect(AMENITIES, _fetch,
                                 target=INTERNET_TARGET_ROWS, workers=4)
    if not all_rows:
        raise RuntimeError("[OSM] All Overpass queries failed.")
    logger.info(f"[OSM] {len(all_rows):,} rows")
    return _rows_to_df(spark, all_rows)


def _load_census_acs(spark: SparkSession, meta: dict) -> DataFrame:
    """All US states fetched in parallel — no API key required for county level."""
    VARIABLES = [
        "B01003_001E","B01002_001E","B19013_001E","B19301_001E",
        "B17001_002E","B23025_005E","B15003_022E","B25003_002E",
        "B25077_001E","B02001_002E","B02001_003E","B03001_003E",
    ]
    STATE_FIPS = [
        "01","02","04","05","06","08","09","10","12","13","15","16","17","18",
        "19","20","21","22","23","24","25","26","27","28","29","30","31","32",
        "33","34","35","36","37","38","39","40","41","42","44","45","46","47",
        "48","49","50","51","53","54","55","56",
    ]
    api_key = os.getenv("CENSUS_API_KEY","")

    def _fetch(state):
        for year in [2021,2020,2019]:
            try:
                params = {
                    "get":"GEO_ID,"+",".join(VARIABLES),
                    "for":"county:*","in":f"state:{state}",
                }
                if api_key:
                    params["key"] = api_key
                resp = _get(
                    f"https://api.census.gov/data/{year}/acs/acs5",
                    params=params,
                )
                if resp.status_code != 200:
                    continue
                data = resp.json()
                hdr  = data[0]
                def sf(rec,k):
                    try: return max(float(rec.get(k,0) or 0),0.0)
                    except: return 0.0
                rows = []
                for rec_list in data[1:]:
                    rec = dict(zip(hdr,rec_list))
                    pop = sf(rec,"B01003_001E")
                    rows.append({
                        "geo_id":                  str(rec.get("GEO_ID","")),
                        "state_fips":              state,
                        "county_fips":             str(rec.get("county","")),
                        "tract_fips":              "000",
                        "survey_year":             str(year),
                        "total_population":        sf(rec,"B01003_001E"),
                        "median_age":              sf(rec,"B01002_001E"),
                        "median_household_income": sf(rec,"B19013_001E"),
                        "per_capita_income":       sf(rec,"B19301_001E"),
                        "poverty_rate":            round(sf(rec,"B17001_002E")/max(pop,1)*100,2),
                        "unemployment_rate":       round(sf(rec,"B23025_005E")/max(pop,1)*100,2),
                        "education_bachelors_pct": round(sf(rec,"B15003_022E")/max(pop,1)*100,2),
                        "owner_occupied_pct":      round(sf(rec,"B25003_002E")/max(pop,1)*100,2),
                        "median_home_value":       sf(rec,"B25077_001E"),
                        "gini_index":              0.0,
                        "race_white_pct":          round(sf(rec,"B02001_002E")/max(pop,1)*100,2),
                        "race_black_pct":          round(sf(rec,"B02001_003E")/max(pop,1)*100,2),
                        "hispanic_pct":            round(sf(rec,"B03001_003E")/max(pop,1)*100,2),
                    })
                if rows:
                    return rows
            except Exception as exc:
                logger.warning(f"[Census] state={state} year={year} failed: {exc}")
        return []

    all_rows = _parallel_collect(STATE_FIPS, _fetch,
                                 target=INTERNET_TARGET_ROWS, workers=8)
    if not all_rows:
        raise RuntimeError("[Census] No state returned data.")
    logger.info(f"[Census] {len(all_rows):,} rows")
    return _rows_to_df(spark, all_rows)


def _load_twitter(spark: SparkSession, meta: dict) -> DataFrame:
    """Reddit public JSON API — all subreddits fetched in parallel."""
    SUBS = [
        "worldnews","technology","science","politics","news",
        "funny","gaming","movies","music","sports",
        "business","environment","space","programming","datascience",
    ]

    def _utc(ts):
        try:
            return datetime.utcfromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    def _fetch(sub):
        rows = []
        for sort in ["top","hot"]:
            try:
                params = {"limit":100}
                if sort == "top":
                    params["t"] = "all"
                resp = requests.get(
                    f"https://www.reddit.com/r/{sub}/{sort}.json",
                    params=params,
                    headers={"User-Agent":"SparkInsight/1.0 (research)"},
                    timeout=(HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT),
                )
                if resp.status_code != 200:
                    continue
                for post in resp.json().get("data",{}).get("children",[]):
                    p = post.get("data",{})
                    rows.append({
                        "tweet_id":       str(p.get("id","")),
                        "user_id":        str(p.get("author_fullname","")),
                        "screen_name":    str(p.get("author","")),
                        "created_at":     _utc(p.get("created_utc",0)),
                        "text":           str(p.get("title",""))[:280],
                        "retweet_count":  float(p.get("num_comments",0) or 0),
                        "favorite_count": float(p.get("ups",0) or 0),
                        "reply_count":    float(p.get("num_comments",0) or 0),
                        "quote_count":    0.0,
                        "lang":           str(p.get("lang","en") or "en"),
                        "source_device":  "Reddit",
                        "is_retweet":     str(p.get("is_self",False)),
                        "hashtags":       str(p.get("link_flair_text","") or ""),
                        "user_followers": float(p.get("score",0) or 0),
                        "user_following": 0.0,
                        "user_verified":  str(bool(p.get("distinguished"))),
                        "geo_country":    "",
                    })
            except Exception as exc:
                logger.warning(f"[Reddit] r/{sub}/{sort} failed: {exc}")
        return rows

    all_rows = _parallel_collect(SUBS, _fetch,
                                 target=INTERNET_TARGET_ROWS, workers=6)
    if not all_rows:
        raise RuntimeError("[Reddit] All subreddits failed.")
    logger.info(f"[Reddit] {len(all_rows):,} rows")
    return _rows_to_df(spark, all_rows)


def _load_oulad(spark: SparkSession, meta: dict) -> DataFrame:
    """OULAD CSV — studentAssessment and studentInfo fetched in parallel."""
    BASES = [
        "https://raw.githubusercontent.com/mragpavank/open-university-learning-analytics-dataset/master",
        "https://raw.githubusercontent.com/ouseful-PR/open-university-learning-analytics-dataset/master/anonymisedData",
    ]
    base_dt = datetime(2013,1,1)

    for base in BASES:
        try:
            logger.info(f"[OULAD] Trying: {base}")
            sa_rows: List[dict] = []
            si_rows: List[dict] = []

            def _get_sa():
                return _stream_csv(
                    f"{base}/studentAssessment.csv",
                    row_limit=INTERNET_TARGET_ROWS
                )
            def _get_si():
                return _stream_csv(
                    f"{base}/studentInfo.csv",
                    row_limit=50_000
                )

            with ThreadPoolExecutor(max_workers=2) as ex:
                fut_sa = ex.submit(_get_sa)
                fut_si = ex.submit(_get_si)
                sa_rows = fut_sa.result()
                try:
                    si_rows = fut_si.result()
                except Exception:
                    si_rows = []

            if not sa_rows:
                continue

            si_map = {r.get("id_student"): r for r in si_rows}
            rows   = []
            for rec in sa_rows:
                sid  = rec.get("id_student","")
                info = si_map.get(sid,{})
                try:
                    di   = int(float(rec.get("date_submitted",0) or 0))
                    date = (base_dt+timedelta(days=di)).strftime("%Y-%m-%d")
                except Exception:
                    date = "2013-01-01"
                rows.append({
                    "id_student":           str(sid),
                    "code_module":          str(info.get("code_module",rec.get("code_module","AAA"))),
                    "code_presentation":    str(info.get("code_presentation","2013B")),
                    "id_assessment":        str(rec.get("id_assessment","")),
                    "assessment_type":      str(rec.get("assessment_type","TMA")),
                    "date_submitted":       date,
                    "is_banked":            str(rec.get("is_banked","0")),
                    "score":                float(rec.get("score",0) or 0),
                    "gender":               str(info.get("gender","")),
                    "region":               str(info.get("region","")),
                    "highest_education":    str(info.get("highest_education","")),
                    "imd_band":             str(info.get("imd_band","")),
                    "age_band":             str(info.get("age_band","")),
                    "num_of_prev_attempts": float(info.get("num_of_prev_attempts",0) or 0),
                    "studied_credits":      float(info.get("studied_credits",0) or 0),
                    "disability":           str(info.get("disability","N")),
                    "final_result":         str(info.get("final_result","")),
                })
            if rows:
                logger.info(f"[OULAD] {len(rows):,} rows")
                return _rows_to_df(spark, rows[:INTERNET_TARGET_ROWS])
        except Exception as exc:
            logger.warning(f"[OULAD] base {base} failed: {exc}")

    raise RuntimeError("[OULAD] All sources failed.")


# ─────────────────────────────────────────────────────────────────────────────
# INTERNET LOADER REGISTRY
# ─────────────────────────────────────────────────────────────────────────────

_INTERNET_LOADERS: Dict[str,Callable] = {
    "nyc_taxi_trips":            _load_nyc_taxi,
    "global_stock_market":       _load_stock_market,
    "medicare_claims":           _load_medicare,
    "common_crawl_web_logs":     _load_common_crawl,
    "amazon_product_reviews":    _load_amazon_reviews,
    "noaa_climate_data":         _load_noaa_climate,
    "openstreetmap_pois":        _load_osm_pois,
    "us_census_acs":             _load_census_acs,
    "twitter_streaming_dataset": _load_twitter,
    "kaggle_open_university":    _load_oulad,
}


# ─────────────────────────────────────────────────────────────────────────────
# SYNTHETIC PIPELINE  (Priority 2)
#
# Data is generated INSIDE Spark executors — driver RAM stays near zero.
# Written to /tmp Parquet so Spark reads it with real partitioned I/O.
# /tmp is deleted immediately after the DataFrame is cached.
#
#   driver RAM used   : schema only (~KB)
#   executor RAM used : ~few MB per partition (20 partitions × 100k rows)
#   peak /tmp usage   : ~300-500MB (deleted after cache)
#   device risk       : NONE — well within 15GB budget
# ─────────────────────────────────────────────────────────────────────────────

def _synthetic_via_spark(spark: SparkSession, meta: dict) -> DataFrame:
    """
    Generates synthetic data inside Spark executors.
    Writes to /tmp Parquet → reads back → caches → deletes /tmp.
    """
    name     = meta["dataset_name"]
    schema   = _SCHEMAS.get(name, _schema_generic)(meta)
    gen_fn   = _GENERATORS.get(name, _gen_generic)
    rows_per = SYNTHETIC_TOTAL_ROWS // SYNTHETIC_PARTITIONS

    meta_bc = spark.sparkContext.broadcast(meta)

    def _partition(partition_idx: int):
        import random as _rng
        rng  = _rng.Random(42 + partition_idx)
        _m   = meta_bc.value
        _gen = gen_fn
        for _ in range(rows_per):
            yield _gen(rng, _m)

    rdd = (
        spark.sparkContext
             .parallelize(range(SYNTHETIC_PARTITIONS), SYNTHETIC_PARTITIONS)
             .flatMap(_partition)
    )

    df = spark.createDataFrame(rdd, schema=schema)

    tmp_dir     = tempfile.mkdtemp(prefix="sparkinsight_synth_")
    parquet_path = os.path.join(tmp_dir, "part.parquet")

    try:
        logger.info(
            f"[SYNTHETIC] Writing {SYNTHETIC_TOTAL_ROWS:,} rows "
            f"({SYNTHETIC_PARTITIONS} partitions) to {parquet_path} ..."
        )
        df.write.mode("overwrite").parquet(parquet_path)

        logger.info(f"[SYNTHETIC] Spark reading back from {parquet_path} ...")
        df_final = spark.read.parquet(parquet_path)

        # Materialise cache BEFORE deleting /tmp
        df_final = df_final.cache()
        df_final.count()

        logger.info(
            f"[SYNTHETIC] '{name}': {SYNTHETIC_TOTAL_ROWS:,} rows cached, "
            f"{df_final.rdd.getNumPartitions()} partitions"
        )
        return df_final

    finally:
        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            logger.info(f"[SYNTHETIC] /tmp cleaned: {tmp_dir}")
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# SCHEMAS  (one StructType per dataset — used by synthetic pipeline)
# ─────────────────────────────────────────────────────────────────────────────

def _F(name, t): return StructField(name, t, True)
S = StringType
D = DoubleType

def _schema_nyc_taxi(_): return StructType([
    _F("vendor_id",S()), _F("tpep_pickup_datetime",S()),
    _F("tpep_dropoff_datetime",S()), _F("passenger_count",D()),
    _F("trip_distance",D()), _F("rate_code_id",D()),
    _F("store_and_fwd_flag",S()), _F("pu_location_id",D()),
    _F("do_location_id",D()), _F("payment_type",S()),
    _F("fare_amount",D()), _F("extra",D()), _F("mta_tax",D()),
    _F("tip_amount",D()), _F("tolls_amount",D()),
    _F("improvement_surcharge",D()), _F("total_amount",D()),
    _F("congestion_surcharge",D()),
])

def _schema_stock(_): return StructType([
    _F("ticker",S()), _F("exchange",S()), _F("security_name",S()),
    _F("trade_date",S()), _F("open_price",D()), _F("high_price",D()),
    _F("low_price",D()), _F("close_price",D()), _F("adj_close",D()),
    _F("volume",D()), _F("market_cap",D()), _F("sector",S()),
    _F("industry",S()), _F("country",S()),
])

def _schema_medicare(_): return StructType([
    _F("beneficiary_code",S()), _F("claim_id",S()), _F("provider_id",S()),
    _F("claim_from_date",S()), _F("claim_thru_date",S()),
    _F("diagnosis_code_1",S()), _F("diagnosis_code_2",S()),
    _F("procedure_code",S()), _F("claim_payment_amount",D()),
    _F("deductible_amount",D()), _F("carrier_line_allowed_charge",D()),
    _F("hcpcs_code",S()), _F("claim_type",S()), _F("state_code",S()),
])

def _schema_web_logs(_): return StructType([
    _F("url",S()), _F("fetch_status",S()), _F("content_type",S()),
    _F("crawl_timestamp",S()), _F("response_bytes",D()),
    _F("redirect_url",S()), _F("detected_language",S()),
    _F("mime_type",S()), _F("warc_filename",S()),
    _F("warc_record_offset",D()), _F("tld",S()), _F("domain",S()),
])

def _schema_amazon(_): return StructType([
    _F("reviewer_id",S()), _F("asin",S()), _F("product_title",S()),
    _F("product_category",S()), _F("star_rating",D()),
    _F("helpful_votes",D()), _F("total_votes",D()), _F("vine",S()),
    _F("verified_purchase",S()), _F("review_headline",S()),
    _F("review_body",S()), _F("review_date",S()),
    _F("marketplace",S()), _F("customer_id",S()),
])

def _schema_noaa(_): return StructType([
    _F("station_id",S()), _F("station_name",S()), _F("latitude",D()),
    _F("longitude",D()), _F("elevation",D()), _F("country",S()),
    _F("state",S()), _F("observation_date",S()), _F("temperature_max",D()),
    _F("temperature_min",D()), _F("precipitation",D()),
    _F("snow_depth",D()), _F("wind_speed",D()), _F("cloud_cover",D()),
    _F("data_quality_flag",S()),
])

def _schema_osm(_): return StructType([
    _F("osm_id",S()), _F("osm_type",S()), _F("name",S()),
    _F("amenity",S()), _F("shop",S()), _F("tourism",S()),
    _F("leisure",S()), _F("natural",S()), _F("latitude",D()),
    _F("longitude",D()), _F("country",S()), _F("city",S()),
    _F("postcode",S()), _F("last_updated",S()), _F("tag_count",D()),
])

def _schema_census(_): return StructType([
    _F("geo_id",S()), _F("state_fips",S()), _F("county_fips",S()),
    _F("tract_fips",S()), _F("survey_year",S()),
    _F("total_population",D()), _F("median_age",D()),
    _F("median_household_income",D()), _F("per_capita_income",D()),
    _F("poverty_rate",D()), _F("unemployment_rate",D()),
    _F("education_bachelors_pct",D()), _F("owner_occupied_pct",D()),
    _F("median_home_value",D()), _F("gini_index",D()),
    _F("race_white_pct",D()), _F("race_black_pct",D()),
    _F("hispanic_pct",D()),
])

def _schema_twitter(_): return StructType([
    _F("tweet_id",S()), _F("user_id",S()), _F("screen_name",S()),
    _F("created_at",S()), _F("text",S()), _F("retweet_count",D()),
    _F("favorite_count",D()), _F("reply_count",D()), _F("quote_count",D()),
    _F("lang",S()), _F("source_device",S()), _F("is_retweet",S()),
    _F("hashtags",S()), _F("user_followers",D()), _F("user_following",D()),
    _F("user_verified",S()), _F("geo_country",S()),
])

def _schema_oulad(_): return StructType([
    _F("id_student",S()), _F("code_module",S()),
    _F("code_presentation",S()), _F("id_assessment",S()),
    _F("assessment_type",S()), _F("date_submitted",S()),
    _F("is_banked",S()), _F("score",D()), _F("gender",S()),
    _F("region",S()), _F("highest_education",S()), _F("imd_band",S()),
    _F("age_band",S()), _F("num_of_prev_attempts",D()),
    _F("studied_credits",D()), _F("disability",S()), _F("final_result",S()),
])

def _schema_generic(meta): return StructType([
    _F(c, D() if any(k in c.lower() for k in [
        "amount","price","rate","count","score","pct","bytes",
        "dist","lat","lon","elev","pop","income","age","credits","total"
    ]) else S())
    for c in meta.get("columns",["id","value","category","timestamp"])
])

_SCHEMAS: Dict[str,Callable] = {
    "nyc_taxi_trips":            _schema_nyc_taxi,
    "global_stock_market":       _schema_stock,
    "medicare_claims":           _schema_medicare,
    "common_crawl_web_logs":     _schema_web_logs,
    "amazon_product_reviews":    _schema_amazon,
    "noaa_climate_data":         _schema_noaa,
    "openstreetmap_pois":        _schema_osm,
    "us_census_acs":             _schema_census,
    "twitter_streaming_dataset": _schema_twitter,
    "kaggle_open_university":    _schema_oulad,
}


# ─────────────────────────────────────────────────────────────────────────────
# ROW GENERATORS  (run inside Spark executors — all imports must be local)
# ─────────────────────────────────────────────────────────────────────────────

def _gen_nyc_taxi(rng, meta):
    from pyspark.sql import Row
    from datetime import datetime, timedelta
    base = datetime(2020,1,1)
    pts  = ["Credit card","Cash","No charge","Dispute"]
    pu   = base + timedelta(seconds=rng.randint(0,63072000))
    do_  = pu   + timedelta(minutes=rng.randint(3,90))
    fare = round(rng.uniform(5,80),2)
    tip  = round(fare*rng.uniform(0,0.3),2)
    return Row(
        vendor_id=rng.choice(["1","2"]),
        tpep_pickup_datetime=pu.strftime("%Y-%m-%d %H:%M:%S"),
        tpep_dropoff_datetime=do_.strftime("%Y-%m-%d %H:%M:%S"),
        passenger_count=float(rng.randint(1,6)),
        trip_distance=round(rng.uniform(0.5,30),2),
        rate_code_id=float(rng.randint(1,6)),
        store_and_fwd_flag=rng.choice(["Y","N"]),
        pu_location_id=float(rng.randint(1,265)),
        do_location_id=float(rng.randint(1,265)),
        payment_type=rng.choice(pts),
        fare_amount=fare, extra=round(rng.uniform(0,2.5),2),
        mta_tax=0.5, tip_amount=tip,
        tolls_amount=round(rng.uniform(0,6),2),
        improvement_surcharge=0.3,
        total_amount=round(fare+tip+0.8,2),
        congestion_surcharge=rng.choice([0.0,2.5]),
    )

def _gen_stock(rng, meta):
    from pyspark.sql import Row
    from datetime import datetime, timedelta
    tickers = ["AAPL","MSFT","GOOGL","AMZN","NVDA","META","TSLA","JPM","V","XOM"]
    sectors = {"AAPL":"Technology","MSFT":"Technology","GOOGL":"Technology",
               "AMZN":"Consumer Discretionary","NVDA":"Technology",
               "META":"Communication Services","TSLA":"Consumer Discretionary",
               "JPM":"Financials","V":"Financials","XOM":"Energy"}
    t = rng.choice(tickers)
    dt = datetime(2019,1,1) + timedelta(days=rng.randint(0,1825))
    p  = round(rng.uniform(50,500),2)
    return Row(
        ticker=t, exchange="NASDAQ", security_name=f"{t} Inc.",
        trade_date=dt.strftime("%Y-%m-%d"),
        open_price=p, high_price=round(p*rng.uniform(1.0,1.03),2),
        low_price=round(p*rng.uniform(0.97,1.0),2),
        close_price=round(p*rng.uniform(0.98,1.02),2), adj_close=p,
        volume=float(rng.randint(1_000_000,50_000_000)),
        market_cap=float(p*rng.randint(500_000_000,3_000_000_000)),
        sector=sectors.get(t,"Other"), industry=sectors.get(t,"Other"),
        country="USA",
    )

def _gen_medicare(rng, meta):
    from pyspark.sql import Row
    from datetime import datetime, timedelta
    types  = ["Inpatient","Outpatient","Carrier","DME","Part D"]
    states = ["CA","TX","NY","FL","PA","IL","OH","GA","NC","MI"]
    diag   = ["Z00.00","I10","E11.9","J18.9","I25.10","N18.3"]
    fd     = datetime(2020,1,1) + timedelta(days=rng.randint(0,1095))
    return Row(
        beneficiary_code=f"B{rng.randint(1000000,9999999)}",
        claim_id=f"CLM{rng.randint(10000000,99999999)}",
        provider_id=f"P{rng.randint(100000,999999)}",
        claim_from_date=fd.strftime("%Y-%m-%d"),
        claim_thru_date=(fd+timedelta(days=rng.randint(0,5))).strftime("%Y-%m-%d"),
        diagnosis_code_1=rng.choice(diag), diagnosis_code_2=rng.choice(diag),
        procedure_code=f"{rng.randint(10000,99999)}",
        claim_payment_amount=round(rng.uniform(50,25000),2),
        deductible_amount=round(rng.uniform(0,1500),2),
        carrier_line_allowed_charge=round(rng.uniform(50,20000),2),
        hcpcs_code=f"{rng.randint(10000,99999)}",
        claim_type=rng.choice(types), state_code=rng.choice(states),
    )

def _gen_web_logs(rng, meta):
    from pyspark.sql import Row
    from datetime import datetime, timedelta
    cts  = ["text/html","application/json","text/javascript","image/jpeg","text/css"]
    st   = ["200","200","200","301","404","500"]
    tlds = [".com",".org",".net",".edu",".io"]
    ts   = datetime(2024,1,1) + timedelta(seconds=rng.randint(0,7776000))
    ct   = rng.choice(cts); tld = rng.choice(tlds)
    return Row(
        url=f"https://site{rng.randint(1,10000)}{tld}/p/{rng.randint(1,100000)}",
        fetch_status=rng.choice(st), content_type=ct,
        crawl_timestamp=ts.strftime("%Y-%m-%d %H:%M:%S"),
        response_bytes=float(rng.randint(512,5_000_000)),
        redirect_url="", detected_language=rng.choice(["en","es","fr","de","zh"]),
        mime_type=ct,
        warc_filename=f"CC-MAIN-2024-{rng.randint(1,52):02d}.warc.gz",
        warc_record_offset=float(rng.randint(0,1_000_000_000)),
        tld=tld, domain=f"site{rng.randint(1,10000)}{tld}",
    )

def _gen_amazon(rng, meta):
    from pyspark.sql import Row
    from datetime import datetime, timedelta
    cats = ["Electronics","Clothing","Books","Home & Kitchen","Sports",
            "Beauty","Toys","Health","Music","Movies"]
    adj  = ["Great","Excellent","Poor","Average","Amazing","Good"]
    dt   = datetime(2015,1,1) + timedelta(days=rng.randint(0,3285))
    s    = rng.choices([1,2,3,4,5], weights=[5,8,12,30,45])[0]
    h    = rng.randint(0,500)
    return Row(
        reviewer_id=f"RUSER{rng.randint(100000,999999)}",
        asin=f"B{rng.randint(10000000,99999999):09d}",
        product_title=f"{rng.choice(adj)} Product {rng.randint(1,1000)}",
        product_category=rng.choice(cats), star_rating=float(s),
        helpful_votes=float(h), total_votes=float(h+rng.randint(0,50)),
        vine=rng.choice(["Y","N"]), verified_purchase=rng.choice(["Y","N"]),
        review_headline=f"{rng.choice(adj)} purchase", review_body="",
        review_date=dt.strftime("%Y-%m-%d"),
        marketplace=rng.choice(["US","UK","DE","JP"]),
        customer_id=f"CUST{rng.randint(100000,999999)}",
    )

def _gen_noaa(rng, meta):
    from pyspark.sql import Row
    from datetime import datetime, timedelta
    countries = ["US","DE","FR","UK","JP","AU","BR","IN","CN","CA"]
    dt = datetime(2010,1,1) + timedelta(days=rng.randint(0,5110))
    return Row(
        station_id=f"USW{rng.randint(10000,99999)}",
        station_name=f"Station {rng.randint(1,5000)}",
        latitude=round(rng.uniform(-90,90),4),
        longitude=round(rng.uniform(-180,180),4),
        elevation=round(rng.uniform(0,4000),1),
        country=rng.choice(countries), state="",
        observation_date=dt.strftime("%Y-%m-%d"),
        temperature_max=round(rng.uniform(-30,45),1),
        temperature_min=round(rng.uniform(-40,35),1),
        precipitation=round(rng.uniform(0,200),1),
        snow_depth=round(rng.uniform(0,100),1),
        wind_speed=round(rng.uniform(0,120),1),
        cloud_cover=round(rng.uniform(0,100),1),
        data_quality_flag=rng.choice(["","D","G","I"]),
    )

def _gen_osm(rng, meta):
    from pyspark.sql import Row
    from datetime import datetime, timedelta
    ams  = ["restaurant","cafe","hospital","school","bank",
            "pharmacy","fuel","hotel","supermarket","library"]
    ctrs = ["US","DE","FR","UK","JP","AU","BR","IN","CN","CA"]
    dt   = datetime(2020,1,1) + timedelta(days=rng.randint(0,1460))
    return Row(
        osm_id=str(rng.randint(100000000,999999999)),
        osm_type=rng.choice(["node","way"]),
        name=f"Place {rng.randint(1,50000)}",
        amenity=rng.choice(ams),
        shop=rng.choice(["supermarket","clothes",""]),
        tourism=rng.choice(["hotel","museum",""]),
        leisure=rng.choice(["park","playground",""]),
        natural=rng.choice(["wood","water",""]),
        latitude=round(rng.uniform(-90,90),6),
        longitude=round(rng.uniform(-180,180),6),
        country=rng.choice(ctrs),
        city=f"City {rng.randint(1,1000)}",
        postcode=f"{rng.randint(10000,99999)}",
        last_updated=dt.strftime("%Y-%m-%d"),
        tag_count=float(rng.randint(1,50)),
    )

def _gen_census(rng, meta):
    from pyspark.sql import Row
    fp  = [f"{i:02d}" for i in range(1,57) if i not in [3,7,11,14,43,52]]
    st  = rng.choice(fp)
    pop = rng.randint(500,80000)
    inc = rng.randint(25000,120000)
    return Row(
        geo_id=f"{st}{rng.randint(100,999)}{rng.randint(100000,999999):06d}",
        state_fips=st, county_fips=f"{rng.randint(1,999):03d}",
        tract_fips=f"{rng.randint(100000,999999):06d}",
        survey_year=str(rng.randint(2015,2022)),
        total_population=float(pop), median_age=round(rng.uniform(25,50),1),
        median_household_income=float(inc),
        per_capita_income=float(inc//rng.randint(2,4)),
        poverty_rate=round(rng.uniform(3,40),1),
        unemployment_rate=round(rng.uniform(2,20),1),
        education_bachelors_pct=round(rng.uniform(10,70),1),
        owner_occupied_pct=round(rng.uniform(20,80),1),
        median_home_value=float(rng.randint(80000,800000)),
        gini_index=round(rng.uniform(0.3,0.6),4),
        race_white_pct=round(rng.uniform(20,95),1),
        race_black_pct=round(rng.uniform(1,60),1),
        hispanic_pct=round(rng.uniform(2,70),1),
    )

def _gen_twitter(rng, meta):
    from pyspark.sql import Row
    from datetime import datetime, timedelta
    langs   = ["en","es","fr","de","pt","ja","ar","tr","ru","hi","ko"]
    devs    = ["Twitter Web App","iPhone","Android","TweetDeck"]
    topics  = ["news","sports","tech","politics","entertainment","science"]
    ts      = datetime(2020,1,1) + timedelta(seconds=rng.randint(0,94608000))
    return Row(
        tweet_id=str(rng.randint(10**18,10**19-1)),
        user_id=str(rng.randint(100000,9999999999)),
        screen_name=f"user_{rng.randint(1000,999999)}",
        created_at=ts.strftime("%Y-%m-%d %H:%M:%S"),
        text=f"Post about {rng.choice(topics)} #{rng.choice(topics)}",
        retweet_count=float(rng.randint(0,50000)),
        favorite_count=float(rng.randint(0,200000)),
        reply_count=float(rng.randint(0,5000)),
        quote_count=float(rng.randint(0,2000)),
        lang=rng.choice(langs), source_device=rng.choice(devs),
        is_retweet=str(rng.random()<0.3), hashtags=rng.choice(topics),
        user_followers=float(rng.randint(0,10000000)),
        user_following=float(rng.randint(0,5000)),
        user_verified=str(rng.random()<0.02),
        geo_country=rng.choice(["US","GB","DE","FR","JP","BR","IN","CA"]),
    )

def _gen_oulad(rng, meta):
    from pyspark.sql import Row
    from datetime import datetime, timedelta
    mods  = ["AAA","BBB","CCC","DDD","EEE","FFF","GGG"]
    pres  = ["2013B","2013J","2014B","2014J"]
    types = ["TMA","CMA","Exam"]
    regs  = ["London Region","South East Region","North Western Region","Wales"]
    edus  = ["HE Qualification","A Level or Equivalent","Lower Than A Level"]
    imds  = [f"{i*10}-{(i+1)*10}%" for i in range(10)]
    res   = ["Pass","Fail","Distinction","Withdrawn"]
    dt    = datetime(2013,1,1) + timedelta(days=rng.randint(0,270))
    return Row(
        id_student=str(rng.randint(100000,999999)),
        code_module=rng.choice(mods),
        code_presentation=rng.choice(pres),
        id_assessment=str(rng.randint(1000,9999)),
        assessment_type=rng.choice(types),
        date_submitted=dt.strftime("%Y-%m-%d"),
        is_banked=str(rng.randint(0,1)),
        score=round(rng.uniform(0,100),1),
        gender=rng.choice(["M","F"]),
        region=rng.choice(regs),
        highest_education=rng.choice(edus),
        imd_band=rng.choice(imds),
        age_band=rng.choice(["0-35","35-55","55<="]),
        num_of_prev_attempts=float(rng.randint(0,3)),
        studied_credits=float(rng.choice([60,120,180,240])),
        disability=rng.choice(["Y","N"]),
        final_result=rng.choice(res),
    )

def _gen_generic(rng, meta):
    from pyspark.sql import Row
    from datetime import datetime, timedelta
    cols    = meta.get("columns",["id","value","category","timestamp"])
    primary = meta.get("primary_metric","value")
    t_col   = meta.get("time_column")
    c_col   = meta.get("category_column")
    cats    = ["TypeA","TypeB","TypeC","TypeD","TypeE"]
    base    = datetime(2020,1,1)
    row     = {}
    for col in cols:
        if col == t_col:
            row[col] = (base+timedelta(days=rng.randint(0,1460))).strftime("%Y-%m-%d")
        elif col == c_col:
            row[col] = rng.choice(cats)
        elif col == primary:
            row[col] = round(rng.uniform(1,10000),2)
        elif any(k in col.lower() for k in ["id","code","flag","type","status","name"]):
            row[col] = f"VAL_{rng.randint(1,9999)}"
        elif any(k in col.lower() for k in ["date","time","year"]):
            row[col] = (base+timedelta(days=rng.randint(0,1460))).strftime("%Y-%m-%d")
        elif any(k in col.lower() for k in ["pct","rate","score","amount","price",
                                              "bytes","lat","lon","count","total"]):
            row[col] = round(rng.uniform(0,100000),2)
        else:
            row[col] = f"VAL_{rng.randint(1,9999)}"
    return Row(**row)


_GENERATORS: Dict[str,Callable] = {
    "nyc_taxi_trips":            _gen_nyc_taxi,
    "global_stock_market":       _gen_stock,
    "medicare_claims":           _gen_medicare,
    "common_crawl_web_logs":     _gen_web_logs,
    "amazon_product_reviews":    _gen_amazon,
    "noaa_climate_data":         _gen_noaa,
    "openstreetmap_pois":        _gen_osm,
    "us_census_acs":             _gen_census,
    "twitter_streaming_dataset": _gen_twitter,
    "kaggle_open_university":    _gen_oulad,
}