"""Comparación medida de particionados de clean/prices con PySpark.

    MSYS_NO_PATHCONV=1 docker compose run --rm --entrypoint /opt/spark/bin/spark-submit etl \
        --master local[2] /workspace/scripts/benchmark_particiones.py

Escribe tres disposiciones (sin partición, year/month y ticker) para dos volúmenes: los datos reales
(lake/clean/prices) y una réplica ampliada x200 (tickers sintéticos) que muestra la tendencia cuando el
volumen deja de ser trivial. Mide escritura, número de ficheros y tres consultas típicas del proyecto
(mediana de varias repeticiones). Resultado: reports/benchmark_particiones.json
"""
import json
import os
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, "/workspace")
from pyspark.sql import SparkSession, functions as F  # noqa: E402

from common import ROOT, save_json, utc_now  # noqa: E402

WORK = "/tmp/benchmark"
REPEAT = 5
LAYOUTS = {"sin_particion": [], "year_month": ["year", "month"], "ticker": ["ticker"]}


def timed(action):
    start = time.perf_counter()
    result = action()
    return time.perf_counter() - start, result


def files_and_bytes(path):
    paths = [p for p in Path(path).rglob("*.parquet")]
    return len(paths), sum(p.stat().st_size for p in paths)


def queries(spark, path, ticker, month):
    df = lambda: spark.read.parquet(path)  # noqa: E731 - lectura nueva en cada consulta (sin caché)
    return {
        "un_ticker_historico": lambda: df().filter(F.col("ticker") == ticker).agg(F.count("*"), F.avg("adjclose")).collect(),
        "un_mes_todos": lambda: df().filter((F.col("year") == month[0]) & (F.col("month") == month[1])).agg(F.count("*")).collect(),
        "recuento_total": lambda: df().count(),
    }


def scaled(base, copies):
    """Réplica x200 con tickers sintéticos y ruido determinista en precios y volumen,
    para que la compresión de Parquet no se beneficie de valores repetidos."""
    noise = (F.abs(F.hash("ticker", "date")) % 1000 - 500) / 1e5
    return (base.crossJoin(copies).withColumn("ticker", F.concat("ticker", F.lit("_"), F.col("copy").cast("string")))
            .withColumn("adjclose", F.col("adjclose") * (1 + noise)).withColumn("close", F.col("close") * (1 + noise))
            .withColumn("volume", F.col("volume") + F.abs(F.hash("ticker", "date")) % 1000).drop("copy"))


def run():
    spark = (SparkSession.builder.master("local[2]").appName("benchmark-particiones")
             .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "4").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    base = spark.read.parquet(str(ROOT / "lake/clean/prices")).select("ticker", "date", "adjclose", "close", "volume", "year", "month")
    copies = spark.range(200).withColumnRenamed("id", "copy")
    datasets = {"real": base,
                "x200": scaled(base, copies)}
    results = []
    for volume, data in datasets.items():
        data = data.cache()
        rows = data.count()
        ticker = data.select("ticker").orderBy("ticker").first()[0]
        month = (2026, 6)
        for layout, partitions in LAYOUTS.items():
            path = f"{WORK}/{volume}/{layout}"
            writer = data.write.mode("overwrite")
            if partitions:
                writer = writer.partitionBy(*partitions)
            write_seconds, _ = timed(lambda: writer.parquet(path))
            files, size = files_and_bytes(path)
            measured = {}
            for name, query in queries(spark, path, ticker, month).items():
                query()  # calentamiento: excluye la primera compilación del plan
                measured[name] = round(statistics.median(timed(query)[0] for _ in range(REPEAT)), 4)
            results.append({"volume": volume, "rows": rows, "layout": layout, "write_s": round(write_seconds, 3),
                            "files": files, "mb": round(size / 1e6, 2), "query_median_s": measured})
            print(json.dumps(results[-1]))
        data.unpersist()
    save_json(ROOT / "reports/benchmark_particiones.json", {
        "generated_at": utc_now(), "engine": "pyspark local[2] (Docker)", "repeat": REPEAT,
        "note": "Mediana de varias ejecuciones tras un calentamiento; réplica x200 con tickers sintéticos.", "results": results})
    spark.stop()


if __name__ == "__main__":
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    run()
