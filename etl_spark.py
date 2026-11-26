"""ETL de RA1 (v2) con PySpark: raw -> clean -> curated, con controles de calidad y puerta de publicación.

Se ejecuta dentro del servicio etl de Docker:  docker compose run --rm etl
Prueba sobre otra copia sin tocar el lake real:
    docker compose run --rm etl --raw-root tests/fixtures/raw_corrupto --lake-root /tmp/lake --out-root /tmp/out

Cada control tiene una acción:
  RECHAZAR    la fila va a lake/quarantine/rejected con su motivo y no llega a clean.
  IDENTIFICAR se anota como aviso en reports/quality.json (no se descarta un dato real).
  DETENER     el ETL termina con error, marca quality.json como failed y no actualiza la entrada del publicador.
"""
import argparse
import hashlib
import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, Window, functions as F, types as T

from common import ROOT, save_json, settings, utc_now
from prepare import write_csv

VERSION = "etl-v2"
MIN_SESSIONS = 61         # return_60 necesita 61 sesiones válidas
MAX_STALE_DAYS = 5        # días naturales entre la última sesión de un ticker y el corte común
JUMP_WARNING = 0.5        # |retorno diario| que se marca para revisión
GAP_WARNING_DAYS = 5      # días naturales entre sesiones consecutivas que se marcan como hueco
CROSS_TOLERANCE = 0.01    # diferencia relativa máxima entre el cierre de Yahoo y el de la segunda fuente

MANIFEST_SCHEMA = T.StructType([
    T.StructField("source", T.StringType()),
    T.StructField("files", T.ArrayType(T.StructType([
        T.StructField("ticker", T.StringType()),
        T.StructField("file", T.StringType()),
        T.StructField("url", T.StringType()),
        T.StructField("provider", T.StringType()),
        T.StructField("downloaded_at", T.StringType()),
        T.StructField("sha256", T.StringType()),
        T.StructField("requested_start", T.StringType()),
        T.StructField("requested_end", T.StringType()),
    ]))),
])

_PERIOD = T.StructType([T.StructField("start", T.LongType()), T.StructField("end", T.LongType())])
CHART_SCHEMA = T.StructType([T.StructField("chart", T.StructType([
    T.StructField("error", T.StringType()),
    T.StructField("result", T.ArrayType(T.StructType([
        T.StructField("meta", T.StructType([
            T.StructField("symbol", T.StringType()),
            T.StructField("currency", T.StringType()),
            T.StructField("exchangeName", T.StringType()),
            T.StructField("exchangeTimezoneName", T.StringType()),
            T.StructField("instrumentType", T.StringType()),
            T.StructField("currentTradingPeriod", T.StructType([T.StructField("regular", _PERIOD)])),
        ])),
        T.StructField("timestamp", T.ArrayType(T.LongType())),
        T.StructField("indicators", T.StructType([
            T.StructField("quote", T.ArrayType(T.StructType([
                T.StructField("close", T.ArrayType(T.DoubleType())),
                T.StructField("volume", T.ArrayType(T.LongType())),
            ]))),
            T.StructField("adjclose", T.ArrayType(T.StructType([
                T.StructField("adjclose", T.ArrayType(T.DoubleType())),
            ]))),
        ])),
    ]))),
]))])


NASDAQ_SCHEMA = T.StructType([T.StructField("data", T.StructType([
    T.StructField("symbol", T.StringType()),
    T.StructField("tradesTable", T.StructType([T.StructField("rows", T.ArrayType(T.StructType([
        T.StructField("date", T.StringType()), T.StructField("close", T.StringType())])))])),
]))])


class QualityError(Exception):
    """Un control de tipo DETENER ha fallado."""


def not_finite(column):
    return F.isnan(column) | column.isin(float("inf"), float("-inf"))


def read_raw_files(spark, raw_root, label):
    """Manifiestos y originales de una fuente, unidos por (descarga, fichero) y con la huella recalculada."""
    manifests = (spark.read.option("multiLine", True).schema(MANIFEST_SCHEMA).json(f"{raw_root}/*/manifest.json")
                 .withColumn("download_id", F.element_at(F.split(F.input_file_name(), "/"), -2)))
    empty = manifests.filter(F.col("files").isNull() | (F.size("files") < 1)).select("download_id").collect()
    if empty:
        raise QualityError(f"Hay manifiestos vacíos o ilegibles en {label}: {', '.join(r.download_id for r in empty)}.")
    entries = manifests.select("download_id", "source", F.explode("files").alias("f")).select("download_id", "source", "f.*")
    raw = (spark.read.format("binaryFile").load(f"{raw_root}/*/*.json")
           .withColumn("download_id", F.element_at(F.split("path", "/"), -2))
           .withColumn("file", F.element_at(F.split("path", "/"), -1))
           .filter(F.col("file") != "manifest.json")
           .select("download_id", "file", "content", F.sha2("content", 256).alias("sha256_actual")))
    return entries.join(raw, ["download_id", "file"], "left")


def raw_problem(*structure_checks):
    """Motivo de DETENER por fichero: ausente, alterado o con estructura inesperada."""
    rule = (F.when(F.col("content").isNull(), "falta_el_fichero")
            .when(F.lower("sha256") != F.col("sha256_actual"), "huella_sha256_distinta"))
    for condition, name in structure_checks:
        rule = rule.when(condition, name)
    return rule


def extract(spark, raw_root, checks):
    """Lee manifiestos y originales; verifica huellas y estructura antes de transformar."""
    try:
        files = read_raw_files(spark, raw_root, "yahoo")
    except QualityError as error:
        checks.append({"name": "integridad_raw", "action": "DETENER", "status": "failed", "detail": str(error)})
        raise

    parsed = files.withColumn("doc", F.from_json(F.col("content").cast("string"), CHART_SCHEMA))
    result = F.col("doc.chart.result")[0]
    parsed = parsed.select(
        "download_id", "ticker", "file", "url", "downloaded_at", "requested_start", "requested_end",
        "sha256", "sha256_actual", "content",
        F.col("doc.chart.error").alias("provider_error"),
        result["meta"]["symbol"].alias("symbol"), result["meta"]["currency"].alias("currency"),
        result["meta"]["exchangeName"].alias("exchange"), result["meta"]["exchangeTimezoneName"].alias("exchange_tz"),
        result["meta"]["instrumentType"].alias("instrument_type"),
        result["meta"]["currentTradingPeriod"]["regular"]["end"].alias("regular_end"),
        result["timestamp"].alias("ts"), result["indicators"]["quote"][0]["close"].alias("closes"),
        result["indicators"]["adjclose"][0]["adjclose"].alias("adjs"),
        result["indicators"]["quote"][0]["volume"].alias("vols"))

    # Controles por fichero (DETENER): un original ausente, alterado o ilegible invalida la ejecución.
    problems = parsed.select("download_id", "file", raw_problem(
        (F.col("provider_error").isNotNull(), "error_del_proveedor"),
        (F.col("ts").isNull() | F.col("closes").isNull() | F.col("adjs").isNull(), "estructura_no_reconocida"),
        (F.size("ts") == 0, "serie_vacia"),
        ((F.size("ts") != F.size("closes")) | (F.size("ts") != F.size("adjs"))
         | (F.col("vols").isNotNull() & (F.size("ts") != F.size("vols"))), "longitudes_distintas"),
    ).alias("problem")).filter(F.col("problem").isNotNull()).collect()
    downloads = [row.asDict() for row in parsed.groupBy("download_id").agg(
        F.count("*").alias("files"), F.min("requested_start").alias("requested_start"),
        F.max("requested_end").alias("requested_end")).orderBy("download_id").collect()]
    checks.append({"name": "integridad_raw", "action": "DETENER", "status": "failed" if problems else "ok",
                   "detail": [f"{p.download_id}/{p.file}: {p.problem}" for p in problems] or
                             f"{sum(d['files'] for d in downloads)} ficheros con huella SHA-256 verificada"})
    if problems:
        raise QualityError("Originales raw ausentes, alterados o ilegibles.")

    rows = (parsed.withColumn("z", F.explode(F.arrays_zip("ts", "closes", "adjs", "vols")))
            .select("download_id", "ticker", "symbol", "currency", "exchange", "exchange_tz", "instrument_type",
                    "downloaded_at", "requested_start", "requested_end", "regular_end",
                    F.col("z.ts").alias("ts"), F.col("z.closes").alias("close"),
                    F.col("z.adjs").alias("adjclose"), F.col("z.vols").alias("volume")))
    return rows, downloads


def transform(rows):
    """Tipos, fechas de sesión en la zona horaria de la bolsa y motivo de rechazo por fila."""
    downloaded = F.col("downloaded_at").cast("timestamp")
    session_end = F.timestamp_seconds("regular_end")
    download_day = F.to_date(F.from_utc_timestamp(downloaded, F.col("exchange_tz")))
    rows = rows.withColumn("date", F.to_date(F.from_utc_timestamp(F.timestamp_seconds("ts"), F.col("exchange_tz"))))
    # Sin el cierre oficial de la sesión no se puede asegurar que la barra del día de descarga sea definitiva.
    still_open = F.coalesce(downloaded < session_end, F.lit(True))
    # Las comparaciones con NULL no deben dejar pasar filas: los metadatos incompletos se rechazan antes.
    reason = (F.when(F.col("symbol").isNull() | F.col("exchange_tz").isNull() | downloaded.isNull()
                     | F.col("requested_start").isNull() | F.col("requested_end").isNull(), "metadatos_incompletos")
              .when(F.col("close").isNull() | F.col("adjclose").isNull(), "cierre_nulo")
              .when(not_finite(F.col("close")) | not_finite(F.col("adjclose")), "valor_no_finito")
              .when((F.col("close") <= 0) | (F.col("adjclose") <= 0), "precio_no_positivo")
              .when(F.col("volume") < 0, "volumen_negativo")
              .when(F.upper("symbol") != F.col("ticker"), "simbolo_distinto")
              .when(F.col("date").isNull(), "fecha_invalida")
              .when(F.col("date") > download_day, "fecha_futura")
              .when((F.col("date") == download_day) & still_open, "sesion_incompleta")
              .when((F.col("date") < F.to_date("requested_start")) | (F.col("date") > F.to_date("requested_end")), "fuera_de_rango")
              .when(F.dayofweek("date").isin(1, 7), "fin_de_semana"))
    rows = rows.withColumn("reason", reason)

    # Duplicados de clave dentro de una descarga: idénticos -> se conserva uno; con precios distintos -> se rechazan todos.
    key = Window.partitionBy("download_id", "ticker", "date")
    valid = rows.filter(F.col("reason").isNull())
    valid = (valid.withColumn("copies", F.count("*").over(key))
             .withColumn("versions", F.size(F.collect_set(F.struct("close", "adjclose", "volume")).over(key)))
             .withColumn("copy", F.row_number().over(key.orderBy("ts")))
             .withColumn("reason", F.when((F.col("copies") > 1) & (F.col("versions") > 1), "duplicado_con_precios_distintos")
                                    .when(F.col("copy") > 1, "duplicado_identico"))
             .drop("copies", "versions", "copy"))
    rows = rows.filter(F.col("reason").isNotNull()).unionByName(valid)
    return rows.withColumn("year", F.year("date")).withColumn("month", F.month("date"))


def select_latest(rows):
    """Por ticker se usa la serie completa de su descarga más reciente (por downloaded_at, antes de filtrar):
    adjclose se recalcula hacia atrás y mezclar descargas daría una serie incoherente. Si esa descarga
    no tiene filas válidas, el ticker queda sin datos y el control de universo detiene el ETL: no se
    recurre en silencio a una descarga anterior."""
    order = Window.partitionBy("ticker").orderBy(F.col("downloaded_at").cast("timestamp").desc(), F.col("download_id").desc())
    latest = (rows.select("ticker", "download_id", "downloaded_at").distinct()
              .withColumn("position", F.row_number().over(order)).filter("position = 1").select("ticker", "download_id"))
    return rows.join(latest, ["ticker", "download_id"], "inner")


def cross_check(spark, check_root, clean, universe, cut, checks, warnings, report):
    """Validación cruzada con una segunda fuente independiente (Nasdaq.com, cierres sin ajustar).

    IDENTIFICAR: cada sesión cuyo cierre difiere más de CROSS_TOLERANCE; cobertura por ticker.
    DETENER: el último cierre del corte difiere en ambas fuentes (es el dato que entra en la selección).
    Si un ticker no está en la segunda fuente (p. ej. bolsas fuera de EE. UU.) se anota y no se detiene.
    """
    if not check_root or (not str(check_root).startswith("s3a://") and not Path(check_root).is_dir()):
        checks.append({"name": "validacion_cruzada", "action": "IDENTIFICAR", "status": "omitido",
                       "detail": f"sin segunda fuente en {check_root}"})
        return
    files = read_raw_files(spark, check_root, "segunda fuente")
    parsed = files.withColumn("doc", F.from_json(F.col("content").cast("string"), NASDAQ_SCHEMA))
    problems = parsed.select("download_id", "file", raw_problem(
        (F.col("doc.data.tradesTable.rows").isNull(), "estructura_no_reconocida"),
        (F.upper("doc.data.symbol") != F.col("ticker"), "simbolo_distinto"),
    ).alias("problem")).filter(F.col("problem").isNotNull()).collect()
    if problems:
        checks.append({"name": "integridad_raw_segunda_fuente", "action": "DETENER", "status": "failed",
                       "detail": [f"{p.download_id}/{p.file}: {p.problem}" for p in problems]})
        raise QualityError("Originales de la segunda fuente ausentes, alterados o ilegibles.")
    second = select_latest(parsed.select("download_id", "ticker", "downloaded_at", F.explode("doc.data.tradesTable.rows").alias("r"))
                           .select("download_id", "ticker", "downloaded_at", F.to_date("r.date", "MM/dd/yyyy").alias("date"),
                                   F.regexp_replace("r.close", "[$,]", "").cast("double").alias("close_2")))
    second = second.filter(F.col("close_2") > 0).select("ticker", "date", "close_2")
    joined = (clean.filter(F.col("ticker").isin(universe)).select("ticker", "date", "close")
              .join(second, ["ticker", "date"], "full_outer")
              .withColumn("diff", F.abs(F.col("close") / F.col("close_2") - 1)))
    stats = {r.ticker: r.asDict() for r in joined.groupBy("ticker").agg(
        F.count(F.when(F.col("diff").isNotNull(), 1)).alias("compared"),
        F.count(F.when(F.col("diff") > CROSS_TOLERANCE, 1)).alias("different"),
        F.count(F.when(F.col("close_2").isNull(), 1)).alias("only_primary"),
        F.count(F.when(F.col("close").isNull(), 1)).alias("only_second"),
        F.max("diff").alias("max_diff")).collect()}
    report["cross_check"] = {"tolerance": CROSS_TOLERANCE, "source": str(check_root),
                             "by_ticker": [{**stats[t], "max_diff": round(stats[t]["max_diff"] or 0, 6)} if t in stats
                                           else {"ticker": t, "compared": 0} for t in universe]}
    for row in joined.filter(F.col("diff") > CROSS_TOLERANCE).orderBy("ticker", "date").limit(50).collect():
        warnings.append({"type": "cierre_distinto_entre_fuentes", "ticker": row.ticker, "date": row.date.isoformat(),
                         "yahoo": row.close, "segunda_fuente": row.close_2, "diff": round(row.diff, 4)})
    uncovered = [t for t in universe if not stats.get(t, {}).get("compared")]
    last = {r.ticker: r for r in joined.join(clean.groupBy("ticker").agg(F.max("date").alias("date")), ["ticker", "date"]).collect()}
    bad_last = [t for t, r in last.items() if r.diff is not None and r.diff > CROSS_TOLERANCE]
    compared = sum(s["compared"] for s in stats.values())
    different = sum(s["different"] for s in stats.values())
    checks.append({"name": "validacion_cruzada", "action": "IDENTIFICAR", "status": "ok",
                   "detail": f"{compared} cierres comparados, {different} fuera de tolerancia; sin cobertura: {', '.join(uncovered) or 'ninguno'}"})
    checks.append({"name": "ultimo_cierre_confirmado", "action": "DETENER", "status": "failed" if bad_last else "ok",
                   "detail": f"{', '.join(bad_last)}: el último cierre no coincide entre fuentes" if bad_last
                             else f"último cierre confirmado en {len(last) - len(uncovered)} tickers"})
    if bad_last:
        raise QualityError("El último cierre no coincide entre las dos fuentes.")


def features(clean):
    ordered = Window.partitionBy("ticker").orderBy("date")
    frame = (clean.withColumn("daily_return", F.col("adjclose") / F.lag("adjclose", 1).over(ordered) - 1)
             .withColumn("gap_days", F.datediff("date", F.lag("date", 1).over(ordered)))
             .withColumn("observations", F.row_number().over(ordered))
             .withColumn("volatility_20", F.stddev_pop("daily_return").over(ordered.rowsBetween(-19, 0))))
    for sessions in (5, 20, 60):
        frame = frame.withColumn(f"return_{sessions}", F.col("adjclose") / F.lag("adjclose", sessions).over(ordered) - 1)
    return frame


def run(raw_root, lake_root, out_root, config, check_root=None):
    student, universe = settings(config)
    out_root = Path(out_root)
    checks, warnings = [], []
    report = {"engine": "pyspark", "version": VERSION, "generated_at": utc_now(), "status": "running",
              "raw_root": str(raw_root), "lake_root": str(lake_root), "universe": universe, "checks": checks, "warnings": warnings}
    spark = (SparkSession.builder.master("local[2]").appName("LaSalle-RA1-ETL-v2")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "4").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        rows, downloads = extract(spark, raw_root, checks)
        report["downloads"] = downloads
        rows = transform(rows).cache()
        rejected = rows.filter(F.col("reason").isNotNull())
        clean = select_latest(rows).filter(F.col("reason").isNull()).select("ticker", "date", "close", "adjclose", "volume", "currency", "exchange",
                                            "exchange_tz", "instrument_type", "download_id", "year", "month").cache()
        read = rows.count()
        reasons = {r["reason"]: r["count"] for r in rejected.groupBy("reason").count().collect()}
        clean_count = clean.count()
        report["counts"] = {"read": read, "rejected": sum(reasons.values()), "rejected_by_reason": reasons,
                            "valid": read - sum(reasons.values()), "superseded_by_newer_download": read - sum(reasons.values()) - clean_count,
                            "clean": clean_count}
        checks.append({"name": "filas_rechazadas", "action": "RECHAZAR", "status": "ok",
                       "detail": reasons or "ninguna fila rechazada"})

        frame = features(clean).cache()
        for row in frame.filter(F.abs("daily_return") > JUMP_WARNING).orderBy("ticker", "date").collect():
            warnings.append({"type": "salto_diario", "ticker": row.ticker, "date": row.date.isoformat(), "daily_return": round(row.daily_return, 4)})
        for row in frame.filter(F.col("gap_days") > GAP_WARNING_DAYS).orderBy("ticker", "date").collect():
            warnings.append({"type": "hueco_entre_sesiones", "ticker": row.ticker, "date": row.date.isoformat(), "days": row.gap_days})
        checks.append({"name": "saltos_y_huecos", "action": "IDENTIFICAR", "status": "ok",
                       "detail": f"{len(warnings)} avisos anotados en warnings"})

        coverage = {row.ticker: row.asDict() for row in clean.groupBy("ticker").agg(
            F.count("*").alias("sessions"), F.min("date").alias("first"), F.max("date").alias("last"),
            F.first("download_id").alias("download_id"), F.first("currency").alias("currency")).collect()}
        report["coverage"] = [{**c, "first": c["first"].isoformat(), "last": c["last"].isoformat()} for _, c in sorted(coverage.items())]

        # DETENER: el universo configurado necesita historia suficiente y datos al día respecto al corte común.
        missing = [t for t in universe if t not in coverage]
        short = [t for t in universe if t in coverage and coverage[t]["sessions"] < MIN_SESSIONS]
        present = [coverage[t]["last"] for t in universe if t in coverage]
        cut = max(present) if present else None
        stale = [t for t in universe if t in coverage and (cut - coverage[t]["last"]).days > MAX_STALE_DAYS]
        for name, bad, text in (("universo_con_datos", missing, "sin datos"), ("historia_minima", short, f"menos de {MIN_SESSIONS} sesiones"),
                                ("datos_al_dia", stale, f"última sesión a más de {MAX_STALE_DAYS} días del corte")):
            checks.append({"name": name, "action": "DETENER", "status": "failed" if bad else "ok",
                           "detail": f"{', '.join(bad)}: {text}" if bad else f"{len(universe)} tickers correctos"})
        if missing or short or stale:
            raise QualityError("El universo configurado no tiene datos suficientes o actuales.")

        cross_check(spark, check_root, clean, universe, cut, checks, warnings, report)

        # IDENTIFICAR: corte alejado del final pedido a la descarga o de la fecha de ejecución.
        requested_end = date.fromisoformat(clean.filter(F.col("ticker").isin(universe))
                                           .join(rows.select("download_id", "requested_end").distinct(), "download_id")
                                           .agg(F.max("requested_end")).first()[0])
        lag_requested = (requested_end - cut).days
        age = (datetime.now(timezone.utc).date() - cut).days
        report["cut_age_days"] = age
        checks.append({"name": "frescura_del_corte", "action": "IDENTIFICAR", "status": "ok",
                       "detail": f"corte {cut.isoformat()}; fin pedido {requested_end.isoformat()}; antigüedad {age} días"})
        if lag_requested > MAX_STALE_DAYS:
            warnings.append({"type": "corte_anterior_al_periodo_pedido", "cut": cut.isoformat(), "requested_end": requested_end.isoformat()})

        latest = Window.partitionBy("ticker").orderBy(F.col("date").desc())
        snapshot = (frame.filter(F.col("ticker").isin(universe)).withColumn("position", F.row_number().over(latest))
                    .filter("position = 1").select("ticker", F.lit(cut).alias("snapshot_date"), F.col("date").alias("last_session"),
                                                    "observations", "return_5", "return_20", "return_60", "volatility_20", "currency"))
        signals = ("return_5", "return_20", "return_60", "volatility_20")
        nulls = snapshot.filter(" OR ".join(f"{s} IS NULL OR isnan({s}) OR abs({s}) = double('inf')" for s in signals)).count()
        checks.append({"name": "senales_completas", "action": "DETENER", "status": "failed" if nulls else "ok",
                       "detail": f"{nulls} tickers con señales nulas" if nulls else "todas las señales calculadas"})
        if nulls:
            raise QualityError("Hay señales nulas en el corte.")

        # Escritura solo después de validar. Parquet particionado y relectura de comprobación.
        daily = frame.select("ticker", "date", "adjclose", "daily_return", "return_5", "return_20", "return_60",
                             "volatility_20", "observations")
        targets = {"clean/prices": (clean, ["year", "month"]), "quarantine/rejected": (rejected.drop("year", "month"), ["reason"]),
                   "curated/features_daily": (daily, ["ticker"]), "curated/features": (snapshot, ["snapshot_date"])}
        expected = {name: df.count() for name, (df, _) in targets.items()}
        for name, (df, partitions) in targets.items():
            df.write.mode("overwrite").partitionBy(*partitions).parquet(f"{lake_root}/{name}")
        # Con esquema explícito: una tabla sin filas (p. ej. ningún rechazo) también se puede releer.
        reread = {name: spark.read.schema(df.schema).parquet(f"{lake_root}/{name}").count() for name, (df, _) in targets.items()}
        bad = [n for n in targets if reread[n] != expected[n]]
        checks.append({"name": "relectura_parquet", "action": "DETENER", "status": "failed" if bad else "ok",
                       "detail": {"escritas": expected, "releidas": reread}})
        report["parquet_reread"] = reread
        if bad:
            raise QualityError("El Parquet releído no coincide con lo escrito.")

        # Exportaciones pequeñas para la API y la trazabilidad (la API no arranca Spark).
        prices = [{**r.asDict(), "date": r.date.isoformat()} for r in
                  clean.select("ticker", "date", "close", "adjclose", "volume", "currency", "download_id").orderBy("ticker", "date").collect()]
        output = [{**r.asDict(), "snapshot_date": r.snapshot_date.isoformat(), "last_session": r.last_session.isoformat()}
                  for r in snapshot.orderBy("ticker").collect()]
        write_csv(out_root / "lake/clean/prices.csv", prices)
        write_csv(out_root / "lake/curated/features.csv", output)
        report.update({"status": "ok", "snapshot_date": cut.isoformat(), "companies": len(output),
                       "features_sha256": hashlib.sha256((out_root / "lake/curated/features.csv").read_bytes()).hexdigest()})
        save_json(out_root / "reports/quality.json", report)
        print(json.dumps({"status": "ok", **report["counts"], "companies": len(output), "snapshot_date": cut.isoformat()}))
    except QualityError as error:
        report.update({"status": "failed", "error": str(error)})
        save_json(out_root / "reports/quality.json", report)
        print(json.dumps({"status": "failed", "error": str(error)}, ensure_ascii=False))
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--raw-root", default=str(ROOT / "lake/raw/yahoo"), help="Carpeta o URI con <descarga>/manifest.json")
    parser.add_argument("--lake-root", default=str(ROOT / "lake"), help="Destino de clean, curated y quarantine (local o s3a://)")
    parser.add_argument("--out-root", default=str(ROOT), help="Carpeta local para los CSV del publicador y reports/")
    parser.add_argument("--config", default=str(ROOT / "config.toml"))
    parser.add_argument("--check-root", default=str(ROOT / "lake/raw/nasdaq"),
                        help="Raw de la segunda fuente para la validación cruzada ('' para omitirla)")
    args = parser.parse_args()
    run(args.raw_root, args.lake_root, args.out_root, args.config, args.check_root)
