"""Crea copias de prueba a partir de originales raw reales, sin modificarlos, con errores inducidos.

    uv run python tests/fixtures/crear_fixtures.py

Casos (cada uno en tests/fixtures/<caso>/<descarga>/ con su manifest.json):
  filas_invalidas      el ETL termina bien y RECHAZA filas: cierre nulo, precio negativo, volumen negativo,
                       duplicados, fin de semana, fecha futura, fuera de rango, sesión incompleta y
                       símbolo distinto (COIN), valor no finito y metadatos incompletos (HOOD).
                       COIN y HOOD no están en el universo configurado: el ETL termina bien.
  raw_alterado         un original cambia después de calcular su huella: el ETL se DETIENE.
  universo_incompleto  falta un ticker configurado: el ETL se DETIENE y no se actualiza la entrada del publicador.
  fichero_ausente      el manifiesto cita un original que no existe: el ETL se DETIENE.
  nasdaq_discrepante   segunda fuente cuyo último cierre de MU no coincide con Yahoo: el ETL se DETIENE.
"""
import copy
import hashlib
import json
import shutil
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
SOURCE = ROOT / "lake/raw/yahoo/20261126T162237261455Z"
DOWNLOAD = "20261126T000000000000Z"
TICKERS = ("MU", "AMD", "GDX")
SESSIONS = 90


def load(ticker):
    doc = json.loads((SOURCE / f"{ticker}.json").read_bytes())
    result = doc["chart"]["result"][0]
    result["timestamp"] = result["timestamp"][-SESSIONS:]
    for block in result["indicators"]["quote"] + result["indicators"]["adjclose"]:
        for key in block:
            block[key] = block[key][-SESSIONS:]
    return doc


def series(doc):
    result = doc["chart"]["result"][0]
    return result, result["indicators"]["quote"][0], result["indicators"]["adjclose"][0]


def set_close(doc, index, value):
    _, quote, adj = series(doc)
    quote["close"][index] = value
    adj["adjclose"][index] = value


def insert_bar(doc, position, timestamp, source_index, price=None):
    result, quote, adj = series(doc)
    result["timestamp"].insert(position, timestamp)
    for block in (quote, adj):
        for key, values in block.items():
            value = values[source_index]
            if price is not None and key in ("close", "adjclose"):
                value = price
            values.insert(position, value)


def write_case(name, docs, alter=None, remove=None):
    folder = HERE / name / DOWNLOAD
    shutil.rmtree(HERE / name, ignore_errors=True)
    folder.mkdir(parents=True)
    original = json.loads((SOURCE / "manifest.json").read_text(encoding="utf-8"))
    files = []
    for ticker, doc in docs.items():
        body = json.dumps(doc, separators=(",", ":")).encode("utf-8")
        (folder / f"{ticker}.json").write_bytes(body)
        entry = next(item for item in original["files"] if item["ticker"] == ticker)
        files.append({**entry, "sha256": hashlib.sha256(body).hexdigest()})
    if alter:
        path = folder / f"{alter}.json"
        path.write_bytes(path.read_bytes().replace(b'"currency":"USD"', b'"currency":"EUR"'))
    if remove:
        (folder / f"{remove}.json").unlink()
    manifest = {"source": "Fixture de prueba derivado de " + SOURCE.name, "files": files,
                "note": "Copia de prueba con errores inducidos; no es un original del proveedor."}
    (folder / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")


def main():
    base = {ticker: load(ticker) for ticker in TICKERS}

    invalid = copy.deepcopy(base)
    invalid["COIN"] = load("COIN")
    series(invalid["COIN"])[0]["meta"]["symbol"] = "OTRO"               # simbolo_distinto (todas sus filas)
    mu, _, _ = series(invalid["MU"])
    mu["meta"]["currentTradingPeriod"]["regular"]["end"] = 1795723200   # 2026-11-26 20:00 UTC, tras la descarga (16:22 UTC)
    insert_bar(invalid["MU"], len(mu["timestamp"]), 1795700000, -1)    # sesion_incompleta (2026-11-26)
    insert_bar(invalid["MU"], 0, 1729000000, 0)                         # fuera_de_rango (2024-10-15)
    series(invalid["AMD"])[1]["volume"][50] = -5                        # volumen_negativo
    set_close(invalid["MU"], 30, float("nan"))                          # valor_no_finito (NaN en el JSON)
    invalid["HOOD"] = load("HOOD")
    del series(invalid["HOOD"])[0]["meta"]["exchangeTimezoneName"]     # metadatos_incompletos (todas sus filas)
    set_close(invalid["MU"], 10, None)                       # cierre_nulo
    set_close(invalid["MU"], 20, -1.0)                       # precio_no_positivo
    amd, _, _ = series(invalid["AMD"])
    insert_bar(invalid["AMD"], 31, amd["timestamp"][30], 30)                    # duplicado_identico
    insert_bar(invalid["AMD"], 42, amd["timestamp"][40], 40, price=999.0)       # duplicado_con_precios_distintos (x2)
    fridays = [i for i, ts in enumerate(amd["timestamp"]) if (ts // 86400 + 4) % 7 == 5]
    friday = fridays[3]
    insert_bar(invalid["AMD"], friday + 1, amd["timestamp"][friday] + 86400, friday)  # fin_de_semana
    gdx, _, _ = series(invalid["GDX"])
    insert_bar(invalid["GDX"], len(gdx["timestamp"]), gdx["timestamp"][-1] + 30 * 86400, -1)  # fecha_futura
    write_case("filas_invalidas", invalid)

    write_case("raw_alterado", copy.deepcopy(base), alter="GDX")
    write_case("universo_incompleto", {t: copy.deepcopy(base[t]) for t in ("MU", "AMD")})
    write_case("fichero_ausente", copy.deepcopy(base), remove="AMD")
    write_second_source("nasdaq_discrepante", ticker="MU", factor=1.05)


def write_second_source(name, ticker, factor):
    """Copia del raw de Nasdaq con el cierre más reciente de un ticker alterado."""
    source = sorted((ROOT / "lake/raw/nasdaq").glob("*/manifest.json"))[-1].parent
    folder = HERE / name / DOWNLOAD
    shutil.rmtree(HERE / name, ignore_errors=True)
    folder.mkdir(parents=True)
    original = json.loads((source / "manifest.json").read_text(encoding="utf-8"))
    files = []
    for entry in original["files"]:
        if entry["ticker"] not in TICKERS:
            continue
        doc = json.loads((source / entry["file"]).read_bytes())
        if entry["ticker"] == ticker:
            row = doc["data"]["tradesTable"]["rows"][0]  # Nasdaq ordena de más reciente a más antiguo.
            row["close"] = "${:.2f}".format(float(row["close"].strip("$").replace(",", "")) * factor)
        body = json.dumps(doc, separators=(",", ":")).encode("utf-8")
        (folder / entry["file"]).write_bytes(body)
        files.append({**entry, "sha256": hashlib.sha256(body).hexdigest()})
    manifest = {"source": "Fixture de prueba derivado de " + source.name, "files": files,
                "note": "Copia de prueba con el último cierre alterado; no es un original del proveedor."}
    (folder / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print("Fixtures creados en", HERE)


if __name__ == "__main__":
    main()
