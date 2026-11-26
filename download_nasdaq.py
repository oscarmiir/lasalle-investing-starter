"""Segunda fuente de precios: histórico diario de Nasdaq.com, para validar los cierres de Yahoo.

    uv run python download_nasdaq.py --start 2024-11-01 --end 2026-11-25

Guarda la respuesta literal en lake/raw/nasdaq/<descarga>/<TICKER>.json con un manifest.json
(URL, hora, SHA-256, clase de activo). Es un acceso web no oficial y sin clave: cubre acciones y
ETF que cotizan en EE. UU.; no sirve para bolsas de otros países. Precios sin ajustar (Close/Last).
"""
import argparse
import hashlib
import json
import time
import urllib.parse
import urllib.request
from datetime import date, datetime, timezone

from common import ROOT, save_json, settings, utc_now

ENDPOINT = "https://api.nasdaq.com/api/quote/{ticker}/historical"
HEADERS = {"User-Agent": "Mozilla/5.0 LaSalleCourse", "Accept": "application/json"}


def fetch(ticker, start, end, asset_class, attempts=3):
    query = urllib.parse.urlencode({"assetclass": asset_class, "fromdate": start.isoformat(),
                                    "todate": end.isoformat(), "limit": 9999})
    url = ENDPOINT.format(ticker=urllib.parse.quote(ticker, safe="")) + "?" + query
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=HEADERS), timeout=30) as response:
                return url, response.read()
        except OSError:
            if attempt == attempts:
                raise
            time.sleep(2 * attempt)


def download(start, end, output=ROOT / "lake/raw/nasdaq"):
    _, tickers = settings()
    destination = output / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    destination.mkdir(parents=True, exist_ok=False)
    files, missing = [], []
    for ticker in tickers:
        for asset_class in ("stocks", "etf"):
            url, body = fetch(ticker, start, end, asset_class)
            data = json.loads(body).get("data") or {}
            if (data.get("tradesTable") or {}).get("rows"):
                break
        else:
            missing.append(ticker)  # Sin cobertura en esta fuente: se anota y no se guarda una respuesta vacía.
            continue
        (destination / f"{ticker}.json").write_bytes(body)  # Respuesta literal, antes de transformar.
        files.append({"ticker": ticker, "file": f"{ticker}.json", "url": url, "provider": "Nasdaq.com historical",
                      "asset_class": asset_class, "downloaded_at": utc_now(), "sha256": hashlib.sha256(body).hexdigest(),
                      "requested_start": start.isoformat(), "requested_end": end.isoformat()})
        time.sleep(0.5)
    save_json(destination / "manifest.json", {"source": "Nasdaq.com historical", "files": files, "not_covered": missing,
                                              "note": "Acceso web no oficial; cierres sin ajustar; usado para validar Yahoo."})
    print(f"Raw conservado en {destination}; sin cobertura: {', '.join(missing) or 'ninguno'}")
    return destination


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    args = parser.parse_args()
    if args.start >= args.end:
        parser.error("start debe ser anterior a end")
    download(args.start, args.end)
