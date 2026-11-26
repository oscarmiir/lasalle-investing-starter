"""Exploración de candidatos para el universo (no forma parte del pipeline; no escribe en el lake).

    uv run python scripts/explorar_universo.py

Para cada candidato descarga de Yahoo el `adjclose` diario de 2024-11-01 a 2026-11-25 y mide con qué
frecuencia ha subido al menos un +10 % en 5, 20 y 63 sesiones, su volatilidad anual y sus metadatos
(moneda, tipo, bolsa). Resultado: reports/universo_exploracion.json
"""
import json
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from statistics import pstdev

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from common import save_json, utc_now  # noqa: E402

CANDIDATES = ["NVDA", "AMD", "TSLA", "PLTR", "COIN", "MSTR", "SMCI", "MU", "HOOD", "RKLB", "IONQ", "SOFI", "AFRM",
              "SHOP", "CRWD", "APP", "RDDT", "ARM", "AVGO", "META", "NFLX", "UBER", "LLY", "NVO", "MRNA", "ENPH",
              "FSLR", "OKLO", "ASTS", "SOUN", "ARKK", "SOXL", "TQQQ", "IBIT", "XBI", "GDX", "SMH", "KWEB", "URA", "TAN"]
START, END = datetime(2024, 11, 1, tzinfo=timezone.utc), datetime(2026, 11, 26, tzinfo=timezone.utc)


def measure(ticker):
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(ticker)}?interval=1d"
           f"&period1={int(START.timestamp())}&period2={int(END.timestamp())}")
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 LaSalleCourse"})
    result = json.loads(urllib.request.urlopen(request, timeout=30).read())["chart"]["result"][0]
    prices = [p for p in result["indicators"]["adjclose"][0]["adjclose"] if p]
    returns = [prices[i] / prices[i - 1] - 1 for i in range(1, len(prices))]

    def hit_rate(n):
        windows = len(prices) - n
        return round(sum(prices[i + n] / prices[i] - 1 >= 0.10 for i in range(windows)) / windows, 3)

    meta = result["meta"]
    return {"ticker": ticker, "sessions": len(prices), "currency": meta.get("currency"), "type": meta.get("instrumentType"),
            "exchange_tz": meta.get("exchangeTimezoneName"), "annual_volatility": round(pstdev(returns[-250:]) * 252 ** 0.5, 2),
            "hit_5": hit_rate(5), "hit_20": hit_rate(20), "hit_63": hit_rate(63)}


if __name__ == "__main__":
    rows = []
    for ticker in CANDIDATES:
        try:
            rows.append(measure(ticker))
        except (OSError, KeyError, ValueError, TypeError) as error:
            rows.append({"ticker": ticker, "error": str(error)[:80]})
        time.sleep(0.4)
    rows.sort(key=lambda r: -(r.get("hit_5", 0) + r.get("hit_20", 0) + r.get("hit_63", 0)))
    save_json(ROOT / "reports/universo_exploracion.json", {"generated_at": utc_now(), "period": [START.date().isoformat(), "2026-11-25"],
                                                           "source": "Yahoo Finance chart (adjclose)", "candidates": rows})
    for row in rows:
        print(row)
