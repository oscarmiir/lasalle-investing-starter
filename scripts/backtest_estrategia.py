"""Backtest semanal de reglas de selección con el criterio del campeonato (acierto si retorno >= +10 %).

    uv run python scripts/backtest_estrategia.py

Simula cada ciclo dominical del histórico de lake/clean/prices.csv (adjclose, el precio con el que se
resuelve): señales con los datos conocidos hasta el último cierre antes del domingo, Top 3 por horizonte y
resultado con el último cierre antes del domingo de vencimiento (+7, +28 y +91 días), como la plataforma.
Las señales se calculan igual que en el ETL (return_n sobre adjclose y volatilidad de 20 sesiones).
Las diferencias frente a la regla común se acompañan de un intervalo del 90 % por bootstrap de bloques
(bloques de 1, 4 y 13 semanas), porque los ciclos solapados no son independientes.
Resultado: reports/backtest_estrategia.json

Advertencia: el universo se eligió mirando este mismo periodo (sesgo de selección); las tasas absolutas
están infladas. La comparación entre reglas sobre los mismos datos sí es informativa.
"""
import csv
import json
import math
import random
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from statistics import NormalDist, pstdev

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from common import HORIZONS, save_json, settings, utc_now  # noqa: E402
import strategy  # noqa: E402

DAYS = {"1W": 7, "1M": 28, "3M": 91}
TARGET = 0.10


def load_prices():
    _, universe = settings()
    series = defaultdict(list)
    with (ROOT / "lake/clean/prices.csv").open(encoding="utf-8") as stream:
        for row in csv.DictReader(stream):
            if row["ticker"] in universe:
                series[row["ticker"]].append((date.fromisoformat(row["date"]), float(row["adjclose"])))
    return {ticker: sorted(values) for ticker, values in series.items()}


def index_on_or_before(values, day):
    low, high = 0, len(values) - 1
    if values[0][0] > day:
        return None
    while low < high:
        middle = (low + high + 1) // 2
        if values[middle][0] <= day:
            low = middle
        else:
            high = middle - 1
    return low


def signals(values, i):
    prices = [price for _, price in values[: i + 1]]
    if len(prices) < 61:
        return None
    returns = [prices[k] / prices[k - 1] - 1 for k in range(len(prices) - 20, len(prices))]
    return {f"return_{n}": prices[-1] / prices[-n - 1] - 1 for n in (5, 20, 60)} | {"volatility_20": pstdev(returns)}


def run(rules):
    series = load_prices()
    last_day = min(values[-1][0] for values in series.values())
    first_day = max(values[60][0] for values in series.values())
    sunday = first_day + timedelta(days=(6 - first_day.weekday()) % 7)
    results = {name: {h: {"hits": 0, "picks": 0, "pending": 0} for h in HORIZONS} for name in rules}
    base = {h: {"hits": 0, "picks": 0} for h in HORIZONS}
    by_year = {name: defaultdict(lambda: {"hits": 0, "picks": 0}) for name in rules}
    per_cycle = {name: {h: [] for h in HORIZONS} for name in rules}  # aciertos (0-3) por ciclo, en orden
    cycles = 0
    while sunday <= last_day + timedelta(days=1):
        rows, entries = {}, {}
        for ticker, values in series.items():
            i = index_on_or_before(values, sunday)
            if i is not None and (sig := signals(values, i)):
                rows[ticker], entries[ticker] = sig, (values, i)
        if len(rows) >= 3:
            cycles += 1
            for horizon in HORIZONS:
                exit_day = sunday + timedelta(days=DAYS[horizon])
                outcome = {}
                for ticker, (values, i) in entries.items():
                    j = index_on_or_before(values, exit_day)
                    resolved = exit_day <= last_day
                    outcome[ticker] = (values[j][1] / values[i][1] - 1 >= TARGET) if resolved else None
                if all(v is None for v in outcome.values()):
                    for name in rules:
                        results[name][horizon]["pending"] += 3
                    continue
                base[horizon]["hits"] += sum(bool(v) for v in outcome.values())
                base[horizon]["picks"] += len(outcome)
                for name, rule in rules.items():
                    ranking = sorted(rows, key=lambda t: (-rule(rows[t], horizon), t))[:3]
                    hits = sum(bool(outcome[t]) for t in ranking)
                    per_cycle[name][horizon].append(hits)
                    results[name][horizon]["hits"] += hits
                    results[name][horizon]["picks"] += 3
                    by_year[name][f"{sunday.year}-{horizon}"]["hits"] += hits
                    by_year[name][f"{sunday.year}-{horizon}"]["picks"] += 3
        sunday += timedelta(days=7)

    def rate(item):
        n = item["picks"]
        p = item["hits"] / n if n else 0.0
        return {**item, "rate": round(p, 3), "se": round(math.sqrt(p * (1 - p) / n), 3) if n else None}

    report = {"generated_at": utc_now(), "cycles": cycles, "period": [first_day.isoformat(), last_day.isoformat()],
              "target": TARGET, "universe": sorted(series),
              "base_rate_random_pick": {h: rate(base[h]) for h in HORIZONS},
              "rules": {name: {h: rate(results[name][h]) for h in HORIZONS} for name in rules},
              "by_year": {name: {k: rate(v) for k, v in sorted(by_year[name].items())} for name in rules},
              "paired_vs_comun_v0": {name: {h: block_bootstrap(per_cycle[name][h], per_cycle["comun_v0"][h], DAYS[h] // 7)
                                            for h in HORIZONS} for name in rules if name != "comun_v0"},
              "note": __doc__.split("Advertencia:")[1].strip()}
    save_json(ROOT / "reports/backtest_estrategia.json", report)
    return report


def block_bootstrap(rule_hits, base_hits, block, resamples=5000, seed=7):
    """Diferencia de tasa de acierto (regla - común) por ciclo con bootstrap de bloques móviles.

    Los bloques tienen la longitud del horizonte en semanas: ciclos consecutivos comparten precios y sus
    resultados no son independientes. Devuelve la diferencia media y el intervalo del 90 %.
    """
    diffs = [(a - b) / 3 for a, b in zip(rule_hits, base_hits)]
    n = len(diffs)
    starts = range(n - block + 1)
    rng = random.Random(seed)
    means = []
    for _ in range(resamples):
        sample = []
        while len(sample) < n:
            start = rng.choice(starts)
            sample.extend(diffs[start:start + block])
        means.append(sum(sample[:n]) / n)
    means.sort()
    return {"diff": round(sum(diffs) / n, 3), "ci90": [round(means[int(0.05 * resamples)], 3), round(means[int(0.95 * resamples)], 3)],
            "cycles": n, "block_weeks": block}


def rule_sharpe(row, horizon):
    """Rendimiento de 60 sesiones por unidad de volatilidad (propuesta de la revisión)."""
    return float(row["return_60"]) / max(float(row["volatility_20"]), 1e-6)


def rule_momentum(row, horizon):
    return float(row[{"1W": "return_5", "1M": "return_20", "3M": "return_60"}[horizon]])


def rule_probability(row, horizon):
    """P(retorno >= +10 %) con un paseo aleatorio: deriva de return_60 y volatilidad de 20 sesiones."""
    sessions = {"1W": 5, "1M": 20, "3M": 63}[horizon]
    drift = math.log1p(float(row["return_60"])) / 60
    sigma = max(float(row["volatility_20"]), 1e-6)
    z = (math.log1p(TARGET) - drift * sessions) / (sigma * math.sqrt(sessions))
    return 1 - NormalDist().cdf(z)


def rule_volatility(row, horizon):
    return float(row["volatility_20"])


if __name__ == "__main__":
    candidates = {"comun_v0": strategy.score, "momentum_sin_penalizar": rule_momentum,
                  "probabilidad_10": rule_probability, "solo_volatilidad": rule_volatility, "ret60_sobre_vol": rule_sharpe}
    report = run(candidates)
    print(f"{report['cycles']} ciclos {report['period']}")
    print("azar   ", {h: (v["rate"], v["picks"]) for h, v in report["base_rate_random_pick"].items()})
    for name, values in report["rules"].items():
        print(f"{name:24}", {h: (v["rate"], v["se"], v["picks"]) for h, v in values.items()})
    for name, values in report["paired_vs_comun_v0"].items():
        print(f"{name:24} vs comun", {h: (v["diff"], v["ci90"]) for h, v in values.items()})
