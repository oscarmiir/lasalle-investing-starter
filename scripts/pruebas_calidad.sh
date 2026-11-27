#!/usr/bin/env bash
# Fallos controlados y reproceso. Requiere Docker en marcha. Ejecutar desde la raíz:  bash scripts/pruebas_calidad.sh
# Las pruebas usan copias en tests/fixtures/; el lake real y los originales raw no se tocan.
# Evidencias: reports/pruebas/<caso>/reports/quality.json y reports/pruebas/resumen.txt
# Termina con código 1 si algún resultado no es el esperado.
set -u -o pipefail
export MSYS_NO_PATHCONV=1  # Git Bash en Windows: no convertir /workspace en una ruta de Windows
export PYTHONUTF8=1 PYTHONIOENCODING=utf-8  # mensajes con acentos legibles en Windows
cd "$(dirname "$0")/.."
OUT=reports/pruebas
mkdir -p "$OUT"
LOG="$OUT/resumen.txt"
: > "$LOG"
FAILURES=0
say() { echo "$*" | tee -a "$LOG"; }
expect() { if [ "$1" = "$2" ]; then say "   OK  $3"; else say "   FALLO  $3 (esperado $2, obtenido $1)"; FAILURES=$((FAILURES + 1)); fi; }
sha() { sha256sum "$1" | cut -d' ' -f1; }
raw_fingerprint() { (cd lake/raw && find . -type f | sort | xargs sha256sum) | sha256sum | cut -d' ' -f1; }

say "== Pruebas de calidad $(date -u +%Y-%m-%dT%H:%M:%SZ)"
raw_before=$(raw_fingerprint)
artifact_before=$(sha artifacts/predictions.json)
features_before=$(sha lake/curated/features.csv)

run_case() {  # caso, código esperado, [raw principal], [segunda fuente]
  rm -rf "$OUT/$1"
  docker compose run --rm etl --raw-root "/workspace/${3:-tests/fixtures/$1}" --lake-root "/tmp/lake_$1" \
    --out-root "/workspace/$OUT/$1" --config /workspace/tests/fixtures/config_fixture.toml \
    --check-root "/workspace/${4:-lake/raw/nasdaq}" > "$OUT/$1.log" 2>&1
  local code=$?
  say "-- $1: $(grep -E '^\{"status"' "$OUT/$1.log" | tail -n 1)"
  expect "$code" "$2" "código de salida del ETL"
}
run_case filas_invalidas 0
run_case raw_alterado 1
run_case universo_incompleto 1
run_case fichero_ausente 1
run_case nasdaq_discrepante 1 tests/fixtures/filas_invalidas tests/fixtures/nasdaq_discrepante
expect "$(sha lake/curated/features.csv)" "$features_before" "la entrada real del publicador no cambia con las pruebas"

say "-- Puerta del publicador con el informe fallido de raw_alterado:"
uv run python publish.py --quality "$OUT/raw_alterado/reports/quality.json" > "$OUT/publicador.log" 2>&1
code=$?
say "   $(tail -n 1 "$OUT/publicador.log")"
expect "$code" 1 "publish.py se niega a publicar"
expect "$(sha artifacts/predictions.json)" "$artifact_before" "artifacts/predictions.json sin cambios"

say "-- Reproceso: dos ejecuciones seguidas del ETL real con el mismo raw"
for run in 1 2; do
  docker compose run --rm etl > "$OUT/reproceso_$run.log" 2>&1
  expect "$?" 0 "ejecución $run del ETL"
  say "   ejecución $run: $(grep -E '^\{"status"' "$OUT/reproceso_$run.log" | tail -n 1)"
  eval "features_$run=$(sha lake/curated/features.csv)"
  eval "prices_$run=$(sha lake/clean/prices.csv)"
done
expect "$features_2" "$features_1" "features.csv idéntico en el reproceso"
expect "$prices_2" "$prices_1" "prices.csv idéntico en el reproceso (sin duplicados nuevos)"
expect "$(raw_fingerprint)" "$raw_before" "raw intacto (huella de todos los ficheros y nombres)"
git diff --quiet -- lake/raw
expect "$?" 0 "git: sin cambios en lake/raw"

say "-- Recuperación: se borran clean, curated y quarantine (Parquet) y se reconstruyen desde raw"
rm -rf lake/clean/prices lake/curated/features lake/curated/features_daily lake/quarantine
docker compose run --rm etl > "$OUT/recuperacion.log" 2>&1
expect "$?" 0 "ETL de recuperación"
expect "$(sha lake/curated/features.csv)" "$features_1" "features.csv recuperado idéntico"
expect "$(sha lake/clean/prices.csv)" "$prices_1" "prices.csv recuperado idéntico"
for table in clean/prices curated/features curated/features_daily quarantine/rejected; do
  [ -d "lake/$table" ]; expect "$?" 0 "tabla Parquet lake/$table reconstruida"
done

{ echo "# Árbol real de las tablas Parquet del lake local ($(date -u +%Y-%m-%dT%H:%M:%SZ))"
  find lake/clean lake/curated lake/quarantine -type d | sort
  echo "# Ficheros Parquet por tabla"
  for t in clean/prices curated/features curated/features_daily quarantine/rejected; do
    echo "lake/$t: $(find lake/$t -name '*.parquet' | wc -l) ficheros"; done; } > reports/lake_arbol.txt
say "-- Árbol del lake guardado en reports/lake_arbol.txt"

say "== Resultado: $FAILURES fallos"
[ "$FAILURES" -eq 0 ]
