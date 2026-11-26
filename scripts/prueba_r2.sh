#!/usr/bin/env bash
# Lake en Cloudflare R2: escritura, lectura desde el ETL y recuperación. Requiere Docker y .env con credenciales.
# Ejecutar desde la raíz:  bash scripts/prueba_r2.sh     Evidencias: reports/cloud/resumen.txt
set -u -o pipefail
export MSYS_NO_PATHCONV=1 PYTHONUTF8=1 PYTHONIOENCODING=utf-8
cd "$(dirname "$0")/.."
[ -f .env ] || { echo "Falta .env (ver .env.example)"; exit 1; }
BUCKET=$(grep -E '^R2_BUCKET=' .env | cut -d= -f2)
OUT=reports/cloud
mkdir -p "$OUT"
LOG="$OUT/resumen.txt"
: > "$LOG"
FAILURES=0
say() { echo "$*" | tee -a "$LOG"; }
expect() { if [ "$1" = "$2" ]; then say "   OK  $3"; else say "   FALLO  $3 (esperado $2, obtenido $1)"; FAILURES=$((FAILURES + 1)); fi; }
sha() { sha256sum "$1" | cut -d' ' -f1; }
cloud() { uv run --group cloud --env-file .env python lake_cloud.py "$@"; }

say "== Lake en R2 (bucket $BUCKET) $(date -u +%Y-%m-%dT%H:%M:%SZ)"
say "-- 1. Escritura de raw (no sobrescribe; compara huellas)"
cloud subir | tee -a "$LOG"; expect "$?" 0 "subida de raw"

say "-- 2. ETL leyendo raw desde R2 y escribiendo clean/curated/quarantine en R2"
docker compose run --rm etl-cloud --raw-root "s3a://$BUCKET/raw/yahoo" --check-root "s3a://$BUCKET/raw/nasdaq" \
  --lake-root "s3a://$BUCKET/lake" --out-root /workspace/$OUT > "$OUT/etl_cloud.log" 2>&1
expect "$?" 0 "ETL contra R2"
say "   $(grep -E '^\{"status"' "$OUT/etl_cloud.log" | tail -n 1)"
expect "$(sha $OUT/lake/curated/features.csv)" "$(sha lake/curated/features.csv)" "features.csv desde R2 idéntico al local"
python -c "import json;d=json.load(open('$OUT/reports/quality.json',encoding='utf-8'));print('   relectura Parquet en R2:',d['parquet_reread'])" | tee -a "$LOG"
cloud listar --prefijo lake/ | tail -n 1 | sed 's/^/   /' | tee -a "$LOG"

say "-- 3. Recuperación: descarga de raw desde R2, verificación de huellas y ETL local con lo recuperado"
rm -rf "$OUT/recuperado"
cloud recuperar --destino "$OUT/recuperado" | tee -a "$LOG"; expect "$?" 0 "recuperación con huellas verificadas"
docker compose run --rm etl --raw-root "/workspace/$OUT/recuperado/raw/yahoo" --check-root "/workspace/$OUT/recuperado/raw/nasdaq" \
  --lake-root /tmp/lake_recuperado --out-root "/workspace/$OUT/recuperado/salida" > "$OUT/etl_recuperado.log" 2>&1
expect "$?" 0 "ETL local con el raw recuperado"
expect "$(sha $OUT/recuperado/salida/lake/curated/features.csv)" "$(sha lake/curated/features.csv)" "features.csv reconstruido idéntico"

say "== Resultado: $FAILURES fallos"
[ "$FAILURES" -eq 0 ]
