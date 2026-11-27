#!/usr/bin/env bash
# Publica la API en Google Cloud Run con el Dockerfile del repositorio. Requiere gcloud con sesión iniciada
# (gcloud auth login) y un proyecto con facturación activa. Ejecutar desde la raíz después de publish.py.
#   bash scripts/desplegar.sh [proyecto] [región]
# Solo se sube lo que lista .gcloudignore (Dockerfile, dependencias, app.py, common.py y el artefacto).
set -euo pipefail
cd "$(dirname "$0")/.."
PROJECT=${1:-lasalle-ra1}
REGION=${2:-europe-west1}
uv run python -c "import json; from common import validate_payload; validate_payload(json.load(open('artifacts/predictions.json', encoding='utf-8')))"
gcloud run deploy lasalle-ra1-api --source . --project "$PROJECT" --region "$REGION" --port 8000 \
  --allow-unauthenticated --memory 512Mi --max-instances 2 --quiet
URL=$(gcloud run services describe lasalle-ra1-api --project "$PROJECT" --region "$REGION" --format='value(status.url)')
curl -fsS "$URL/api/predictions" | uv run python -c "import json, sys; from common import validate_payload; d = validate_payload(json.load(sys.stdin)); print('OK', d['student'], d['generated_at'])"
echo "Endpoint público: $URL/api/predictions"
