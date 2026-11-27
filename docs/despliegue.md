# Publicación de la API por HTTPS

**Endpoint público:** <https://lasalle-ra1-api-578339148607.europe-west1.run.app/api/predictions>

## Servicio elegido: Google Cloud Run

| Opción | Motivo para elegirla o descartarla |
|---|---|
| **Google Cloud Run** (elegida) | Ejecuta el `Dockerfile` del repositorio sin cambios, HTTPS incluido, y el plan gratuito cubre el uso del proyecto. Un arranque en frío tarda menos de 1 s, dentro del límite de unos 8 s que da la plataforma a cada consulta. |
| Render (plan gratuito) | El servicio se suspende cuando no recibe tráfico y tarda decenas de segundos en despertar: la consulta del domingo podría fallar. |
| Vercel | Obliga a adaptar FastAPI al modelo *serverless* y a empaquetar el artefacto aparte. |

## Cómo funciona

- La imagen se construye con el `Dockerfile` de la base: Python 3.11, dependencias de `uv.lock`, `app.py`,
  `common.py` y `artifacts/predictions.json`.
- `.gcloudignore` solo deja subir esos ficheros: ni `.env` ni el lake ni los informes salen del ordenador.
- **La API no calcula nada al recibir una consulta**: sirve el artefacto ya validado por `publish.py`. Como va
  dentro de la imagen, sobrevive a reinicios y escalados. Para publicar un ciclo nuevo hay que regenerar el
  artefacto y volver a desplegar.
- Configuración: región `europe-west1`, puerto 8000, acceso público sin autenticación, 512 MiB y un máximo
  de 2 instancias, que acota el coste.

## Comprobaciones

Evidencias en [`docs/evidencias/despliegue/`](evidencias/despliegue/resumen.txt):

- HTTP 200 con `application/json` por HTTPS; `http://` redirige a `https://`.
- La respuesta cumple el contrato (`validate_payload`) y coincide con `artifacts/predictions.json`.
- Dos consultas consecutivas devuelven lo mismo, incluido `generated_at`.
- Después de forzar un reinicio (revisión nueva) la respuesta es idéntica y llega en 0,65 s con arranque en frío.
- Desde otro dispositivo: móvil con datos 5G (otra red), 2026-11-27 17:42; mismo alumno, misma fecha de
  generación y las 9 predicciones ([captura](evidencias/despliegue/movil_5g.png)).

## Repetir el despliegue

```bash
gcloud auth login                      # una vez
uv run python publish.py               # artefacto validado
bash scripts/desplegar.sh lasalle-ra1 europe-west1
```

El script valida el artefacto antes de subirlo y, al terminar, consulta el endpoint público y vuelve a
validar la respuesta. En un proyecto nuevo de Google Cloud puede hacer falta conceder a la cuenta de servicio
de compilación el rol `roles/run.builder` (Cloud Build lo indica en el error).
