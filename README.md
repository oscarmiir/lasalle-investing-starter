# LaSalle Investing · RA1 Market Data Lake — Òscar Mir Sala

Proyecto de RA1 (Sistemas Big Data) construido sobre la base del profesor
[ericbellet/lasalle-investing-starter](https://github.com/ericbellet/lasalle-investing-starter). Se descargan
precios reales, se guardan sin modificar en un Data Lake (local y en Cloudflare R2), se procesan con PySpark con
controles de calidad que pueden detener la publicación, y se publica un Top 3 por horizonte en una API pública.

| | |
|---|---|
| **API pública** | <https://lasalle-ra1-api-578339148607.europe-west1.run.app/api/predictions> |
| **Alumno** (`student`) | Òscar Mir Sala |
| **Universo** | 15 acciones y ETF de EE. UU. ([justificación](docs/universo.md)) |
| **Versión entregada** | etiqueta Git `v1` |
| **Selección** | regla común de la base, sin cambios ([backtest y decisión](docs/estrategia.md)) |

## Recorrido de los datos

```text
Yahoo Finance ─┐                       ┌─ lake/clean/prices (Parquet, year/month)
               ├─> lake/raw/<fuente>/ ─┤  lake/quarantine/rejected (filas rechazadas y motivo)
Nasdaq.com ────┘   + manifest.json     └─ lake/curated/features_daily + features (Parquet)
                   (SHA-256)                       │
                        ETL PySpark (Docker) ──────┘ reports/quality.json (controles y puerta)
                                                   │
          publish.py (solo si el ETL está en ok) ──> artifacts/predictions.json ──> API FastAPI (Cloud Run)
```

1. **Descarga** (`download.py`, `download_nasdaq.py`): cada ejecución crea una carpeta raw nueva con la
   respuesta literal del proveedor y un manifiesto (URL, hora, rango pedido, SHA-256).
2. **ETL** (`etl_spark.py`, PySpark 3.5 en Docker): verifica las huellas, transforma y escribe Parquet
   particionado, relee lo escrito y genera `reports/quality.json`.
3. **Publicación** (`publish.py`): aplica `strategy.py` al corte validado y guarda el Top 3 por horizonte. Se
   niega a publicar si el último ETL falló o si `features.csv` ha cambiado después de validarse.
4. **API** (`app.py`): sirve el artefacto ya cerrado; no descarga ni recalcula al recibir una consulta.

## Puesta en marcha

Requisitos: Git, [uv](https://docs.astral.sh/uv/) y Docker Desktop en marcha.

```bash
git clone https://github.com/oscarmiir/lasalle-investing-starter.git
cd lasalle-investing-starter
uv sync --locked --python 3.11
uv run pytest -q                                  # 9 pruebas
uv run uvicorn app:app --reload                   # http://127.0.0.1:8000/api/predictions y /docs
```

En Windows, si una política del sistema bloquea `uvicorn.exe`: `uv run python -m uvicorn app:app --reload`.

### Repetir todo el proceso con los datos incluidos

```bash
docker compose run --rm etl        # raw -> clean/curated/quarantine + reports/quality.json
uv run python publish.py           # artifacts/predictions.json + reports/selection.json
bash scripts/pruebas_calidad.sh    # fallos controlados, reproceso y recuperación (reports/pruebas/)
```

Los Parquet no se versionan: se reconstruyen desde raw con el primer comando. Con el raw incluido el ETL produce
9.190 precios limpios de 25 empresas (las 15 del universo y las 10 del snapshot inicial, que se conserva), 0
rechazos y un corte de 15 empresas a 2026-11-25. Repetirlo da los mismos `prices.csv` y `features.csv` byte a
byte (el informe cambia solo en su fecha de generación).

Las pruebas de PySpark se ejecutan en Docker con `scripts/pruebas_calidad.sh`; `pytest` cubre la parte Python
(publicador, puerta de calidad, contrato, API y lectura de la configuración).

### Actualizar los datos para un ciclo nuevo

```bash
uv run python download.py --start 2024-11-01 --end <último día hábil>
uv run python download_nasdaq.py --start 2024-11-01 --end <último día hábil>
docker compose run --rm etl
uv run python publish.py --max-age-days 4      # rechaza publicar con un corte antiguo
bash scripts/desplegar.sh                      # nueva imagen en Cloud Run y comprobación del endpoint
```

Las descargas anteriores se conservan; el ETL usa, para cada ticker, la serie completa de su descarga más
reciente. En Git Bash (Windows) conviene `export MSYS_NO_PATHCONV=1` antes de pasar rutas `/workspace` a Docker.

## Configuración

`config.toml` contiene el nombre registrado y el universo. Un ticker sin datos detiene el ETL y el publicador
conserva la última salida correcta: añadir un símbolo exige descargar antes sus precios.

## Data Lake

Detalle de zonas, tablas, diccionario de campos, particionado y recuperación en [docs/lake.md](docs/lake.md).

- **raw** (versionado en Git): `lake/raw/yahoo/<descarga>/<TICKER>.json` y `lake/raw/nasdaq/<descarga>/…`,
  cada carpeta con su `manifest.json`. Nunca se edita: se corrige la transformación. `.gitattributes` impide
  que Git altere sus bytes.
- **clean**: `lake/clean/prices/` (Parquet por `year/month`, ordenado por ticker y fecha) y `prices.csv`.
- **quarantine**: `lake/quarantine/rejected/` (Parquet por `reason`).
- **curated**: `lake/curated/features_daily/` (por `year`), `lake/curated/features/` (por `snapshot_date`) y
  `features.csv`, la entrada del publicador.
- **Particionado medido**: particionar por ticker generó 7.600 ficheros pequeños y fue unas 100 veces más
  lento con una réplica ×200; se eligió `year/month`. Para repetirlo:
  `docker compose run --rm --entrypoint /opt/spark/bin/spark-submit etl --master local[2] /workspace/scripts/benchmark_particiones.py`
- **Árbol real** de las tablas generadas: `reports/lake_arbol.txt` (lo regenera `scripts/pruebas_calidad.sh`).

### Almacenamiento en la nube (ampliación): Cloudflare R2

- Bucket **`lasalle-ra1-lake`**, prefijos **`raw/<fuente>/<descarga>/`** y **`lake/{clean,curated,quarantine}/`**.
- Credenciales en `.env` (ignorado por Git). Variables en [.env.example](.env.example): `R2_ENDPOINT`,
  `R2_BUCKET`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`.

```bash
uv run --group cloud --env-file .env python lake_cloud.py subir                  # raw -> R2 (no sobrescribe)
docker compose run --rm etl-cloud --raw-root s3a://lasalle-ra1-lake/raw/yahoo \
  --check-root s3a://lasalle-ra1-lake/raw/nasdaq --lake-root s3a://lasalle-ra1-lake/lake --out-root /workspace/reports/cloud
uv run --group cloud --env-file .env python lake_cloud.py recuperar --destino <carpeta>   # con verificación SHA-256
bash scripts/prueba_r2.sh                                                        # las tres pruebas, con evidencias
```

Evidencia en `reports/cloud/resumen.txt`: el ETL lee raw desde R2, escribe y relee allí sus tablas, obtiene el
mismo `features.csv` que en local y, tras recuperar raw desde R2, reconstruye las mismas salidas.

## Calidad de datos

Cada control tiene una acción. Resultado de la última ejecución en `reports/quality.json`.

| Acción | Controles |
|---|---|
| **Rechazar la fila** (a `quarantine` con su motivo) | cierre nulo, valor no finito, precio ≤ 0, volumen negativo, símbolo distinto del pedido, fecha inválida, fecha futura, sesión no terminada en la hora de descarga, fuera del rango pedido, fin de semana, metadatos incompletos, duplicado idéntico (se conserva uno) y duplicado con precios distintos (se rechazan todos) |
| **Identificar** (aviso en el informe) | salto diario de más del 50 %, hueco de más de 5 días, diferencias de más del 1 % con la segunda fuente, antigüedad del corte |
| **Detener** (el ETL falla y no se publica) | original ausente, alterado o ilegible; ticker configurado sin datos, con menos de 61 sesiones o desfasado más de 5 días del corte; último cierre distinto entre fuentes; señales nulas; relectura de Parquet distinta de lo escrito |

- **Fechas y sesiones:** la fecha de cada barra se calcula en la zona horaria de su bolsa, no en UTC.
- **Precio:** las señales usan `adjclose`, el mismo precio con el que se resuelve el campeonato. Se conservan la
  moneda y la unidad del proveedor.
- **Fallos controlados** (`scripts/pruebas_calidad.sh`, copias en `tests/fixtures/`, el raw real no se toca):
  filas inválidas (todos los motivos salvo `fecha_invalida`, que es defensivo), original alterado, ticker ausente, fichero ausente y segunda fuente
  discrepante. El publicador se niega a publicar con un informe fallido y el artefacto no cambia. Dos ETL
  seguidos dan salidas idénticas; si se borran todas las tablas Parquet, se reconstruyen idénticas desde raw.

## Fuentes de datos

Comparación de nueve fuentes con pruebas reales en [docs/fuentes.md](docs/fuentes.md). Resumen:

- **Yahoo Finance** (principal): es el precio con el que la plataforma resuelve. Acceso no oficial y sin garantías.
- **Nasdaq.com** (validación cruzada, integrada): sin clave, cubre todo el universo. 7.770 cierres comparados,
  2 fuera de tolerancia y el último cierre confirmado en los 15 tickers.
- **Tiingo**: alternativa oficial si Yahoo deja de funcionar. **SEC EDGAR**: fundamentales oficiales para RA2.
  Stooq ya bloquea las descargas automáticas y Alpha Vantage limita a 25 peticiones al día.

## Selección

Se mantiene la regla común (`strategy.py`). `scripts/backtest_estrategia.py` simula 95 ciclos con el criterio
del campeonato y compara cinco reglas con intervalos por *bootstrap* de bloques: ninguna mejora en 1W ni en 1M, y
la mejora en 3M no es concluyente por el sesgo de selección del universo ([docs/estrategia.md](docs/estrategia.md)).
Los candidatos completos de cada publicación están en `reports/selection.json`, con las huellas de
`strategy.py` y de `features.csv`.

## API

- `GET /api/predictions`: contrato de la plataforma (`student`, `generated_at` con zona horaria y 9 selecciones,
  3 por horizonte, con puesto y ticker únicos). No incluye `model_name`, `model_version`, `probability` ni
  `expected_return`.
- `GET /api/health`: estado, alumno y fecha del artefacto.
- **Publicación:** Google Cloud Run con el `Dockerfile` de la base ([docs/despliegue.md](docs/despliegue.md)).
  El artefacto va dentro de la imagen y se mantiene tras reiniciar (comprobado forzando una revisión nueva). Dos
  consultas seguidas devuelven lo mismo.

## Versión v1 y evidencias para RA2

La etiqueta `v1` fija el código, la configuración, el raw, el corte (2026-11-25), los candidatos
(`reports/selection.json`) y el artefacto publicado. Evidencias:

| Evidencia | Ubicación |
|---|---|
| API base y API con la configuración propia | `docs/evidencias/01_api_base.json`, `02_api_fase1.json` |
| Ejecución de PySpark, controles y relectura de Parquet | `reports/quality.json` |
| Fallos controlados, reproceso y recuperación | `reports/pruebas/resumen.txt` y `reports/pruebas/*/` |
| Lake en R2 | `reports/cloud/resumen.txt` |
| Particionado medido | `reports/benchmark_particiones.json` |
| Fuentes (muestras reales) | `docs/evidencias/fuentes/` |
| Universo y backtest | `reports/universo_exploracion.json`, `reports/backtest_estrategia.json` |
| API pública | `docs/evidencias/despliegue/` |

## Límites conocidos

- Yahoo y Nasdaq.com son accesos no oficiales: pueden cambiar o bloquearse. El raw versionado permite repetir
  el proceso sin ellos.
- El universo solo incluye EE. UU.: la validación cruzada no cubriría bolsas de otros países (quedarían anotadas
  como «sin cobertura»).
- Las frecuencias históricas de subidas del +10 % están infladas por el sesgo de selección del universo.
- La actualización de cada ciclo es manual (cinco comandos); no hay una tarea programada.
- La imagen de Spark usa Python 3.8: `config.toml` se lee sin `tomllib` (solo cadenas y listas).

## Estructura

```text
app.py · common.py · publish.py · strategy.py          API, utilidades, publicador y regla de selección
download.py · download_nasdaq.py · lake_cloud.py       descargas raw y lake en R2
etl_spark.py · prepare.py                              ETL PySpark (prepare.py: referencia Python de la base)
compose.yaml · Dockerfile · .gcloudignore              Spark (etl, etl-cloud), imagen de la API y despliegue
scripts/                                               pruebas, benchmark, backtest, exploración y despliegue
tests/                                                 pytest y fixtures con errores inducidos
docs/                                                  fuentes, lake, universo, estrategia y despliegue
```

## Continuidad

- **RA2:** cargar el warehouse y entrenar modelos frente a `v1` (`curated/features_daily` y SEC EDGAR).
- **RA3:** agentes, conservando el contrato de la API.

La colaboración con la plataforma del profesor es voluntaria y se coordina en su
[repositorio](https://github.com/ericbellet/proyecto_lasalle_2026_2027).
