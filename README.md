# LaSalle Investing · RA1 Market Data Lake — Òscar Mir Sala

Proyecto del RA1 (Sistemas Big Data) desarrollado a partir de la base del profesor
[ericbellet/lasalle-investing-starter](https://github.com/ericbellet/lasalle-investing-starter).

| | |
|---|---|
| **API pública** | <https://lasalle-ra1-api-578339148607.europe-west1.run.app/api/predictions> |
| **Alumno** (`student`) | Òscar Mir Sala |
| **Universo** | 15 acciones y ETF de EE. UU. ([justificación](docs/universo.md)) |
| **Versión entregada** | etiqueta Git `v1` |
| **Selección** | regla común de la base, sin cambios ([backtest y decisión](docs/estrategia.md)) |

## En pocas palabras

1. Se descargan los precios diarios reales de 15 acciones y ETF de EE. UU. desde **Yahoo Finance** y, para
   comprobarlos, desde una segunda fuente (**Nasdaq.com**).
2. Los ficheros descargados se guardan tal cual (zona **raw**), con su huella digital SHA-256, y nunca se
   modifican.
3. Un ETL con **PySpark** comprueba las huellas, limpia los precios, aparta las filas incorrectas con su motivo
   y calcula los indicadores. Si encuentra un problema grave, se detiene y no se publica nada.
4. Con esos indicadores se eligen las 3 mejores acciones para 1 semana, 1 mes y 3 meses.
5. El resultado se publica en una **API pública** que consulta la plataforma del campeonato.

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
3. **Publicación** (`publish.py`): aplica la regla de selección (`strategy.py`) a los datos validados y guarda
   el Top 3 de cada plazo. No publica si el último ETL falló o si `features.csv` ha cambiado después de validarse.
4. **API** (`app.py`): devuelve el resultado ya calculado; no descarga ni recalcula nada al recibir una consulta.

## Puesta en marcha

Requisitos: Git, [uv](https://docs.astral.sh/uv/) y Docker Desktop en marcha (abrir Docker Desktop y esperar a
que indique «Engine running»; no hace falta abrir ningún contenedor: `docker compose run --rm` crea uno temporal
y lo borra al terminar). Los comandos se ejecutan desde la raíz del repositorio.

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

En Windows, los scripts `.sh` se ejecutan desde Git Bash (no desde PowerShell). La prueba de calidad completa
tarda unos 15 minutos.

Las tablas Parquet no se suben a Git: se regeneran desde raw con el primer comando. Con los datos incluidos, el
ETL obtiene 9.190 precios limpios de 25 empresas (las 15 elegidas y las 10 del ejemplo inicial de la base, que se
conserva), ningún rechazo y datos hasta el 2026-11-25. Si se repite, `prices.csv` y `features.csv` salen
idénticos; en el informe solo cambia la fecha de generación.

Las pruebas de PySpark se ejecutan en Docker con `scripts/pruebas_calidad.sh`; `pytest` cubre la parte Python
(publicador, puerta de calidad, contrato, API y lectura de la configuración).

### Actualizar los datos para un ciclo nuevo

La plataforma lee la API cada **domingo** (el ciclo cierra a las 21:59 UTC) y usa lo que esté publicado en ese
momento. Para competir con datos recientes se actualiza **una vez por semana, el sábado o el domingo** antes de
ese cierre. Si una semana no se actualiza, la API sigue sirviendo la selección anterior.

1. **Elegir la fecha de fin:** el último día que abrió la bolsa de EE. UU., normalmente el **viernes**, en
   formato `AAAA-MM-DD`. No se pone la fecha de hoy si es fin de semana o festivo: esos días no hay precios.
   Ejemplo: si se actualiza el sábado 28/11/2026, la fecha de fin es `2026-11-27`.
2. **Ejecutar en Git Bash** desde la raíz del repositorio, con Docker Desktop en marcha. Cada semana solo hay
   que cambiar la fecha de la primera línea:

```bash
FIN=2026-11-27                                                  # último día hábil (cambiar cada semana)
uv run python download.py --start 2024-11-01 --end $FIN         # 1. precios de Yahoo en una carpeta raw nueva
uv run python download_nasdaq.py --start 2024-11-01 --end $FIN  # 2. segunda fuente para validar los cierres
docker compose run --rm etl                                     # 3. ETL PySpark y controles de calidad
uv run python publish.py --max-age-days 4                       # 4. Top 3 por horizonte
bash scripts/desplegar.sh                                       # 5. nueva versión de la API en Cloud Run
```

3. **Comprobar que se ha publicado:** abrir la URL de la API y ver que `generated_at` tiene la fecha de hoy.
4. **Guardar en Git** la carpeta raw nueva y los informes (`git add`, `git commit`, `git push`).

A tener en cuenta:

- `--start 2024-11-01` no cambia: es el inicio de los dos años de historia con los que se calculan los indicadores.
- Si algo falla, no se publica nada y la API mantiene la última selección correcta. El ETL se detiene, por
  ejemplo, si falta un ticker o si las dos fuentes no coinciden, y `publish.py` no publica si el ETL ha fallado
  o si los datos tienen más de 4 días (`--max-age-days 4`).
- Si se descarga un día cuya sesión aún no ha cerrado, ese día se descarta (`sesion_incompleta`).
- Las descargas anteriores no se borran; para cada ticker se usa la descarga más reciente.

## Configuración

`config.toml` define dos cosas: el nombre registrado en la plataforma (`student`) y los 15 tickers del universo
(`tickers`). Si se añade un ticker, hay que descargar sus precios antes de ejecutar el ETL; si no, el ETL se
detiene por falta de datos.

## Data Lake

Detalle de zonas, tablas, diccionario de campos, particionado y recuperación en [docs/lake.md](docs/lake.md).

- **raw** (versionado en Git): `lake/raw/yahoo/<descarga>/<TICKER>.json` y `lake/raw/nasdaq/<descarga>/…`,
  cada carpeta con su `manifest.json`. Nunca se modifica: si hay un error, se corrige el ETL, no el original.
  `.gitattributes` impide que Git altere sus bytes.
- **clean**: `lake/clean/prices/` (Parquet por `year/month`, ordenado por ticker y fecha) y `prices.csv`.
- **quarantine**: `lake/quarantine/rejected/` (Parquet por `reason`).
- **curated**: `lake/curated/features_daily/` (por `year`), `lake/curated/features/` (por `snapshot_date`) y
  `features.csv`, la entrada del publicador.
- **Particionado medido**: con los datos multiplicados por 200, particionar por ticker generó 7.600 ficheros
  pequeños y fue unas 100 veces más lento que particionar por `year/month`, que es el elegido. Para repetirlo:
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

Cada control tiene definida una acción. El resultado de la última ejecución está en `reports/quality.json`.

| Acción | Controles |
|---|---|
| **Rechazar la fila** (a `quarantine` con su motivo) | cierre nulo, valor no finito, precio ≤ 0, volumen negativo, símbolo distinto del pedido, fecha inválida, fecha futura, sesión no terminada en la hora de descarga, fuera del rango pedido, fin de semana, metadatos incompletos, duplicado idéntico (se conserva uno) y duplicado con precios distintos (se rechazan todos) |
| **Identificar** (aviso en el informe) | salto diario de más del 50 %, hueco de más de 5 días, diferencias de más del 1 % con la segunda fuente, antigüedad del corte |
| **Detener** (el ETL falla y no se publica) | original ausente, alterado o ilegible; ticker configurado sin datos, con menos de 61 sesiones o desfasado más de 5 días del corte; último cierre distinto entre fuentes; señales nulas; relectura de Parquet distinta de lo escrito |

- **Fechas y sesiones:** la fecha de cada precio se calcula con la hora de su bolsa, no en UTC, para que una
  sesión de Nueva York no cambie de día.
- **Precio:** las señales usan `adjclose`, el mismo precio con el que se resuelve el campeonato. Se conservan la
  moneda y la unidad del proveedor.
- **Fallos provocados a propósito** (`scripts/pruebas_calidad.sh`, con copias de los datos en `tests/fixtures/`;
  los originales reales no se tocan): filas inválidas (todos los motivos de rechazo excepto `fecha_invalida`, que
  es una regla preventiva), original alterado, ticker ausente, fichero ausente y segunda fuente discrepante. Con
  un informe fallido, el publicador no publica y el resultado anterior se mantiene. Dos ejecuciones seguidas dan
  el mismo resultado y, si se borran todas las tablas Parquet, se reconstruyen idénticas desde raw.

## Fuentes de datos

Comparación de nueve fuentes con pruebas reales en [docs/fuentes.md](docs/fuentes.md). Resumen:

- **Yahoo Finance** (principal): es la fuente con la que la plataforma puntúa. Acceso no oficial y sin garantías.
- **Nasdaq.com** (segunda fuente para validar): sin clave y con todo el universo. De 7.770 cierres comparados,
  solo 2 difieren más del 1 %, y el último cierre coincide en los 15 tickers.
- **Tiingo**: alternativa oficial si Yahoo deja de funcionar. **SEC EDGAR**: fundamentales oficiales para RA2.
  Stooq ya bloquea las descargas automáticas y Alpha Vantage limita a 25 peticiones al día.

## Selección

Se mantiene la regla común de la base (`strategy.py`). Antes de decidirlo, `scripts/backtest_estrategia.py`
simuló 95 ciclos con las reglas del campeonato y comparó cinco reglas: ninguna mejora en 1W ni en 1M, y la mejora
en 3M no es concluyente por el sesgo de selección del universo ([docs/estrategia.md](docs/estrategia.md)).
`reports/selection.json` guarda todos los candidatos de cada publicación con su puntuación.

## API

- `GET /api/predictions`: contrato de la plataforma (`student`, `generated_at` con zona horaria y 9 selecciones,
  3 por horizonte, con puesto y ticker únicos). No incluye `model_name`, `model_version`, `probability` ni
  `expected_return`.
- `GET /api/health`: estado, alumno y fecha del artefacto.
- **Publicación:** Google Cloud Run con el `Dockerfile` de la base ([docs/despliegue.md](docs/despliegue.md)).
  El resultado va dentro de la imagen, así que se mantiene al reiniciar (comprobado forzando una revisión
  nueva). Dos consultas seguidas devuelven lo mismo.

## Versión v1 y evidencias para RA2

La etiqueta `v1` marca la versión entregada: código, configuración, datos raw, fecha de corte (2026-11-25),
candidatos (`reports/selection.json`) y resultado publicado. Evidencias:

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
- El universo se eligió mirando el mismo periodo que se analiza, así que los porcentajes históricos de subidas
  del +10 % son optimistas.
- La actualización de cada ciclo es manual (cinco comandos); no hay una tarea programada.
- La imagen de Spark trae Python 3.8, sin `tomllib`; por eso `config.toml` se lee con un lector propio que solo
  admite textos y listas.

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

- **RA2:** construir el warehouse a partir del lake y entrenar modelos que se compararán con la `v1`
  (`curated/features_daily` y SEC EDGAR).
- **RA3:** agentes, conservando el contrato de la API.

La colaboración con la plataforma del profesor es voluntaria y se coordina en su
[repositorio](https://github.com/ericbellet/proyecto_lasalle_2026_2027).
