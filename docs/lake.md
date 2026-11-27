# Data Lake: zonas, tablas, particionado y recuperación

## Ubicación y zonas

El lake vive en `lake/` dentro del proyecto (almacenamiento local, válido para la entrega). El ETL acepta
`--lake-root s3a://...` para escribir las mismas tablas en un almacenamiento de objetos (ver README).

```text
lake/
  raw/yahoo/<descarga>/<TICKER>.json + manifest.json     originales del proveedor principal (Git)
  raw/nasdaq/<descarga>/<TICKER>.json + manifest.json    originales de la segunda fuente (Git)
  clean/prices/year=AAAA/month=M/*.parquet               precios validados (regenerable)
  clean/prices.csv                                       exportación pequeña para inspección y trazabilidad
  quarantine/rejected/reason=<motivo>/*.parquet          filas rechazadas con su motivo (regenerable)
  curated/features_daily/year=AAAA/*.parquet             señales por ticker y sesión, para RA2 (regenerable)
  curated/features/snapshot_date=AAAA-MM-DD/*.parquet    señales del corte usadas por la selección (regenerable)
  curated/features.csv                                   entrada del publicador (la valida su huella)
```

- **raw** no se modifica nunca. Cada descarga crea una carpeta nueva con nombre de fecha y hora UTC; el
  manifiesto guarda URL, hora de descarga, rango pedido y SHA-256 de cada fichero. El ETL recalcula la huella
  y se detiene si no coincide. `.gitattributes` impide que Git cambie sus bytes.
- **clean**, **quarantine** y **curated** se reconstruyen desde raw; los Parquet no se versionan.

## Diccionario de campos (raw → clean)

Origen: respuesta `v8/finance/chart` de Yahoo, `chart.result[0]`.

| Campo en clean | Origen en raw | Conversión |
|---|---|---|
| `ticker` | `manifest.files[].ticker` | Se comprueba que coincide con `meta.symbol` (si no, `simbolo_distinto`) |
| `date` | `timestamp[i]` (segundos UTC de la apertura) | `timestamp_seconds` → hora local de `meta.exchangeTimezoneName` → fecha. Evita asignar el día anterior o el siguiente en bolsas fuera de UTC |
| `close` | `indicators.quote[0].close[i]` | `double`; nulo, no finito o ≤ 0 se rechaza. Se usa para la validación cruzada |
| `adjclose` | `indicators.adjclose[0].adjclose[i]` | `double`; cierre ajustado por *splits* y dividendos. **Base de todas las señales** porque es el precio con el que la plataforma resuelve |
| `volume` | `indicators.quote[0].volume[i]` | `long`; negativo se rechaza |
| `currency` | `meta.currency` | Se conserva la etiqueta del proveedor (no se convierte a euros) |
| `exchange`, `exchange_tz`, `instrument_type` | `meta.exchangeName`, `meta.exchangeTimezoneName`, `meta.instrumentType` | Sin cambios |
| `download_id` | carpeta de la descarga | Linaje: de qué descarga raw procede cada fila |
| `year`, `month` | `date` | Columnas de partición |

Segunda fuente (Nasdaq.com, `data.tradesTable.rows[]`): `date` en formato `MM/dd/yyyy` → fecha; `close`
como texto `"$601.25"` → se quitan `$` y `,` → `double`. Solo se usa para comparar, no entra en clean.

Señales en curated (sobre `adjclose`, por ticker en orden de fecha): `daily_return` = adjclose / anterior − 1;
`return_n` = adjclose / adjclose de hace *n* sesiones − 1 (n = 5, 20, 60); `volatility_20` = desviación típica
poblacional de los últimos 20 `daily_return`; `observations` = sesiones acumuladas. `curated/features` toma la
última sesión de cada ticker; `snapshot_date` es el corte común (la sesión más reciente del universo) y
`last_session` la del ticker. Las ventanas cuentan **sesiones**, no días naturales: un día sin cotización (festivo)
no genera fila, así que `return_5` compara con el quinto cierre anterior que existe. Los huecos de más de 5 días
se anotan como aviso.

## Particionado: comparación medida

`scripts/benchmark_particiones.py` (PySpark en Docker, `local[2]`) escribe `clean/prices` con tres
disposiciones y mide tres consultas típicas (mediana de 5 repeticiones tras un calentamiento). Además de los
datos reales se usa una réplica ×200 con tickers sintéticos y ruido en los precios para que la compresión sea
realista. Resultados completos en [`reports/benchmark_particiones.json`](../reports/benchmark_particiones.json).

| Volumen | Partición | Ficheros | MB | Escritura (s) | 1 ticker (s) | 1 mes (s) | Recuento (s) |
|---|---|---|---|---|---|---|---|
| real (9.190 filas) | sin partición | 4 | 0,18 | 0,71 | 0,26 | 0,19 | 0,10 |
| real | year/month | 65 | 0,28 | 0,73 | 0,22 | 0,14 | 0,15 |
| real | ticker | 38 | 0,27 | 0,43 | 0,13 | 0,15 | 0,12 |
| ×200 (1,84 M filas) | sin partición | 4 | 36,7 | 1,00 | 0,11 | 0,13 | 0,08 |
| ×200 | year/month | 65 | 37,0 | 1,45 | 0,16 | 0,16 | 0,16 |
| ×200 | ticker | 7.600 | 61,4 | 42,6 | 15,0 | 19,6 | 17,6 |

Conclusiones:

- Con nuestro volumen real todas las opciones tardan décimas de segundo: domina el coste fijo de Spark, no
  la lectura. Las diferencias no justifican una decisión por sí solas.
- Al crecer el número de tickers, **particionar por ticker genera miles de ficheros pequeños** (7.600 con
  3.000 tickers) y todo se vuelve unas 100 veces más lento, también la escritura.
- `year/month` mantiene un número de ficheros acotado (un directorio por mes) y un rendimiento cercano al de no
  particionar. Además permite reprocesar o retener periodos completos.

Decisión: `clean/prices` por `year/month`, con las filas ordenadas por `ticker` y `date` dentro de cada
partición (las estadísticas mín./máx. de Parquet permiten saltar bloques al filtrar un ticker);
`curated/features_daily` por `year`; `quarantine/rejected` por `reason` (pocas categorías, útil para auditar).
Límite reconocido: con este volumen, una tabla sin particionar sería igual de buena; el particionado se elige
por escalabilidad y mantenimiento, no por velocidad actual.

## Lake en la nube: Cloudflare R2 (ampliación)

- **Servicio:** Cloudflare R2, almacenamiento de objetos compatible con la API de S3. Plan gratuito con
  10 GB al mes de almacenamiento y sin coste de salida de datos. El proyecto ocupa menos de 3 MB.
- **Bucket:** `lasalle-ra1-lake`. **Prefijos:** `raw/<fuente>/<descarga>/<TICKER>.json` (originales y
  manifiestos) y `lake/clean`, `lake/curated`, `lake/quarantine` (Parquet escritos por el ETL).
- **Credenciales:** token de cuenta de R2 limitado a este bucket (lectura y escritura de objetos). Se guardan
  en `.env`, que Git ignora; en el repositorio solo está `.env.example` con los nombres de las variables
  (`R2_ENDPOINT`, `R2_BUCKET`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`).
- **Escritura de raw:** `lake_cloud.py subir`. No sobrescribe un original: si el objeto ya existe compara la
  huella SHA-256 guardada en sus metadatos y se detiene si es distinta.
- **Lectura desde el ETL:** el servicio `etl-cloud` de `compose.yaml` añade el conector S3A de Hadoop
  (`hadoop-aws` 3.3.4) a la misma imagen de Spark. El ETL lee raw con `--raw-root s3a://...`, verifica las
  huellas, escribe las tablas en `s3a://lasalle-ra1-lake/lake/` y las relee allí.
- **Recuperación:** `lake_cloud.py recuperar --destino <carpeta>` descarga raw y comprueba cada fichero con su
  manifiesto. Con lo recuperado el ETL local reconstruye las mismas salidas.

`scripts/prueba_r2.sh` hace las tres comprobaciones y deja la evidencia en `reports/cloud/resumen.txt`.

## Reproceso y recuperación

`scripts/pruebas_calidad.sh` demuestra con evidencias en `reports/pruebas/resumen.txt`:

- Dos ejecuciones seguidas del ETL con el mismo raw producen exactamente los mismos ficheros (huella SHA-256)
  y el raw no cambia (huella de todos los ficheros y nombres, y `git diff` vacío).
- **Recuperación**: se borran todas las tablas Parquet de clean, curated y quarantine, se ejecuta el ETL y se
  obtienen las mismas salidas, byte a byte.
- Sin acceso al proveedor el proceso se puede repetir igualmente: raw está versionado en Git.
