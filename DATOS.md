# Procedencia de los datos

Todos los originales están en `lake/raw/<fuente>/<descarga>/`, cada carpeta con un `manifest.json` (URL, hora
de descarga, rango pedido y SHA-256 de cada fichero). Las respuestas se conservan sin alterar.

| Descarga | Fuente | Contenido | Uso |
|---|---|---|---|
| `yahoo/20261023T182525773928Z` | Yahoo Finance, endpoint `v8/finance/chart` | Snapshot inicial de la base: AAPL, MSFT, NVDA, META, GOOGL, AMZN, TSLA, JPM, V y NFLX; 2026-04-01 a 2026-10-22; 1.420 cierres | Se conserva y se procesa; no forma parte del universo actual |
| `yahoo/20261126T162237261455Z` | Yahoo Finance, endpoint `v8/finance/chart` | Los 15 instrumentos de `config.toml`; 2024-11-01 a 2026-11-25; 7.770 sesiones | Fuente principal: `adjclose` para las señales y `close` para validar |
| `nasdaq/20261126T165940730737Z` | Nasdaq.com, `api/quote/<T>/historical` | Los mismos 15 instrumentos y el mismo periodo; cierres sin ajustar | Validación cruzada de cierres en el ETL |

- Frecuencia diaria (sesiones). Del JSON de Yahoo se usan `timestamp`, `indicators.quote.close`,
  `indicators.adjclose.adjclose`, `indicators.quote.volume` y los metadatos de bolsa y moneda; la conversión
  campo a campo está en `docs/lake.md`.
- La fecha de descarga no convierte un valor histórico en una cotización actual: el corte de los datos es el
  2026-11-25 (`reports/quality.json`, `snapshot_date`).
- Para actualizar, ver «Actualizar los datos para un ciclo nuevo» en el README. Cada descarga crea una carpeta
  nueva; las anteriores no se borran.

Los datos de terceros mantienen sus condiciones de origen (ver `docs/fuentes.md`). La licencia del código no
concede derechos adicionales sobre ellos. Las descargas no usan claves, cookies ni datos de cuenta.
