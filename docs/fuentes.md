# Investigación de fuentes de datos

Revisión de noviembre de 2026. Para cada fuente se indica qué se ha **comprobado con una petición real**
(muestra en [`docs/evidencias/fuentes/`](evidencias/fuentes/)) y qué procede solo de su documentación.

## Criterios

El campeonato resuelve cada selección con el **`adjclose` de Yahoo Finance** (cierre ajustado del último día
hábil antes del domingo de vencimiento; ver `yahoo-provider.ts` en la plataforma del profesor). Por eso la
fuente principal debe dar exactamente ese precio y las alternativas sirven para **validar** o **ampliar**
(fundamentales para RA2). Se valoran cobertura de nuestro universo (acciones y ETF de EE. UU.), histórico,
ajuste por *splits* y dividendos, autenticación, cuota gratuita, estabilidad y condiciones de uso.

## Comparación

| Fuente | Datos | Ajuste | Autenticación y cuota gratuita | Condiciones | Prueba real |
|---|---|---|---|---|---|
| **Yahoo Finance** `v8/finance/chart` | OHLCV diario, `adjclose`, metadatos de bolsa, moneda y zona horaria | Sí (`adjclose`) | Sin clave; límite no publicado | Acceso web **no oficial**, sin garantías; sus condiciones no permiten redistribuir | ✅ 15/15 tickers, 518 sesiones ([muestra](evidencias/fuentes/yahoo_MU.txt)) |
| **Nasdaq.com** `api/quote/<T>/historical` | OHLCV diario de EE. UU. (acciones con `assetclass=stocks`, ETF con `etf`) | No (cierre sin ajustar) | Sin clave; límite no publicado | Acceso web **no oficial**; sin redistribución | ✅ 15/15 tickers, 518 sesiones ([muestra](evidencias/fuentes/nasdaq_MU.txt)) |
| **Alpha Vantage** | Diario, técnicos, fundamentales | `DAILY_ADJUSTED` según plan | `apikey` gratuita: 25 peticiones/día ([soporte](https://www.alphavantage.co/support/)) | Oficial y documentada | ⚠️ La clave `demo` solo sirve para IBM; según la documentación el diario ajustado puede requerir plan de pago (no verificado) ([IBM](evidencias/fuentes/alphavantage_IBM_demo.txt), [MU rechazado](evidencias/fuentes/alphavantage_MU_demo.txt)); 15 tickers ≈ 15 peticiones/día |
| **Tiingo** | Diario de acciones, ETF y fondos de EE. UU., cierre bruto y ajustado | Sí | Token; plan gratuito con límites por hora, día y símbolos ([precios](https://www.tiingo.com/about/pricing)) | Oficial; uso personal, sin redistribución | ❌ No probada: requiere cuenta. Límites **no verificados** (la página se carga con JavaScript) |
| **SEC EDGAR** `companyfacts` | Fundamentales oficiales XBRL (10-K, 10-Q); **sin precios** | — | Sin clave; `User-Agent` identificativo obligatorio; 10 peticiones/s ([documentación](https://www.sec.gov/search-filings/edgar-application-programming-interfaces)) | Datos públicos oficiales | ✅ Micron (CIK 723125) ([muestra](evidencias/fuentes/sec_edgar_MU_companyfacts.txt)); no cubre ETF |
| **Stooq** | Diario en CSV | Normalmente sin ajustar | Sin registro, pero ahora exige verificación del navegador con JavaScript | Uso personal | ❌ Bloqueada para descargas automáticas ([respuesta](evidencias/fuentes/stooq_MU.txt)) |
| Financial Modeling Prep, Finnhub, EODHD | Diario y fundamentales | Según plan | Clave obligatoria; planes gratuitos con pocas peticiones o histórico corto; las velas diarias de Finnhub son de pago | Oficiales | No probadas (requieren cuenta); se descartan por cuota u histórico frente a Tiingo |

## Decisión

1. **Fuente principal: Yahoo Finance** (la del snapshot inicial). Es el mismo precio con el que se puntúa;
   cualquier otra fuente introduciría diferencias de ajuste entre lo que predecimos y lo que se evalúa.
   Riesgo: acceso no oficial. Mitigación: cada respuesta se conserva en raw con su huella y el ETL se
   puede repetir sin el proveedor.
2. **Segunda fuente para validar: Nasdaq.com**, integrada en el proyecto (`download_nasdaq.py`,
   `lake/raw/nasdaq/`). No necesita cuenta, cubre todo nuestro universo y es independiente de Yahoo.
   El ETL compara los cierres sin ajustar de ambas fuentes (sección siguiente).
3. **Alternativa oficial si Yahoo deja de funcionar: Tiingo** (cierre ajustado y documentación oficial),
   a costa de crear una cuenta y respetar la cuota. Alpha Vantage queda por detrás por su límite diario.
4. **Fundamentales para RA2: SEC EDGAR**, oficial y gratuita, solo para empresas (no ETF).

## Integración y validación cruzada

- `uv run python download_nasdaq.py --start 2024-11-01 --end 2026-11-27` guarda la respuesta literal y su
  `manifest.json` (URL, hora, SHA-256, clase de activo) en `lake/raw/nasdaq/<descarga>/`.
- `etl_spark.py --check-root lake/raw/nasdaq` (valor por defecto) verifica las huellas de esta fuente, une
  por `(ticker, fecha)` el `close` de Yahoo con el `Close/Last` de Nasdaq y aplica:
  - **IDENTIFICAR**: cada sesión con más de un 1 % de diferencia y la cobertura por ticker.
  - **DETENER**: si el **último cierre** (el que entra en la selección) difiere entre las dos fuentes.
  - Si un ticker no existe en la segunda fuente (p. ej., bolsas fuera de EE. UU.) se anota sin detener.
- Resultado con los datos actuales (`reports/quality.json`, `cross_check`): **7.770 cierres comparados,
  2 fuera de tolerancia** (GDX y SOXL, ambos el 2026-04-20, un 1,2-1,3 %), **último cierre confirmado en los
  15 tickers**. La diferencia máxima del resto es inferior al 0,01 %.
