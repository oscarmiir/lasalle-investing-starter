# Universo de instrumentos

`config.toml` contiene 15 instrumentos: 13 acciones y 2 ETF, todos cotizados en EE. UU. y en USD.

| Grupo | Instrumentos |
|---|---|
| Semiconductores | MU, AMD, SOXL (ETF apalancado x3 del sector) |
| Software e IA | PLTR, APP, CRWD |
| Computación cuántica y energía nuclear | IONQ, OKLO |
| Espacio | RKLB, ASTS |
| Fintech y cripto | HOOD, AFRM, COIN |
| Biotecnología | MRNA |
| Oro | GDX (ETF de mineras de oro, poco correlacionado con la tecnología) |

## Criterio

Un punto exige una subida de al menos un +10 % al vencimiento. Con valores de baja volatilidad eso casi
nunca ocurre en una semana, así que la elección del universo pesa tanto como la regla de selección (ver
`docs/estrategia.md`).

1. **Frecuencia histórica de subidas de +10 %.** `scripts/explorar_universo.py` mide, para 40 candidatos,
   en qué porcentaje de ventanas de 5, 20 y 63 sesiones se alcanzó el umbral (2024-11 a 2026-11; resultado
   en [`reports/universo_exploracion.json`](../reports/universo_exploracion.json)). Por ejemplo, MU: 22 %,
   44 % y 69 %; frente a NFLX: 3 %, 16 % y 24 %.
2. **Diversificación.** Se reparten sectores para que un mal día de un sector no haga fallar a la vez las tres
   selecciones. Solo hay un ETF apalancado (SOXL): varios de ellos estarían muy correlacionados y pierden valor
   en mercados laterales.
3. **Resolución en la plataforma.** La plataforma resuelve con el `adjclose` de Yahoo Finance: todos los
   símbolos existen en Yahoo con 518 sesiones, tienen 10 caracteres o menos y cumplen la expresión regular
   del contrato (`^[A-Z][A-Z0-9.\-]*$`).
4. **Verificación con una segunda fuente.** Los 15 están en Nasdaq.com, de modo que el ETL puede confirmar
   cada cierre (ver `docs/fuentes.md`).

## Limitaciones

- **Sesgo de selección:** el periodo analizado ha sido alcista para muchos de estos valores y se ha mirado al
  elegirlos. Las frecuencias pasadas no garantizan las futuras.
- **Solo EE. UU.:** el ETL ya trata otras bolsas (fecha en la zona horaria de cada bolsa, moneda del
  proveedor), pero la validación cruzada con Nasdaq.com solo cubre EE. UU. Añadir instrumentos europeos o
  fondos es posible; para ellos la segunda fuente quedaría como «sin cobertura» y se anotaría sin detener el
  ETL.
- **Riesgo alto:** son instrumentos muy volátiles. Encaja con el criterio del campeonato, no con una cartera
  de inversión real.
