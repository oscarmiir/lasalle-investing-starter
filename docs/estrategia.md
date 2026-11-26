# Selección: se mantiene la regla común en v1

**Decisión:** la versión `v1` usa sin cambios la regla común de `strategy.py`
(`return_n − 0,25 · volatility_20`, con n = 5, 20 y 60 sesiones para 1W, 1M y 3M). El enunciado lo permite
y así RA2 tendrá una línea base limpia con la que comparar los modelos de ML. La decisión se ha tomado
después de medir alternativas, no por omisión.

## Por qué se planteó cambiarla

El campeonato da un punto si una selección sube al menos un +10 % al vencimiento. Con un umbral binario
la volatilidad aumenta la probabilidad de superarlo, y la regla común la penaliza. Por eso se comparó con
reglas que no penalizan o que favorecen la volatilidad.

## Cómo se ha medido

`scripts/backtest_estrategia.py` reproduce cada ciclo dominical del histórico (`lake/clean/prices.csv`,
`adjclose`):

- las señales se calculan con los datos conocidos hasta el último cierre antes del domingo, igual que el ETL;
- se resuelve con el último cierre antes del domingo + 7, 28 o 91 días, igual que la plataforma;
- 95 ciclos, de enero de 2025 a noviembre de 2026; las selecciones no vencidas quedan pendientes;
- las diferencias frente a la regla común llevan un intervalo del 90 % por *bootstrap* de bloques de 1, 4 y
  13 semanas, porque los ciclos solapados no son independientes.

Resultados completos en [`reports/backtest_estrategia.json`](../reports/backtest_estrategia.json).

| Regla | 1W | 1M | 3M | Diferencia 3M frente a la común (IC 90 %) |
|---|---|---|---|---|
| Elección al azar dentro del universo | 18,0 % | 35,2 % | 53,8 % | — |
| **Común (v1)** | 21,3 % | 35,2 % | 59,3 % | — |
| Momento sin penalizar la volatilidad | 21,3 % | 34,4 % | 59,3 % | 0,0 (0,0 a 0,0) |
| Probabilidad de +10 % (paseo aleatorio) | 21,6 % | 34,8 % | 66,3 % | +6,9 (+3,7 a +11,8) |
| `return_60 / volatility_20` | 20,6 % | 34,4 % | 64,2 % | +4,9 (+0,8 a +9,8) |
| Solo volatilidad | 23,0 % | 37,7 % | 46,7 % | −12,6 (−22,4 a −2,8) |

## Lectura

- En **1W y 1M** ninguna alternativa mejora la regla común: los intervalos incluyen el cero.
- En **3M** las reglas que dividen la tendencia de 60 sesiones por la volatilidad mejoran entre 5 y 7 puntos.
  No es suficiente para cambiar la v1:
  - el universo se eligió mirando este mismo periodo y favorece a los valores con mucha tendencia de 60 días,
    justo lo que premian esas reglas (sesgo de selección);
  - con horizontes de 13 semanas solo hay unos 6 o 7 periodos independientes;
  - elegir una regla distinta por horizonte después de ver los resultados sería ajustar a los datos.
- Favorecer solo la volatilidad empeora mucho a 3M: la intuición inicial no se confirma.
- La mayor parte de la ventaja frente al azar en otros universos procede de **qué instrumentos se eligen**
  (ver `docs/universo.md`), no de cómo se ordenan.

Queda como hipótesis para RA2: evaluar `return_60 / volatility_20` en 3M con datos fuera de muestra y un
universo elegido sin mirar el periodo de prueba.
