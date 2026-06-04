# Tarea 2: sistemas distribuidos
**Autores**: Enzo Rodriguez y Alonso Iturra
Este proyecto es un sistema con procesamiento de consultas y Fallback con Apache Kafka en Internet, el cual evalúa el rendimiento de diferentes métricas en distintos escenarios posbiles dentro de la realidad.
Antes de empezar, es necesario tener:
-  Git
-  Docker
-  Bash
-  dataset (buildgins.csv)
## Paso 1: Clonar el repositorio
En cualquier carpeta correr:
```bash
git clone https://github.com/HonryQuinn/Tarea2_SistemasDistribuidos.git
cd Tarea2_SistemasDistribuidos/
``` 
## Paso 2: Añadir dataset
Una vez descargado el repositorio, se debe colocar el dataset como en el árbol, debería ver la siguiente estructura:
 ```
.
├── consumidores
│   ├── dlq.py
│   ├── Dockerfile
│   ├── principal.py
│   └── reintentos.py
├── dataset
│   └── buildings.csv <-- Aquí debe ir el dataset
├── docker-compose.yml
├── metricas
│   ├── Dockerfile
│   └── metricas.py
├── README.md
├── respuestas
│   ├── Dockerfile
│   └── engine.py
├── resultados
├── run.sh
├── test_e5.sh --> Pruebas, ignorar
└── trafico
    ├── Dockerfile
    └── main.py

```
## Paso 3: Correr simulación
Para iniciar el proceso de pruebas automáticas, ejecuta el script principal con privilegios de administrador:
```bash
sudo bash run.sh
```
Simplemente dejé correr el script, automaticamente generará un txt con los resultados obtenidos para cada configuración y distribución

---

### Componentes

| Archivo | Rol |
|---|---|
| `trafico/main.py` | Generador de tráfico. Envía consultas en modo síncrono o Kafka según `MODO_TRANSPORTE`. |
| `respuestas/engine.py` | Motor de respuestas. Procesa consultas desde `cola:consultas`, calcula resultados y los guarda en caché. |
| `consumidores/principal.py` | Consumer Kafka principal. Lee `consultas-principales`, consulta Redis y delega al engine en caso de miss. |
| `consumidores/reintentos.py` | Consumer de reintentos. Lee `consultas-reintento` e intenta resolver consultas que fallaron. |
| `consumidores/dlq.py` | Monitor de Dead Letter Queue. Registra en `resultados/dlq.jsonl` las consultas irrecuperables. |
| `metricas/metricas.py` | Recolector de métricas. Lee contadores de Redis y genera reportes `.txt` en `resultados/`. |
| `run.sh` | Orquestador principal. Ejecuta los 7 escenarios de forma secuencial. |

---

## Resultados esperados

Al finalizar cada escenario se genera un archivo `.txt` con la siguiente tabla de métricas:

| Métrica | Descripción |
|---|---|
| **Hit Rate** | % de consultas resueltas desde caché |
| **Latencia p50 / p95** | Percentiles de latencia end-to-end en ms |
| **Throughput** | Consultas procesadas por segundo (qps) |
| **Eviction Rate** | Claves eviccionadas de Redis por minuto |
| **Retry Rate** | % de consultas que requirieron reintento |
| **Recovery Rate** | % de reintentos que lograron resolverse |
| **DLQ Rate** | % de consultas irrecuperables |
| **Backlog peak** | Máximo lag acumulado en Kafka |
| **Recovery Time** | Segundos desde que el engine vuelve hasta backlog = 0 |

---
## Escenarios del experimento

### Escenario 1 — Sistema Base Síncrono

**Propósito:** Establecer una línea base sin Kafka, replicando la arquitectura de la Tarea 1.

**Infraestructura:** solo `cache` + `generador_respuestas`

**Modos:** uniforme y zipf

**Flujo:** `main.py` consulta Redis directamente. En caso de miss, encola en `cola:consultas` y espera la respuesta del engine (máx. 5s). No hay Kafka.

**Resultados:** `caso1_sincrono_uniforme.txt`, `caso1_sincrono_zipf.txt`

---

### Escenario 2 — Kafka con 1 Consumer

**Propósito:** Medir el rendimiento base de la arquitectura asíncrona con Kafka y compararlo con el modo síncrono.

**Infraestructura:** stack completo (Kafka, Zookeeper, consumer ×1, consumer_retry, dlq, kafka-ui)

**Modos:** uniforme y zipf

**Flujo:** `main.py` publica en `consultas-principales`. El consumer lee, consulta Redis y delega al engine en misses. Se espera que el backlog drene antes de capturar métricas.

**Resultados:** `caso2_kafka_1consumer_uniforme.txt`, `caso2_kafka_1consumer_zipf.txt`

---

### Escenario 3 — Kafka con Múltiples Consumers

**Propósito:** Evaluar el impacto del escalado horizontal de consumers en throughput, latencia y backlog.

**Infraestructura:** stack completo, consumers escalados con `--scale`

**Consumers probados:** 3, 5 y 8

**Modo:** uniforme

**Flujo:** Igual que Escenario 2, pero se itera sobre cada cantidad de consumers. Cada iteración levanta y baja la infraestructura desde cero para garantizar condiciones limpias.

**Resultados:** `caso3_kafka_3consumers.txt`, `caso3_kafka_5consumers.txt`, `caso3_kafka_8consumers.txt`

---

### Escenario 4 — Falla Temporal del Engine

**Propósito:** Observar cómo Kafka absorbe el backlog durante una caída del engine y medir el tiempo de recuperación al volver.

**Infraestructura:** stack completo con 1 consumer

**Tiempos de caída probados:** 10s, 30s y 50s

**Flujo:**
1. Se inicia el tráfico en background.
2. Se graba el lag durante 15s en fase normal.
3. Se detiene el engine (`docker stop servicio_respuestas`).
4. Se graba el crecimiento del backlog durante el tiempo de falla configurado.
5. Se reinicia el engine y se mide cuánto tarda en vaciar el backlog acumulado.

**Resultados:** `caso4_falla_10s.txt`, `caso4_falla_30s.txt`, `caso4_falla_50s.txt`

**Archivos adicionales:** `lag_historico_Xs.csv` con columnas `t, lag, fase` (normal / caida / recuperacion)

---

### Escenario 5 — Reintentos con FALLA_RATE

**Propósito:** Evaluar la efectividad del mecanismo de reintentos y la DLQ cuando el engine descarta consultas aleatoriamente.

**Infraestructura:** stack completo, consumers escalados

**FALLA_RATE probados:** 0.2 (20%) y 0.5 (50%)

**Consumers probados:** 1, 5 y 8

**Nota:** Se usa `CONF_DECIMALES=4` para generar más claves únicas (~90.000 valores posibles de confidence en lugar de ~900), lo que fuerza una tasa de miss alta y mayor actividad en el pipeline de reintentos.

**Flujo:** El engine descarta consultas con probabilidad `FALLA_RATE`. Las fallidas van a `consultas-reintento`; `reintentos.py` intenta resolverlas. Tras `MAX_REINTENTOS` (default 3), la consulta pasa a la DLQ.

**Resultados:** 6 archivos — `caso5_falla{0.2|0.5}_{1|5|8}consumers.txt`

---

### Escenario 6 — Spike de Tráfico

**Propósito:** Medir la resiliencia del sistema ante un aumento repentino y breve de carga.

**Infraestructura:** stack completo, consumers escalados

**Consumers probados:** 1, 3, 5 y 8

**Configuración del spike:**
- Se activa en el pedido 1250 (mitad de los 2500 totales).
- Dura 800 pedidos con delay de 1ms (frente a los 5ms normales, es decir, 5× más rápido).

**Flujo:** El tráfico corre en background mientras se graba el lag en tiempo real. Se espera que el backlog drene por completo antes de capturar métricas.

**Resultados:** `caso6_spike_{1|3|5|8}consumers.txt`

**Archivos adicionales:** `lag_historico_spike_Xconsumers.csv` con columnas `t, lag`

---

### Escenario 7 — Recuperación ante Fallos: Síncrono vs Kafka

**Propósito:** Comparar directamente cómo reacciona cada arquitectura ante una caída del engine.

**Sub-escenario 7A — Síncrono:**
- Infraestructura mínima (cache + engine).
- Se cae el engine durante 20s mientras el tráfico sigue fluyendo.
- Las consultas en vuelo que no obtienen respuesta se **pierden** sin posibilidad de recuperación.
- Resultado: `caso7a_recuperacion_sincrono.txt`

**Sub-escenario 7B — Kafka:**
- Stack completo con consumers escalados (1, 5 y 8).
- Se cae el engine durante 20s; Kafka **retiene** los mensajes no procesados en el backlog.
- Al recuperarse el engine se mide el recovery time hasta backlog = 0.
- Resultado: `caso7b_kafka_{1|5|8}consumers.txt`

---

## Notas de implementación

- Cada escenario termina con `docker compose down -v` para garantizar un entorno limpio en el siguiente.
- Entre modos dentro de un mismo escenario se ejecuta `FLUSHALL` en Redis para evitar contaminación de métricas.
- El lag de Kafka se consulta vía `kafka-consumer-groups` cada 3 segundos; el peak se guarda en Redis con un script Lua atómico para evitar condiciones de carrera.
- Kafka UI disponible en `http://localhost:8080` mientras la infraestructura esté activa.
