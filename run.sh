#!/bin/bash

GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
BOLD='\033[1m'
NC='\033[0m'

header() {
    echo -e "\n${CYAN}╔══════════════════════════════════════════════════════════════╗${NC}"
    printf "${CYAN}║${NC}  ${BOLD}${YELLOW}%-58s${NC}  ${CYAN}║${NC}\n" "$1"
    echo -e "${CYAN}╚══════════════════════════════════════════════════════════════╝${NC}\n"
}
paso()      { echo -e "${BLUE}▶${NC} ${BOLD}$1${NC}"; }
ok()        { echo -e "${GREEN}✔${NC} $1"; }
warn()      { echo -e "${YELLOW}⚠${NC} $1"; }
separador() { echo -e "${PURPLE}────────────────────────────────────────────────────────────────${NC}"; }

guardar_metricas() {
    local escenario=$1
    local modo=$2
    paso "Guardando métricas: escenario=$escenario | modo=$modo"
    docker compose run --rm \
        -e MODO_METRICAS="$modo" \
        -e ESCENARIO="$escenario" \
        metricas
    ok "Métricas guardadas en resultados/"
}

limpiar_redis() {
    paso "Limpiando Redis para el próximo escenario..."
    docker exec sistema_cache redis-cli flushall > /dev/null
    ok "Redis limpio"
}

# ─────────────────────────────────────────────────────────────────────────────
clear
header "BATERÍA DE EXPERIMENTOS — TAREA 2"
paso "Construyendo imágenes..."
docker compose build
ok "Build completado"
mkdir -p resultados

# ═════════════════════════════════════════════════════════════════════════════
# ESCENARIO 1: SISTEMA BASE SÍNCRONO (sin Kafka)
# Replica la arquitectura de la Tarea 1 para línea base de comparación.
# MODO_TRANSPORTE=sincrono hace que main.py hable directo con Redis/engine,
# sin publicar en Kafka (que no está levantado en este escenario).
# ═════════════════════════════════════════════════════════════════════════════
separador
header "ESCENARIO 1 — Sistema Base Síncrono (sin Kafka)"

paso "Levantando solo cache + engine..."
docker compose up -d cache generador_respuestas
sleep 15

for modo in uniforme zipf; do
    paso "Corriendo simulación SÍNCRONA modo=$modo..."
    docker compose run --rm \
        -e SIMULATION_MODE="$modo" \
        -e N_PEDIDOS=2000 \
        -e DELAY_MS=5 \
        -e MODO_TRANSPORTE=sincrono \
        generador_trafico
    guardar_metricas "base_sincrono_$modo" "$modo"
    limpiar_redis
    docker compose restart generador_respuestas
    sleep 10
done

docker compose down -v
ok "Escenario 1 completado"

# ═════════════════════════════════════════════════════════════════════════════
# ESCENARIO 2: KAFKA + 1 CONSUMER
# ═════════════════════════════════════════════════════════════════════════════
separador
header "ESCENARIO 2 — Kafka + 1 Consumer"

paso "Levantando infraestructura completa con 1 consumer..."
docker compose up -d cache zookeeper kafka kafka-setup generador_respuestas consumer consumer_retry consumer_dlq kafka-ui
sleep 30

for modo in uniforme zipf; do
    paso "Corriendo tráfico Kafka modo=$modo | 1 consumer..."
    docker compose run --rm \
        -e SIMULATION_MODE="$modo" \
        -e N_PEDIDOS=2000 \
        -e DELAY_MS=5 \
        generador_trafico
    sleep 10
    guardar_metricas "kafka_1consumer_$modo" "$modo"
    limpiar_redis
    docker compose restart generador_respuestas
    sleep 10
done

docker compose down -v
ok "Escenario 2 completado"

# ═════════════════════════════════════════════════════════════════════════════
# ESCENARIO 3: KAFKA + MÚLTIPLES CONSUMERS
# ═════════════════════════════════════════════════════════════════════════════
separador
header "ESCENARIO 3 — Kafka + Múltiples Consumers"

for n_consumers in 2 4; do
    paso "Levantando infraestructura con $n_consumers consumers..."
    docker compose up -d cache zookeeper kafka kafka-setup generador_respuestas consumer_retry consumer_dlq kafka-ui
    docker compose up -d --scale consumer=$n_consumers consumer
    sleep 30

    paso "Corriendo tráfico | $n_consumers consumers | modo=uniforme..."
    docker compose run --rm \
        -e SIMULATION_MODE=uniforme \
        -e N_PEDIDOS=3000 \
        -e DELAY_MS=2 \
        generador_trafico
    sleep 10
    guardar_metricas "kafka_${n_consumers}consumers" "uniforme"
    limpiar_redis

    docker compose down -v
    sleep 5
done
ok "Escenario 3 completado"

# ═════════════════════════════════════════════════════════════════════════════
# ESCENARIO 4: FALLA TEMPORAL DEL ENGINE
# ═════════════════════════════════════════════════════════════════════════════
separador
header "ESCENARIO 4 — Falla Temporal del Engine"

paso "Levantando infraestructura completa..."
docker compose up -d cache zookeeper kafka kafka-setup generador_respuestas consumer consumer_retry consumer_dlq kafka-ui
sleep 30

paso "Iniciando tráfico en background..."
docker compose run --rm -d \
    -e SIMULATION_MODE=uniforme \
    -e N_PEDIDOS=3000 \
    -e DELAY_MS=3 \
    generador_trafico

sleep 15
warn "Simulando caída del engine (30 segundos)..."
docker stop servicio_respuestas
echo -e "${RED}  ✗ Engine caído — backlog creciendo en Kafka${NC}"

sleep 30

paso "Recuperando engine..."
docker start servicio_respuestas
ok "Engine recuperado"
sleep 20

guardar_metricas "falla_temporal" "uniforme"
docker compose down -v
ok "Escenario 4 completado"

# ═════════════════════════════════════════════════════════════════════════════
# ESCENARIO 5: REINTENTOS CON FALLA_RATE
# ═════════════════════════════════════════════════════════════════════════════
separador
header "ESCENARIO 5 — Reintentos con FALLA_RATE"

for falla_rate in 0.2 0.5; do
    paso "Levantando infraestructura | FALLA_RATE=$falla_rate..."
    FALLA_RATE=$falla_rate docker compose up -d \
        cache zookeeper kafka kafka-setup generador_respuestas consumer consumer_retry consumer_dlq kafka-ui
    sleep 30

    paso "Corriendo tráfico con FALLA_RATE=$falla_rate..."
    docker compose run --rm \
        -e SIMULATION_MODE=uniforme \
        -e N_PEDIDOS=2000 \
        -e DELAY_MS=5 \
        generador_trafico
    sleep 15
    guardar_metricas "reintentos_falla${falla_rate}" "uniforme"
    limpiar_redis

    docker compose down -v
    sleep 5
done
ok "Escenario 5 completado"

# ═════════════════════════════════════════════════════════════════════════════
# ESCENARIO 6: SPIKE DE TRÁFICO
# ═════════════════════════════════════════════════════════════════════════════
separador
header "ESCENARIO 6 — Spike de Tráfico"

paso "Levantando infraestructura completa..."
docker compose up -d cache zookeeper kafka kafka-setup generador_respuestas consumer consumer_retry consumer_dlq kafka-ui
sleep 30

paso "Corriendo tráfico con spike activado..."
docker compose run --rm \
    -e SIMULATION_MODE=uniforme \
    -e N_PEDIDOS=3000 \
    -e DELAY_MS=5 \
    -e SPIKE_ENABLED=true \
    -e SPIKE_EN_PEDIDO=1000 \
    -e SPIKE_DURACION=500 \
    -e SPIKE_DELAY_MS=1 \
    generador_trafico
sleep 15
guardar_metricas "spike_trafico" "uniforme"

docker compose down -v
ok "Escenario 6 completado"

# ═════════════════════════════════════════════════════════════════════════════
# ESCENARIO 7: RECUPERACIÓN ANTE FALLOS — Síncrono vs Kafka
# ═════════════════════════════════════════════════════════════════════════════
separador
header "ESCENARIO 7 — Recuperación ante Fallos: Síncrono vs Kafka"

# 7A — Síncrono: caída del engine = pérdida directa de consultas
paso "7A: Sistema SÍNCRONO con caída del engine..."
docker compose up -d cache generador_respuestas
sleep 15

docker compose run --rm -d \
    -e SIMULATION_MODE=uniforme \
    -e N_PEDIDOS=2000 \
    -e DELAY_MS=5 \
    -e MODO_TRANSPORTE=sincrono \
    generador_trafico

sleep 10
warn "Cayendo engine en modo SÍNCRONO..."
docker stop servicio_respuestas
sleep 20
docker start servicio_respuestas
sleep 10

guardar_metricas "recuperacion_sincrono" "uniforme"
limpiar_redis
docker compose down -v
sleep 5

# 7B — Kafka: caída del engine = backlog retenido, recuperación automática
paso "7B: Sistema KAFKA con caída del engine..."
docker compose up -d cache zookeeper kafka kafka-setup generador_respuestas consumer consumer_retry consumer_dlq kafka-ui
sleep 30

docker compose run --rm -d \
    -e SIMULATION_MODE=uniforme \
    -e N_PEDIDOS=2000 \
    -e DELAY_MS=5 \
    generador_trafico

sleep 10
warn "Cayendo engine en modo KAFKA..."
docker stop servicio_respuestas
sleep 20
docker start servicio_respuestas
ok "Engine recuperado — Kafka retiene las consultas acumuladas"
sleep 15

guardar_metricas "recuperacion_kafka" "uniforme"
docker compose down -v
ok "Escenario 7 completado"

# ─────────────────────────────────────────────────────────────────────────────
separador
header "TODOS LOS EXPERIMENTOS COMPLETADOS"
echo -e "${GREEN}Los resultados están en: ${BOLD}./resultados/${NC}"
echo -e "${CYAN}Kafka UI: ${BOLD}http://localhost:8080${NC}\n"