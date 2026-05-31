#!/bin/bash

GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
RED='\033[0;31m'
NC='\033[0m'

export REDIS_MAX_MEMORY=200mb
export REDIS_POLICY=allkeys-lru

draw_header() {
    echo -e "${CYAN}╔══════════════════════════════════════════════════════════════╗${NC}"
    printf "${CYAN}║${NC}  ${YELLOW}%-58s${NC}  ${CYAN}║${NC}\n" "$1"
    echo -e "${CYAN}╚══════════════════════════════════════════════════════════════╝${NC}"
}

draw_step() {
    echo -e "\n${BLUE}┌──────────────────────────────────────────────────────────────┐${NC}"
    echo -e "${BLUE}│${NC}  ${PURPLE}$1${NC}"
    echo -e "${BLUE}└──────────────────────────────────────────────────────────────┘${NC}"
}

esperar_servicio() {
    local nombre=$1
    local check_cmd=$2
    echo -e "${YELLOW}  Esperando $nombre...${NC}"
    until eval "$check_cmd" &>/dev/null; do
        sleep 2
    done
    echo -e "${GREEN}  ✔ $nombre listo${NC}"
}

limpiar_metricas_redis() {
    local modo=$1
    echo -e "${YELLOW}  Limpiando métricas Redis para modo=$modo...${NC}"
    docker exec sistema_cache redis-cli del \
        "${modo}:hits" "${modo}:misses" \
        "${modo}:latencies" "${modo}:timestamps" \
        "${modo}:retry_count" "${modo}:dlq_count" \
        "${modo}:recovered_count" > /dev/null
}

generar_reporte() {
    local modo=$1
    local escenario=$2
    echo -e "${GREEN}  Esperando que el consumer vacíe la cola...${NC}"
    sleep 15
    echo -e "${GREEN}  Generando reporte: $escenario ($modo)...${NC}"
    docker compose run --rm \
        -e MODO_METRICAS="$modo" \
        -e ESCENARIO="$escenario" \
        metricas
}

levantar_infra() {
    echo -e "${YELLOW}  Levantando infraestructura base (Zookeeper, Kafka, Redis, Engine)...${NC}"
    docker compose up -d zookeeper kafka cache
    esperar_servicio "Zookeeper" "docker exec zookeeper nc -z localhost 2181"
    esperar_servicio "Kafka"     "docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list"

    docker compose up -d kafka-setup
    echo -e "${YELLOW}  Esperando creación de tópicos...${NC}"
    sleep 15

    docker compose up -d generador_respuestas
    esperar_servicio "Engine (dataset cargado)" \
        "docker exec sistema_cache redis-cli get status:engine_ready | grep -q 1"
}

bajar_todo() {
    echo -e "${RED}  Bajando todos los servicios...${NC}"
    docker compose down -v
}

escenario_base_sincrono() {
    draw_step "ESCENARIO 1: Sistema base síncrono (referencia Tarea 1)"
    docker compose up -d cache generador_respuestas
    esperar_servicio "Engine" \
        "docker exec sistema_cache redis-cli get status:engine_ready | grep -q 1"

    limpiar_metricas_redis "uniforme"
    docker compose run --rm -e SIMULATION_MODE=uniforme generador_trafico
    generar_reporte "uniforme" "base-sincrono"

    docker compose stop generador_respuestas
}

escenario_kafka_1_consumer() {
    draw_step "ESCENARIO 2: Kafka + 1 Consumer"
    docker compose up -d --scale consumer=1 consumer

    for modo in uniforme zipf; do
        limpiar_metricas_redis "$modo"
        echo -e "${CYAN}  Simulación: $modo${NC}"
        docker compose run --rm -e SIMULATION_MODE="$modo" generador_trafico
        generar_reporte "$modo" "kafka-1-consumer"
        docker exec sistema_cache redis-cli flushall > /dev/null
        docker compose restart generador_respuestas
        esperar_servicio "Engine" \
            "docker exec sistema_cache redis-cli get status:engine_ready | grep -q 1"
    done

    docker compose stop consumer
}

escenario_kafka_multi_consumer() {
    for n_consumers in 2 4; do
        draw_step "ESCENARIO 3: Kafka + $n_consumers Consumers"
        docker compose up -d --scale consumer="$n_consumers" consumer

        limpiar_metricas_redis "uniforme"
        docker compose run --rm -e SIMULATION_MODE=uniforme generador_trafico
        generar_reporte "uniforme" "kafka-${n_consumers}-consumers"

        docker compose stop consumer
        docker exec sistema_cache redis-cli flushall > /dev/null
        docker compose restart generador_respuestas
        esperar_servicio "Engine" \
            "docker exec sistema_cache redis-cli get status:engine_ready | grep -q 1"
    done
}

escenario_falla_temporal() {
    draw_step "ESCENARIO 4: Falla temporal del Generador de Respuestas"
    docker compose up -d --scale consumer=1 consumer consumer_retry

    limpiar_metricas_redis "uniforme"

    docker compose run --rm -d -e SIMULATION_MODE=uniforme generador_trafico

    echo -e "${RED}  Simulando caída del engine en 10s...${NC}"
    sleep 10
    docker compose stop generador_respuestas
    echo -e "${RED}  Engine CAÍDO — consultas deberían ir a reintento...${NC}"
    sleep 15

    echo -e "${GREEN}  Restaurando engine...${NC}"
    docker compose up -d generador_respuestas
    esperar_servicio "Engine (recuperado)" \
        "docker exec sistema_cache redis-cli get status:engine_ready | grep -q 1"

    generar_reporte "uniforme" "falla-temporal"

    docker compose stop consumer consumer_retry
}

escenario_spike() {
    draw_step "ESCENARIO 5: Spike de tráfico"
    docker compose up -d --scale consumer=2 consumer consumer_retry

    limpiar_metricas_redis "uniforme"

    echo -e "${CYAN}  Lanzando spike (2 generadores simultáneos)...${NC}"
    docker compose run --rm -d -e SIMULATION_MODE=uniforme generador_trafico
    docker compose run --rm -d -e SIMULATION_MODE=uniforme generador_trafico
    generar_reporte "uniforme" "spike-trafico"

    docker compose stop consumer consumer_retry
}

escenario_reintentos() {
    draw_step "ESCENARIO 6: Reintentos — recuperación tras fallo"
    docker compose up -d --scale consumer=1 consumer consumer_retry

    limpiar_metricas_redis "uniforme"

    docker compose run --rm -d -e SIMULATION_MODE=uniforme generador_trafico
    sleep 5
    docker compose pause generador_respuestas
    sleep 8
    docker compose unpause generador_respuestas
    esperar_servicio "Engine (tras pausa)" \
        "docker exec sistema_cache redis-cli get status:engine_ready | grep -q 1"

    generar_reporte "uniforme" "reintentos"

    docker compose stop consumer consumer_retry
}

# ── MAIN ──────────────────────────────────────────────────────────────
clear
draw_header "TAREA 2 — BATERÍA DE EXPERIMENTOS KAFKA"

echo -e "${YELLOW}  Config caché base: ${GREEN}${REDIS_MAX_MEMORY} / ${REDIS_POLICY}${NC}\n"

docker compose build

levantar_infra
escenario_base_sincrono
escenario_kafka_1_consumer
escenario_kafka_multi_consumer
escenario_falla_temporal
escenario_spike
escenario_reintentos

bajar_todo
draw_header "EXPERIMENTOS FINALIZADOS — revisa ./resultados/"