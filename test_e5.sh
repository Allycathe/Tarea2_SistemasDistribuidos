#!/bin/bash

GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
BOLD='\033[1m'
NC='\033[0m'

paso()  { echo -e "\033[0;34m▶\033[0m \033[1m$1\033[0m"; }
ok()    { echo -e "\033[0;32m✔\033[0m $1"; }
warn()  { echo -e "\033[1;33m⚠\033[0m $1"; }

N_PEDIDOS=2500
DELAY_MS=5
CONSUMERS_E5=(1 5 8)
FALLA_RATES=(0.2 0.5)

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
    docker exec sistema_cache redis-cli flushall > /dev/null
    ok "Redis limpio"
}

obtener_lag(){
    docker exec kafka kafka-consumer-groups \
        --bootstrap-server localhost:9092 \
        --group grupo-consumidores \
        --describe 2>/dev/null \
        | awk 'NR>1 && $6 ~ /^[0-9]+$/ {sum+=$6} END {print sum+0}'
}

esperar_backlog() {
    local max_espera=${1:-180}
    local modo=${2:-uniforme}
    local inicio=$(date +%s)
    paso "Esperando backlog..."
    while true; do
        lag=$(obtener_lag)
        if [ -n "$lag" ] && [ "$lag" -gt 0 ] 2>/dev/null; then
            docker exec sistema_cache redis-cli \
                eval "local cur=tonumber(redis.call('get',KEYS[1]) or 0); if tonumber(ARGV[1])>cur then redis.call('set',KEYS[1],ARGV[1]) end" \
                1 "${modo}:backlog_peak" "$lag" > /dev/null 2>&1
        fi
        [ "$lag" = "0" ] && break
        ahora=$(date +%s)
        transcurrido=$(( ahora - inicio ))
        [ $transcurrido -ge $max_espera ] && break
        echo "  Backlog: $lag msgs... (${transcurrido}s)"
        sleep 3
    done
    ok "Backlog drenado"
}

docker compose build

export CONF_DECIMALES=4

for falla_rate in "${FALLA_RATES[@]}"; do
    for n_consumers in "${CONSUMERS_E5[@]}"; do
        paso "FALLA_RATE=$falla_rate | consumers=$n_consumers"
        export FALLA_RATE=$falla_rate
        docker compose up -d \
            cache zookeeper kafka kafka-setup generador_respuestas consumer_retry consumer_dlq kafka-ui
        docker compose up -d --scale consumer=$n_consumers consumer
        sleep 30

        docker compose run --rm \
            -e SIMULATION_MODE=uniforme \
            -e N_PEDIDOS=$N_PEDIDOS \
            -e DELAY_MS=$DELAY_MS \
            generador_trafico
        esperar_backlog 180 uniforme
        guardar_metricas "caso5_falla${falla_rate}_${n_consumers}consumers" "uniforme"
        limpiar_redis
        docker compose down -v
        sleep 5
    done
done

export CONF_DECIMALES=2
export FALLA_RATE=0.0 # se guardaba en la shell, afectando los otros casos
ok "Escenario 5 completado"
