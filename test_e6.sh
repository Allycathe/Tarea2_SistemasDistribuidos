#!/bin/bash

N_PEDIDOS=2500
DELAY_MS=5
CONSUMERS_E6=(1 3 5 8)

paso()  { echo -e "\033[0;34m▶\033[0m \033[1m$1\033[0m"; }
ok()    { echo -e "\033[0;32m✔\033[0m $1"; }
warn()  { echo -e "\033[1;33m⚠\033[0m $1"; }

obtener_lag(){
    docker exec kafka kafka-consumer-groups \
        --bootstrap-server localhost:9092 \
        --group grupo-consumidores \
        --describe 2>/dev/null \
        | awk 'NR>1 && $6 ~ /^[0-9]+$/ {sum+=$6} END {print sum+0}'
}

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

mkdir -p resultados
export FALLA_RATE=0.0
export CONF_DECIMALES=2

docker compose build

for n_consumers in "${CONSUMERS_E6[@]}"; do
    paso "Levantando infraestructura | spike | $n_consumers consumers..."
    docker compose up -d cache zookeeper kafka kafka-setup generador_respuestas consumer_retry consumer_dlq kafka-ui
    docker compose up -d --scale consumer=$n_consumers consumer
    sleep 30

    paso "Corriendo tráfico con spike | $n_consumers consumers..."
    docker compose run --rm -d \
        -e SIMULATION_MODE=uniforme \
        -e N_PEDIDOS=$N_PEDIDOS \
        -e DELAY_MS=$DELAY_MS \
        -e SPIKE_ENABLED=true \
        -e SPIKE_EN_PEDIDO=$(( N_PEDIDOS / 2 )) \
        -e SPIKE_DURACION=800 \
        -e SPIKE_DELAY_MS=1 \
        generador_trafico

    # Grabar historial de lag en paralelo
    csv_file="resultados/lag_historico_spike_${n_consumers}consumers.csv"
    echo "t,lag" > "$csv_file"
    t0=$(date +%s)

    esperar_backlog 180 uniforme &
    WAIT_PID=$!

    while kill -0 $WAIT_PID 2>/dev/null; do
        lag=$(obtener_lag)
        t=$(( $(date +%s) - t0 ))
        echo "$t,$lag" >> "$csv_file"
        sleep 3
    done

    wait $WAIT_PID
    ok "Historial guardado en $csv_file"

    guardar_metricas "caso6_spike_${n_consumers}consumers" "uniforme"
    limpiar_redis

    docker compose down -v
    sleep 5
done
ok "Escenario 6 completado"