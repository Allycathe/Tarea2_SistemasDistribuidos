#!/bin/bash

N_PEDIDOS=2500
DELAY_MS=5
TIEMPOS_FALLA=(10 30 50)

paso()  { echo -e "\033[0;34\033[0m \033[1m$1\033[0m"; }
ok()    { echo -e "\033[0;32\033[0m $1"; }
warn()  { echo -e "\033[1;33\033[0m $1"; }

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

mkdir -p resultados

docker compose build

for tiempo_falla in "${TIEMPOS_FALLA[@]}"; do
    paso "Levantando infraestructura | caída de ${tiempo_falla}s..."
    docker compose up -d cache zookeeper kafka kafka-setup generador_respuestas consumer consumer_retry consumer_dlq kafka-ui
    sleep 30

    paso "Iniciando tráfico en background..."
    docker compose run --rm -d \
        -e SIMULATION_MODE=uniforme \
        -e N_PEDIDOS=$N_PEDIDOS \
        -e DELAY_MS=$DELAY_MS \
        generador_trafico

    csv_file="resultados/lag_historico_${tiempo_falla}s.csv"
    echo "t,lag,fase" > "$csv_file"
    t0=$(date +%s)

    # Fase normal: grabar lag mientras llega tráfico antes de caer
    paso "Grabando lag en fase normal (15s antes de caer)..."
    deadline_pre=$(( $(date +%s) + 15 ))
    while [ $(date +%s) -lt $deadline_pre ]; do
        lag=$(obtener_lag)
        t=$(( $(date +%s) - t0 ))
        echo "$t,$lag,normal" >> "$csv_file"
        echo "  Normal — Backlog: $lag msgs | t=${t}s"
        sleep 3
    done

    warn "Simulando caída del engine (${tiempo_falla} segundos)..."
    docker stop servicio_respuestas

    # Fase caída: grabar lag mientras el engine está caído
    deadline=$(( $(date +%s) + tiempo_falla ))
    while [ $(date +%s) -lt $deadline ]; do
        lag=$(obtener_lag)
        t=$(( $(date +%s) - t0 ))
        echo "$t,$lag,caida" >> "$csv_file"
        if [ -n "$lag" ] && [ "$lag" -gt 0 ] 2>/dev/null; then
            docker exec sistema_cache redis-cli \
                eval "local cur=tonumber(redis.call('get',KEYS[1]) or 0); if tonumber(ARGV[1])>cur then redis.call('set',KEYS[1],ARGV[1]) end" \
                1 "uniforme:backlog_peak" "$lag" > /dev/null 2>&1
        fi
        echo "  Engine caído — Backlog: $lag msgs | t=${t}s"
        sleep 3
    done

    paso "Recuperando engine..."
    docker start servicio_respuestas
    ok "Engine recuperado — grabando historial de recuperación..."

    # Fase recuperación: grabar lag hasta que llegue a 0
    max_recovery=300
    inicio_recovery=$(date +%s)
    while true; do
        lag=$(obtener_lag)
        t=$(( $(date +%s) - t0 ))
        echo "$t,$lag,recuperacion" >> "$csv_file"
        [ "$lag" = "0" ] && break
        transcurrido=$(( $(date +%s) - inicio_recovery ))
        [ $transcurrido -ge $max_recovery ] && break
        echo "  Recovery: $lag msgs | t=${t}s"
        sleep 3
    done

    fin=$(date +%s)
    recovery=$(( fin - inicio_recovery ))
    docker exec sistema_cache redis-cli set "uniforme:recovery_time" "$recovery" > /dev/null
    ok "Recovery time: ${recovery}s — historial en $csv_file"

    guardar_metricas "caso4_falla_${tiempo_falla}s" "uniforme"
    docker compose down -v
    sleep 5
done
ok "Escenario 4 completado"