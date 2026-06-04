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

# Configuración para todos los escenarios

N_PEDIDOS=2500   # Escenario 1 y 2: parámetros base
DELAY_MS=5

CONSUMERS_E3=(3 5 8)  # Escenario 3: cantidades de consumers a probar

TIEMPOS_FALLA=(10 30 50) # Escenario 4: tiempos de caída del engine en segundos

CONSUMERS_E5=(1 5 8) # Escenario 5: consumers para reintentos

CONSUMERS_E6=(1 3 5 8) # Escenario 6: consumers para spike

CONSUMERS_E7=(1 5 8) # Escenario 7: consumers para recuperación

FALLA_RATES=(0.2 0.5) # Falla rate para escenario 5

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

obtener_lag(){
    docker exec kafka kafka-consumer-groups \
        --bootstrap-server localhost:9092 \
        --group grupo-consumidores \
        --describe 2>/dev/null \
        | awk 'NR>1 && $6 ~ /^[0-9]+$/ {sum+=$6} END {print sum+0}'
}

esperar_backlog() {
    #espera que el consumer drene el backlog y registra el peak en Redis
    local max_espera=${1:-180}
    local modo=${2:-uniforme}
    local inicio=$(date +%s)
    paso "Esperando que el consumer desocupe el backlog"
    while true; do
        lag=$(obtener_lag)
        #guardar peak en Redis si es mayor al actual
       if [ -n "$lag" ] && [ "$lag" -gt 0 ] 2>/dev/null; then
            docker exec sistema_cache redis-cli \
                eval "local cur=tonumber(redis.call('get',KEYS[1]) or 0); if tonumber(ARGV[1])>cur then redis.call('set',KEYS[1],ARGV[1]) end" \
                1 "${modo}:backlog_peak" "$lag" > /dev/null 2>&1
        fi
 
        [ "$lag" = "0" ] && break
 
        ahora=$(date +%s)
        transcurrido=$(( ahora - inicio ))
        if [ $transcurrido -ge $max_espera ]; then
            warn "Timeout esperando backlog (${max_espera}s). Lag restante: $lag msgs"
            break
        fi
 
        echo "  Backlog: $lag msgs pendientes... (${transcurrido}s)"
        sleep 3
    done
    ok "Backlog drenado — métricas listas para capturar"
}

medir_recovery_time() {
    # Mide cuántos segundos tarda en vaciarse el backlog desde que el engine vuelve, ádemas llamar justo después de docker start servicio_respuestas.
    local modo=${1:-uniforme}
    local max_espera=${2:-300}
    local inicio=$(date +%s)
    paso "Midiendo recovery time (desde recuperación del engine hasta backlog=0)..."
    while true; do
        lag=$(obtener_lag)
 
        [ "$lag" = "0" ] && break
 
        ahora=$(date +%s)
        transcurrido=$(( ahora - inicio ))
        if [ $transcurrido -ge $max_espera ]; then
            warn "Timeout recovery time (${max_espera}s). Lag restante: $lag msgs"
            # Guardar el timeout como recovery time
            docker exec sistema_cache redis-cli set "${modo}:recovery_time" "$max_espera" > /dev/null
            return
        fi
 
        echo "  Recovery: backlog=$lag msgs | transcurrido=${transcurrido}s"
        sleep 3
    done
 
    local fin=$(date +%s)
    local recovery=$(( fin - inicio ))
    docker exec sistema_cache redis-cli set "${modo}:recovery_time" "$recovery" > /dev/null
    ok "Recovery time: ${recovery}s — backlog vaciado"
}



clear
header "Casos del experimento — Tarea 2"
paso "Construyendo imágenes..."
docker compose build
ok "Build completado"
mkdir -p resultados
rm -f resultados/*.txt   # limpiar resultados anteriores

# ESCENARIO 1: SISTEMA BASE SÍNCRONO (sin Kafka), replica la arquitectura de la Tarea 1 para línea base de comparación, MODO_TRANSPORTE=sincrono hace que main.py hable directo con Redis/engine, sin publicar en Kafka (que no está levantado en este escenario).
separador
header "ESCENARIO 1 — Sistema Base Síncrono (sin Kafka)"

paso "Levantando solo cache + engine..."
docker compose up -d cache generador_respuestas
sleep 15

for modo in uniforme zipf; do
    paso "Corriendo simulación SÍNCRONA modo=$modo..."
    docker compose run --rm \
        -e SIMULATION_MODE="$modo" \
        -e N_PEDIDOS=$N_PEDIDOS \
        -e DELAY_MS=$DELAY_MS \
        -e MODO_TRANSPORTE=sincrono \
        generador_trafico
    guardar_metricas "caso1_sincrono_$modo" "$modo"
    limpiar_redis
    docker compose restart generador_respuestas
    sleep 10
done

docker compose down -v
ok "Escenario 1 completado"

# Escenario 2: Kafka + 1 Consumer

separador
header "ESCENARIO 2 — Kafka + 1 Consumer"

paso "Levantando infraestructura completa con 1 consumer..."
docker compose up -d cache zookeeper kafka kafka-setup generador_respuestas consumer consumer_retry consumer_dlq kafka-ui
sleep 30

for modo in uniforme zipf; do
    paso "Corriendo tráfico Kafka modo=$modo | 1 consumer..."
    docker compose run --rm \
        -e SIMULATION_MODE="$modo" \
        -e N_PEDIDOS=$N_PEDIDOS \
        -e DELAY_MS=$DELAY_MS \
        generador_trafico
    esperar_backlog 180
    guardar_metricas "caso2_kafka_1consumer_$modo" "$modo"
    limpiar_redis
    docker compose restart generador_respuestas
    sleep 10
done

docker compose down -v
ok "Escenario 2 completado"


# Escenario 3: Kafka + Múltimples consumers, loop sobre distintas cantidades para ver impacto en throughput, latencia y backlog

separador
header "ESCENARIO 3 — Kafka + Múltiples Consumers"
for n_consumers in "${CONSUMERS_E3[@]}"; do
    paso "Levantando infraestructura con $n_consumers consumers..."
    docker compose up -d cache zookeeper kafka kafka-setup generador_respuestas consumer_retry consumer_dlq kafka-ui
    docker compose up -d --scale consumer=$n_consumers consumer
    sleep 30

    paso "Corriendo tráfico | $n_consumers consumers | modo=uniforme..."
    docker compose run --rm \
        -e SIMULATION_MODE=uniforme \
        -e N_PEDIDOS=$N_PEDIDOS \
        -e DELAY_MS=$DELAY_MS \
        generador_trafico
    esperar_backlog 180
    guardar_metricas "caso3_kafka_${n_consumers}consumers" "uniforme"
    limpiar_redis

    docker compose down -v
    sleep 5
done
ok "Escenario 3 completado"

# Escenario 4: Falla temporal del engine

separador
header "ESCENARIO 4 — Falla Temporal del Engine"
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

    #historial de lag durante la caída, para ver cómo crece el backlog en Kafka, y registrar el peak en Redis para cada tiempo de falla
    csv_file="resultados/lag_historico_${tiempo_falla}s.csv"
    echo "t,lag,fase" > "$csv_file"
    t0=$(date +%s)

    # Fase normal: grabar lag mientras llega tráfico antes de caer
    deadline_pre=$(( $(date +%s) + 15 ))
    while [ $(date +%s) -lt $deadline_pre ]; do
        lag=$(obtener_lag)
        t=$(( $(date +%s) - t0 ))
        echo "$t,$lag,normal" >> "$csv_file"
        sleep 3
    done
    warn "Simulando caída del engine (${tiempo_falla} segundos)..."
    docker stop servicio_respuestas
    echo -e "${RED}  ✗ Engine caído — backlog creciendo en Kafka${NC}"


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
    ok "Engine recuperado — midiendo recovery time y grabando historial..."
 
    # Grabar lag durante la recuperación hasta llegar a 0
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
 
    # Guardar recovery time en Redis
    fin=$(date +%s)
    recovery=$(( fin - inicio_recovery ))
    docker exec sistema_cache redis-cli set "uniforme:recovery_time" "$recovery" > /dev/null
    ok "Recovery time: ${recovery}s — historial guardado en $csv_file"
 
    guardar_metricas "caso4_falla_${tiempo_falla}s" "uniforme"
    docker compose down -v
    sleep 5
done
ok "Escenario 4 completado"


# Escenario 5: reintentos con falla_rate
export CONF_DECIMALES=4 # para generar más claves únicas y forzar más misses, lo que hace que los reintentos tengan más chances de entrar en acción, especialmente con altos falla_rate
separador
header "ESCENARIO 5 — Reintentos con FALLA_RATE"
for falla_rate in "${FALLA_RATES[@]}"; do
    for n_consumers in "${CONSUMERS_E5[@]}"; do
        paso "Levantando infraestructura | FALLA_RATE=$falla_rate | consumers=$n_consumers..."
        export FALLA_RATE=$falla_rate
        docker compose up -d \
            cache zookeeper kafka kafka-setup generador_respuestas consumer_retry consumer_dlq kafka-ui
        docker compose up -d --scale consumer=$n_consumers consumer
        sleep 30


        paso "Corriendo tráfico con FALLA_RATE=$falla_rate | $n_consumers consumers..."
        docker compose run --rm \
            -e SIMULATION_MODE=uniforme \
            -e N_PEDIDOS=$N_PEDIDOS \
            -e DELAY_MS=$DELAY_MS \
            generador_trafico
        esperar_backlog 180
        guardar_metricas "caso5_falla${falla_rate}_${n_consumers}consumers" "uniforme"
        limpiar_redis

        docker compose down -v
        sleep 5
    done
done
ok "Escenario 5 completado"

export FALLA_RATE=0.0 # se guardaba en la shell, afectando los otros casos
export CONF_DECIMALES=2 # volver a 2 decimales para escenario 6 y 7, para no generar tantas claves únicas y que el spike tenga más impacto en el cache y en los reintentos

# Escenario 6: Spike de consultas
separador
header "ESCENARIO 6 — Spike de Tráfico"
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

# Escenario 7: Recuperación antes fallos — Síncrono vs Kafka

separador
header "ESCENARIO 7 — Recuperación ante Fallos: Síncrono vs Kafka"

# 7A — Síncrono: caída del engine = pérdida directa de consultas
paso "7A: Sistema SÍNCRONO con caída del engine..."
docker compose up -d cache generador_respuestas
sleep 15

docker compose run --rm -d \
    -e SIMULATION_MODE=uniforme \
    -e N_PEDIDOS=$N_PEDIDOS \
    -e DELAY_MS=$DELAY_MS \
    -e MODO_TRANSPORTE=sincrono \
    generador_trafico

sleep 10
warn "Cayendo engine en modo SÍNCRONO..."
docker stop servicio_respuestas
sleep 20
docker start servicio_respuestas
sleep 10
guardar_metricas "caso7a_recuperacion_sincrono" "uniforme"
limpiar_redis
docker compose down -v
sleep 5

# 7B — Kafka: loop sobre distintas cantidades de consumers
for n_consumers in "${CONSUMERS_E7[@]}"; do
    paso "7B: Sistema KAFKA con caída del engine | $n_consumers consumers..."
    docker compose up -d cache zookeeper kafka kafka-setup generador_respuestas consumer_retry consumer_dlq kafka-ui
    docker compose up -d --scale consumer=$n_consumers consumer
    sleep 30

    docker compose run --rm -d \
        -e SIMULATION_MODE=uniforme \
        -e N_PEDIDOS=$N_PEDIDOS \
        -e DELAY_MS=$DELAY_MS \
        generador_trafico

    sleep 10
    warn "Cayendo engine en modo KAFKA (${n_consumers} consumers)..."
    docker stop servicio_respuestas

    # Registrar peak de backlog mientras engine está caído
    deadline=$(( $(date +%s) + 20 ))
    while [ $(date +%s) -lt $deadline ]; do
        lag=$(obtener_lag)
        if [ -n "$lag" ] && [ "$lag" -gt 0 ] 2>/dev/null; then
            docker exec sistema_cache redis-cli \
                eval "local cur=tonumber(redis.call('get',KEYS[1]) or 0); if tonumber(ARGV[1])>cur then redis.call('set',KEYS[1],ARGV[1]) end" \
                1 "uniforme:backlog_peak" "$lag" > /dev/null 2>&1
        fi
        echo "  Engine caído — Backlog: $lag msgs"
        sleep 3
    done

    docker start servicio_respuestas
    ok "Engine recuperado — Kafka retiene las consultas acumuladas"
    medir_recovery_time uniforme 300
    guardar_metricas "caso7b_kafka_${n_consumers}consumers" "uniforme"
    docker compose down -v
    sleep 5
done
ok "Escenario 7 completado"

separador
header "TODOS LOS EXPERIMENTOS COMPLETADOS"
echo -e "${GREEN}Los resultados están en: ${BOLD}./resultados/${NC}"
echo -e "${CYAN}Kafka UI: ${BOLD}http://localhost:8080${NC}\n"