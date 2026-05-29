#!/bin/bash
# ============================================================
#  run.sh — Escenarios de evaluación Tarea 2
#  Sistemas Distribuidos 2026-1
# ============================================================

# ── Parámetros ajustables ────────────────────────────────────
N_PEDIDOS=3000          # consultas por simulación
DELAY_MS=10             # delay entre consultas en ms (0.01s)
FALLA_ESPERA_INICIO=8   # segundos antes de matar el engine
FALLA_DURACION=20       # segundos que el engine permanece caído
FALLA_RECOVERY=25       # segundos para drenar reintentos tras recovery
SPIKE_PEDIDOS=8000      # consultas para el escenario spike
SPIKE_DELAY_MS=1        # delay mínimo para el spike
# ────────────────────────────────────────────────────────────

set -e

GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
RED='\033[0;31m'
NC='\033[0m'

RESULTADOS_DIR="resultados"
mkdir -p "$RESULTADOS_DIR"

RESUMEN="$RESULTADOS_DIR/resumen_experimentos.txt"
echo "================================================" > "$RESUMEN"
echo "  RESUMEN EXPERIMENTOS — TAREA 2" >> "$RESUMEN"
echo "  $(date)" >> "$RESUMEN"
echo "  N_PEDIDOS=$N_PEDIDOS  DELAY_MS=${DELAY_MS}ms" >> "$RESUMEN"
echo "================================================" >> "$RESUMEN"

# ── Helpers visuales ─────────────────────────────────────────
draw_header() {
    echo ""
    echo -e "${CYAN}╔══════════════════════════════════════════════════════════════╗${NC}"
    printf "${CYAN}║${NC}  ${YELLOW}%-58s${NC}  ${CYAN}║${NC}\n" "$1"
    echo -e "${CYAN}╚══════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

draw_step() {
    echo -e "${BLUE}  ▶ $1${NC}"
}

draw_ok() {
    echo -e "${GREEN}  ✓ $1${NC}"
}

draw_warn() {
    echo -e "${RED}  ✗ $1${NC}"
}

# ── Infraestructura ──────────────────────────────────────────

# Levanta zookeeper, kafka, kafka-setup, cache, engine, consumer_retry
# NO levanta consumers principales (se escalan por escenario)
infra_up() {
    draw_step "Levantando infraestructura (kafka, cache, engine, consumer_retry)..."
    docker compose up -d \
        zookeeper kafka kafka-setup \
        cache \
        generador_respuestas \
        consumer_retry \
        kafka-ui

    draw_step "Esperando que el engine cargue el dataset..."
    local intentos=0
    until docker compose exec -T cache redis-cli get "status:engine_ready" 2>/dev/null | grep -q "1"; do
        sleep 3
        intentos=$((intentos + 1))
        if [ $intentos -gt 40 ]; then
            draw_warn "Timeout esperando el engine. Abortando."
            exit 1
        fi
    done
    draw_ok "Engine listo."
}

# Escala el consumer principal a N instancias
consumers_up() {
    local n=$1
    draw_step "Escalando consumer principal a $n instancia(s)..."
    docker compose up -d --scale consumer=$n --no-recreate consumer
    sleep 4   # dar tiempo al rebalance de Kafka
    draw_ok "$n consumer(s) activos."
}

# Limpia Redis y reinicia el engine para experimento limpio
flush_y_reiniciar() {
    draw_step "Limpiando Redis y reiniciando engine..."
    docker compose exec -T cache redis-cli flushall > /dev/null
    docker compose restart generador_respuestas > /dev/null
    sleep 10
    draw_ok "Sistema limpio y listo."
}

# Corre el generador de tráfico y espera a que termine
run_traffic() {
    local modo=$1
    local n_pedidos=${2:-$N_PEDIDOS}
    local delay=${3:-$DELAY_MS}
    draw_step "Tráfico: modo=$modo  consultas=$n_pedidos  delay=${delay}ms"
    docker compose run --rm \
        -e SIMULATION_MODE="$modo" \
        -e N_PEDIDOS="$n_pedidos" \
        -e DELAY_MS="$delay" \
        generador_trafico
    draw_ok "Tráfico $modo terminado."
}

# Imprime métricas y guarda en archivo
collect_metrics() {
    local modo=$1
    local escenario=$2
    draw_step "Recolectando métricas: escenario=$escenario  modo=$modo"

    # Dar 3s para que los últimos mensajes se asienten en Redis
    sleep 3

    docker compose run --rm \
        -e MODO_METRICAS="$modo" \
        -e ESCENARIO="$escenario" \
        metricas

    # Agregar separador al resumen
    echo "" >> "$RESUMEN"
    echo "── $escenario / $modo ──────────────────────────" >> "$RESUMEN"

    # Copiar el último archivo de métricas al resumen
    ultimo=$(ls -t "$RESULTADOS_DIR"/metricas-*.txt 2>/dev/null | head -1)
    if [ -n "$ultimo" ]; then
        cat "$ultimo" >> "$RESUMEN"
        draw_ok "Métricas guardadas en $ultimo"
    fi
}

# Baja todo limpiamente
infra_down() {
    draw_step "Deteniendo servicios..."
    docker compose down -v --remove-orphans 2>/dev/null || true
    sleep 3
    draw_ok "Servicios detenidos."
}

# ════════════════════════════════════════════════════════════
clear
docker compose build
draw_header "BATERÍA DE ESCENARIOS — TAREA 2"
echo -e "  N_PEDIDOS=${YELLOW}$N_PEDIDOS${NC}  DELAY=${YELLOW}${DELAY_MS}ms${NC}  SPIKE=${YELLOW}$SPIKE_PEDIDOS consultas${NC}"
echo ""

# ════════════════════════════════════════════════════════════
# ESCENARIO 1 — Sistema Base Síncrono (sin Kafka)
# Levanta solo cache + engine, el tráfico va directo a Redis
# sin pasar por Kafka, para comparar con T1
# ════════════════════════════════════════════════════════════
draw_header "ESCENARIO 1: Sistema Base Síncrono (sin Kafka)"

docker compose up -d cache generador_respuestas
draw_step "Esperando engine..."
intentos=0
until docker compose exec -T cache redis-cli get "status:engine_ready" 2>/dev/null | grep -q "1"; do
    sleep 3; intentos=$((intentos+1))
    [ $intentos -gt 40 ] && { draw_warn "Timeout engine"; exit 1; }
done
draw_ok "Engine listo."

run_traffic "uniforme" $N_PEDIDOS $DELAY_MS
collect_metrics "uniforme" "1_sincrono"

flush_y_reiniciar

run_traffic "zipf" $N_PEDIDOS $DELAY_MS
collect_metrics "zipf" "1_sincrono"

infra_down

# ════════════════════════════════════════════════════════════
# ESCENARIO 2 — Kafka + 1 Consumer
# ════════════════════════════════════════════════════════════
draw_header "ESCENARIO 2: Kafka + 1 Consumer"

infra_up
consumers_up 1

run_traffic "uniforme" $N_PEDIDOS $DELAY_MS
collect_metrics "uniforme" "2_kafka_1consumer"

flush_y_reiniciar
consumers_up 1

run_traffic "zipf" $N_PEDIDOS $DELAY_MS
collect_metrics "zipf" "2_kafka_1consumer"

infra_down

# ════════════════════════════════════════════════════════════
# ESCENARIO 3 — Kafka + Múltiples Consumers
# Se prueba con 2, 3 y 5 consumers
# ════════════════════════════════════════════════════════════
for N_CONSUMERS in 2 3 5; do
    draw_header "ESCENARIO 3: Kafka + ${N_CONSUMERS} Consumers"

    infra_up
    consumers_up $N_CONSUMERS

    run_traffic "uniforme" $N_PEDIDOS $DELAY_MS
    collect_metrics "uniforme" "3_kafka_${N_CONSUMERS}consumers"

    flush_y_reiniciar
    consumers_up $N_CONSUMERS

    run_traffic "zipf" $N_PEDIDOS $DELAY_MS
    collect_metrics "zipf" "3_kafka_${N_CONSUMERS}consumers"

    infra_down
done

# ════════════════════════════════════════════════════════════
# ESCENARIO 4 — Falla Temporal del Engine
# Levanta tráfico en background, mata el engine a los N segundos,
# lo restaura después de FALLA_DURACION segundos
# ════════════════════════════════════════════════════════════
draw_header "ESCENARIO 4: Falla Temporal del Engine"

infra_up
consumers_up 1

draw_step "Iniciando tráfico en background..."
docker compose up -d generador_trafico

draw_step "Esperando ${FALLA_ESPERA_INICIO}s antes de simular falla..."
sleep $FALLA_ESPERA_INICIO

draw_warn "Deteniendo engine (falla simulada)..."
docker compose stop generador_respuestas

draw_step "Engine caído por ${FALLA_DURACION}s — los timeouts deberían ir a reintento..."
sleep $FALLA_DURACION

draw_ok "Restaurando engine..."
docker compose start generador_respuestas

draw_step "Esperando ${FALLA_RECOVERY}s para que consumer_retry drene la cola..."
sleep $FALLA_RECOVERY

# Detener tráfico si aún corre
docker compose stop generador_trafico 2>/dev/null || true

collect_metrics "uniforme" "4_falla_temporal"
infra_down

# ════════════════════════════════════════════════════════════
# ESCENARIO 5 — Reintentos hasta DLQ
# Mata el engine inmediatamente para forzar que todas las
# consultas agoten reintentos y terminen en DLQ
# ════════════════════════════════════════════════════════════
draw_header "ESCENARIO 5: Reintentos y Dead Letter Queue"

infra_up
consumers_up 1

draw_step "Iniciando tráfico en background..."
docker compose up -d generador_trafico

sleep 3
draw_warn "Deteniendo engine para forzar reintentos → DLQ..."
docker compose stop generador_respuestas

# MAX_REINTENTOS=3, cada intento espera hasta 1s (10 x 100ms)
# Con 3 reintentos necesitamos al menos 30s para agotar
draw_step "Esperando 40s para que los reintentos se agoten..."
sleep 40

docker compose stop generador_trafico 2>/dev/null || true
collect_metrics "uniforme" "5_reintentos_dlq"

draw_ok "Restaurando engine y midiendo recuperación tardía..."
docker compose start generador_respuestas
sleep 20

collect_metrics "uniforme" "5_reintentos_recovery"
infra_down

# ════════════════════════════════════════════════════════════
# ESCENARIO 6 — Spike de Tráfico
# Ráfaga de SPIKE_PEDIDOS consultas con delay mínimo
# Se prueba con 1 y 3 consumers para ver diferencia
# ════════════════════════════════════════════════════════════
draw_header "ESCENARIO 6: Spike de Tráfico"

for N_CONSUMERS in 1 3; do
    draw_step "Spike con $N_CONSUMERS consumer(s)..."
    infra_up
    consumers_up $N_CONSUMERS

    run_traffic "uniforme" $SPIKE_PEDIDOS $SPIKE_DELAY_MS
    collect_metrics "uniforme" "6_spike_${N_CONSUMERS}consumers"

    infra_down
done

# ════════════════════════════════════════════════════════════
# ESCENARIO 7 — Recuperación: Síncrono vs Kafka
# Repite falla temporal con 1 consumer para comparar
# directamente contra el escenario 1 (sin Kafka)
# ════════════════════════════════════════════════════════════
draw_header "ESCENARIO 7: Recuperación — Síncrono vs Kafka"

draw_step "Parte A: síncrono con falla (sin Kafka)..."
docker compose up -d cache generador_respuestas
intentos=0
until docker compose exec -T cache redis-cli get "status:engine_ready" 2>/dev/null | grep -q "1"; do
    sleep 3; intentos=$((intentos+1))
    [ $intentos -gt 40 ] && { draw_warn "Timeout engine"; exit 1; }
done

# Tráfico directo a Redis (modo síncrono)
SIMULATION_MODE=uniforme KAFKA_DISABLED=1 N_PEDIDOS=$N_PEDIDOS DELAY_MS=$DELAY_MS \
    docker compose up -d generador_trafico

sleep $FALLA_ESPERA_INICIO
draw_warn "Falla en modo síncrono — engine caído..."
docker compose stop generador_respuestas
sleep $FALLA_DURACION
docker compose start generador_respuestas
sleep 10
docker compose stop generador_trafico 2>/dev/null || true
collect_metrics "uniforme" "7_recovery_sincrono"
infra_down

draw_step "Parte B: Kafka con falla y recovery..."
infra_up
consumers_up 1

docker compose up -d generador_trafico

sleep $FALLA_ESPERA_INICIO
draw_warn "Falla en modo Kafka — engine caído..."
docker compose stop generador_respuestas
sleep $FALLA_DURACION
draw_ok "Restaurando engine con Kafka..."
docker compose start generador_respuestas
sleep $FALLA_RECOVERY
docker compose stop generador_trafico 2>/dev/null || true
collect_metrics "uniforme" "7_recovery_kafka"
infra_down

# ════════════════════════════════════════════════════════════
draw_header "TODOS LOS ESCENARIOS COMPLETADOS"
echo -e "${GREEN}"
echo "  Resultados individuales en: ./$RESULTADOS_DIR/"
echo "  Resumen consolidado en:     ./$RESULTADOS_DIR/resumen_experimentos.txt"
echo ""
echo "  Archivos generados:"
ls -1 "$RESULTADOS_DIR"/*.txt 2>/dev/null | sed 's/^/    /'
echo -e "${NC}"