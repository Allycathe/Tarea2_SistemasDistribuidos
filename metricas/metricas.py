import redis
import os
import time
import datetime
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from kafka import KafkaAdminClient
from kafka.structs import TopicPartition

console = Console()

r = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True
)

KAFKA_HOST = os.getenv("KAFKA_HOST", "kafka:9092")
TOPICOS_KAFKA = ["consultas-principales", "consultas-reintento", "dlq"]

def get_backlog():
    # Obtener el backlog (lag) de cada tópico en Kafka
    backlog = {}
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_HOST)
        from kafka import KafkaConsumer
        consumer_tmp = KafkaConsumer(
            bootstrap_servers=KAFKA_HOST,
            group_id='grupo-consumidores',
            enable_auto_commit=False
        )
        for topic in TOPICOS_KAFKA:
            partitions = consumer_tmp.partitions_for_topic(topic) or set()
            total_lag = 0
            for p in partitions:
                tp = TopicPartition(topic, p)
                # end offset (último mensaje escrito)
                end_offsets = consumer_tmp.end_offsets([tp])
                committed = consumer_tmp.committed(tp) or 0
                lag = end_offsets.get(tp, 0) - committed
                total_lag += max(lag, 0)
            backlog[topic] = total_lag
        consumer_tmp.close()
        admin.close()
    except Exception as e:
        console.print(f"[yellow]⚠ No se pudo obtener backlog de Kafka: {e}[/yellow]")
        for t in TOPICOS_KAFKA:
            backlog[t] = "N/A"
    return backlog


def imprimir_resumen(modo):
    # Métricas base de cache
    hits   = int(r.get(f"{modo}:hits")   or 0)
    misses = int(r.get(f"{modo}:misses") or 0)
    total  = hits + misses
    hit_rate = round((hits / total) * 100, 2) if total > 0 else 0

    # Latencias p50 y p95
    lats = [float(x) for x in r.lrange(f"{modo}:latencies", 0, -1)]
    lats_sorted = sorted(lats)
    n = len(lats_sorted)
    p50 = lats_sorted[int(n * 0.50)]             if n > 0 else None
    p95 = lats_sorted[min(int(n * 0.95), n - 1)] if n > 0 else None

    # Throughput (consultas por segundo en la última ventana de 60 segundos)
    timestamps = [float(t) for t in r.lrange(f"{modo}:timestamps", 0, -1)]
    now = time.time()
    recent = [t for t in timestamps if t >= now - 60]
    throughput = len(recent) / 60

    # Métricas Kafka (nuevas) 
    retry_count     = int(r.get(f"{modo}:retry_count")     or 0)
    dlq_count       = int(r.get(f"{modo}:dlq_count")       or 0)
    recovered_count = int(r.get(f"{modo}:recovered_count") or 0)

    retry_rate     = round((retry_count     / total) * 100, 2) if total > 0 else 0
    dlq_rate       = round((dlq_count       / total) * 100, 2) if total > 0 else 0
    recovery_rate  = round((recovered_count / max(retry_count, 1)) * 100, 2)

    #Backlog Kafka
    escenario = os.getenv("ESCENARIO", "")
    if "sincrono" not in escenario:
        backlog = get_backlog()
    else:
        backlog = {t: "N/A (escenario sin Kafka)" for t in TOPICOS_KAFKA}

    # Guardar en archivo y mostrar tabla Rich 
    os.makedirs('resultados', exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    filename  = f"resultados/metricas-{timestamp}.txt"

    with open(filename, 'a') as f:
        f.write(f"\nSIMULACION: {modo.upper()}\n")
        f.write("-" * 40 + "\n")
        f.write(f"Hits: {hits} | Misses: {misses} | Hit Rate: {hit_rate}%\n")
        f.write(f"Latencia p50: {p50} ms | p95: {p95} ms\n")
        f.write(f"Throughput: {throughput} qps\n")
        f.write(f"Retry Rate: {retry_rate}% ({retry_count} consultas)\n")
        f.write(f"DLQ Rate: {dlq_rate}% ({dlq_count} consultas)\n")
        f.write(f"Recovery Rate: {recovery_rate}% ({recovered_count} recuperadas)\n")
        for topic, lag in backlog.items():
            f.write(f"Backlog [{topic}]: {lag}\n")
        f.write("-" * 40 + "\n")

    # Tabla Rich
    table = Table(
        title=f" Reporte de Simulación: {modo.upper()}",
        title_style="bold magenta",
        show_header=True,
        header_style="bold cyan"
    )
    table.add_column("Métrica",  style="dim")
    table.add_column("Valor",    justify="right", style="bold green")

    table.add_row("Hits",           str(hits))
    table.add_row("Misses",         str(misses))
    table.add_row("Hit Rate",       f"{hit_rate}%")
    table.add_row("Latencia p50",   f"{round(p50, 2) if p50 else 0} ms")
    table.add_row("Latencia p95",   f"{round(p95, 2) if p95 else 0} ms")
    table.add_row("Throughput",     f"{round(throughput, 4)} qps")
    # métricas nuevas Kafka
    table.add_row("Retry Rate",     f"{retry_rate}% ({retry_count})")
    table.add_row("DLQ Rate",       f"{dlq_rate}% ({dlq_count})")
    table.add_row("Recovery Rate",  f"{recovery_rate}% ({recovered_count})")
    for topic, lag in backlog.items():
        table.add_row(f"Backlog [{topic}]", str(lag))

    console.print(Panel(table, expand=False, border_style="bright_blue"))
    return filename

# El time_sleep(2) se cambio por este code, esot porque métricas empezaba a analizar antes de tener listo el redis
modo_objetivo = os.getenv("MODO_METRICAS", "uniforme")

# Esperar a que haya datos reales en Redis
tiempo_espera = 0
while tiempo_espera < 30:
    hits   = int(r.get(f"{modo_objetivo}:hits")   or 0)
    misses = int(r.get(f"{modo_objetivo}:misses") or 0)
    if hits + misses > 0:
        break
    time.sleep(2)
    tiempo_espera += 2

imprimir_resumen(modo_objetivo)

modo_objetivo = os.getenv("MODO_METRICAS", "uniforme")
imprimir_resumen(modo_objetivo)