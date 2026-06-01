import redis
import os
import time
import datetime
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from kafka import KafkaConsumer, TopicPartition
import json

console = Console()

r = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True
)


def obtener_backlog(topic="consultas-principales", group_id="grupo-consumidores"):
    """
    Calcula el lag real del consumer group: mensajes publicados − mensajes procesados.
    committed() devuelve None si el grupo aún no hizo commit en esa partición;
    en ese caso asumimos lag = 0 (grupo al día, sin pendientes reales).
    """
    try:
        tmp = KafkaConsumer(
            bootstrap_servers=os.getenv("KAFKA_HOST", "kafka:9092"),
            group_id=group_id,
            enable_auto_commit=False
        )
        partitions = tmp.partitions_for_topic(topic)
        if not partitions:
            tmp.close()
            return 0

        tps = [TopicPartition(topic, p) for p in partitions]
        tmp.assign(tps)
        end_offsets = tmp.end_offsets(tps)

        lag_total = 0
        for tp in tps:
            committed = tmp.committed(tp)   # None si el grupo nunca hizo commit
            end       = end_offsets[tp]
            # Si committed es None el grupo no tiene mensajes pendientes reales
            if committed is None:
                committed = end
            lag_total += max(0, end - committed)

        tmp.close()
        return lag_total
    except Exception as e:
        console.print(f"[dim red]No se pudo leer backlog de Kafka: {e}[/dim red]")
        return 0


def imprimir_resumen(modo):
    # Métricas de caché
    hits   = int(r.get(f"{modo}:hits")   or 0)
    misses = int(r.get(f"{modo}:misses") or 0)
    total  = hits + misses

    # Métricas de resiliencia Kafka
    retries    = int(r.get(f"{modo}:retry_count")     or 0)
    recoveries = int(r.get(f"{modo}:recovered_count") or 0)
    dlq        = int(r.get(f"{modo}:dlq_count")       or 0)
    backlog    = obtener_backlog("consultas-principales", "grupo-consumidores")

    # Tasas
    hit_rate      = round((hits      / total)   * 100, 2) if total   > 0 else 0
    retry_rate    = round((retries   / total)   * 100, 2) if total   > 0 else 0
    recovery_rate = round((recoveries / retries) * 100, 2) if retries > 0 else 0
    dlq_rate      = round((dlq       / total)   * 100, 2) if total   > 0 else 0

    # Latencias (end-to-end, registradas por los consumers)
    lats = [float(x) for x in r.lrange(f"{modo}:latencies", 0, -1)]
    lats_sorted = sorted(lats)
    n   = len(lats_sorted)
    p50 = lats_sorted[int(n * 0.50)]             if n > 0 else None
    p95 = lats_sorted[min(int(n * 0.95), n - 1)] if n > 0 else None

    # Throughput
    timestamps = [float(t) for t in r.lrange(f"{modo}:timestamps", 0, -1)]
    now    = time.time()
    recent = [t for t in timestamps if t >= now - 60]
    throughput = len(recent) / 60

    # Eviction rate
    evs_first = r.lindex(f"{modo}:evictions", 0)
    evs_last  = r.lindex(f"{modo}:evictions", -1)
    if evs_first and evs_last:
        t1, c1 = evs_first.split(":")
        t2, c2 = evs_last.split(":")
        dt = float(t2) - float(t1)
        eviction_rate = ((float(c2) - float(c1)) / dt) * 60 if dt > 0 else 0.0
    else:
        eviction_rate = 0.0

    # Guardar en archivo
    os.makedirs("resultados", exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    filename  = f"resultados/metricas-{os.getenv('ESCENARIO', 'exp')}-{timestamp}.txt"

    with open(filename, "a") as f:
        f.write(f"\nSIMULACION: {modo.upper()}\n")
        f.write("-" * 30 + "\n")
        f.write(f"Hits: {hits} | Misses: {misses} | Backlog: {backlog}\n")
        f.write(f"Hit Rate: {hit_rate}%\n")
        f.write(f"Retry Rate: {retry_rate}% | Recovery Rate: {recovery_rate}% | DLQ Rate: {dlq_rate}%\n")
        f.write(f"Latencia p50: {p50} ms | p95: {p95} ms\n")
        f.write(f"Throughput: {throughput} qps\n")
        f.write(f"Eviction Rate: {eviction_rate} ev/min\n")
        f.write("-" * 30 + "\n")

    # Tabla visual
    table = Table(
        title=f" Reporte de Simulación: {modo.upper()}",
        title_style="bold magenta",
        show_header=True,
        header_style="bold cyan"
    )
    table.add_column("Métrica",  style="dim")
    table.add_column("Valor",    justify="right", style="bold green")

    table.add_row("Hits / Misses",  f"{hits} / {misses}")
    table.add_row("Hit Rate",       f"{hit_rate}%")
    table.add_row("Latencia p50",   f"{round(p50, 2) if p50 else 0} ms")
    table.add_row("Latencia p95",   f"{round(p95, 2) if p95 else 0} ms")
    table.add_row("Throughput",     f"{round(throughput, 2)} qps")
    table.add_section()
    table.add_row("[yellow]Retry Rate[/yellow]",        f"[yellow]{retry_rate}%[/yellow]")
    table.add_row("[green]Recovery Rate[/green]",       f"[green]{recovery_rate}%[/green]")
    table.add_row("[red]DLQ Rate[/red]",                f"[red]{dlq_rate}%[/red]")
    table.add_row("[cyan]Backlog Size (Lag)[/cyan]",    f"[cyan]{backlog} msgs[/cyan]")

    console.print(Panel(table, expand=False, border_style="bright_blue"))
    return filename


time.sleep(2)
modo_objetivo = os.getenv("MODO_METRICAS", "uniforme")
imprimir_resumen(modo_objetivo)