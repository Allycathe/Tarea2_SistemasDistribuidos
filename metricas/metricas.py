import redis
import os
import time
import datetime
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
import os

console = Console()

r = redis.Redis(host=os.getenv("REDIS_HOST", "localhost"), port=int(os.getenv("REDIS_PORT", 6379)), decode_responses=True)

def imprimir_resumen(modo):
    #Calculos base
    hits   = int(r.get(f"{modo}:hits") or 0)
    misses = int(r.get(f"{modo}:misses") or 0)
    total  = hits + misses

    # Hit Rate
    hit_rate = round((hits / total) * 100, 2) if total > 0 else 0
    
    #Latencias
    lats = [float(x) for x in r.lrange(f"{modo}:latencies", 0, -1)]
    lats_sorted = sorted(lats)
    n = len(lats_sorted)
    p50 = lats_sorted[int(n * 0.50)]             if n > 0 else None
    p95 = lats_sorted[min(int(n * 0.95), n - 1)] if n > 0 else None
    
    # Trhougput y evictions
    timestamps = [float(t) for t in r.lrange(f"{modo}:timestamps", 0, -1)]
    now = time.time()
    recent = [t for t in timestamps if t >= now - 60]
    throughput = len(recent) / 60

    evs_first = r.lindex(f"{modo}:evictions", 0)
    evs_last = r.lindex(f"{modo}:evictions", -1)

    # Eviction rate: (evs_last - evs_first) / (t_last - t_first) * 60
    if evs_first and evs_last:
        t1, c1 = evs_first.split(":")
        t2, c2 = evs_last.split(":")
        dt = float(t2) - float(t1)
        # Total de llaves borradas / tiempo total
        eviction_rate = ((float(c2) - float(c1)) / dt) * 60 if dt > 0 else 0.0
    else:
        eviction_rate = 0.0
    
    # Crear archivo por experimento
    os.makedirs('resultados', exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    filename = f"resultados/metricas-{timestamp}.txt"
    
    with open(filename, 'a') as f:
        f.write(f"\nSIMULACION: {modo.upper()}\n")
        f.write("-" * 30 + "\n")
        f.write(f"Hits: {hits} | Misses: {misses}\n")
        f.write(f"Hit Rate: {hit_rate}%\n")
        f.write(f"Latencia p50: {p50} ms | p95: {p95} ms\n")
        f.write(f"Throughput: {throughput} qps\n")
        f.write(f"Eviction Rate: {eviction_rate} ev/min\n")
        f.write("-" * 30 + "\n")

    table = Table(title=f" Reporte de Simulación: {modo.upper()}", title_style="bold magenta", show_header=True, header_style="bold cyan")
    
    table.add_column("Métrica", style="dim")
    table.add_column("Valor", justify="right", style="bold green")

    table.add_row("Hits", str(hits))
    table.add_row("Misses", str(misses))
    table.add_row("Hit Rate", f"{round(hits/total*100, 2) if total > 0 else 0}%")
    table.add_row("Latencia p50", f"{round(p50, 2) if p50 else 0} ms")
    table.add_row("Latencia p95", f"{round(p95, 2) if p95 else 0} ms")
    table.add_row("Throughput", f"{round(throughput, 4)} qps")
    table.add_row("Eviction Rate", f"{round(eviction_rate, 4)} ev/min")

    console.print(Panel(table, expand=False, border_style="bright_blue"))
    return filename

time.sleep(2) 
modo_objetivo = os.getenv("MODO_METRICAS", "uniforme")

imprimir_resumen(modo_objetivo)
