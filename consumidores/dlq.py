from kafka import KafkaConsumer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
import json
import redis
import time
import os
import datetime

console = Console()

r = redis.Redis(
    host=os.getenv("REDIS_HOST", "cache"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True
)

consumer = KafkaConsumer(
    "dlq",
    bootstrap_servers=os.getenv("KAFKA_HOST", "kafka:9092"),
    # grupo propio para no interferir con ningún otro consumer
    group_id="grupo-dlq",
    # empieza desde el principio para no perder mensajes anteriores
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode())
)

os.makedirs("resultados", exist_ok=True)

console.print(Panel(
    "[bold white]MONITOR DLQ ONLINE[/bold white]\n"
    "[dim]Escuchando: dlq — registrando consultas irrecuperables[/dim]",
    border_style="white"
))

def registrar_en_archivo(mensaje):
    """Guarda cada mensaje de la DLQ en un archivo para análisis posterior."""
    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    filename  = "resultados/dlq.jsonl"

    entrada = {
        "registrado_en": timestamp,
        "id":            mensaje.get("id"),
        "tipo":          mensaje.get("tipo"),
        "zona":          mensaje.get("zona"),
        "cache_key":     mensaje.get("cache_key"),
        "retry_count":   mensaje.get("retry_count"),
        "modo":          mensaje.get("modo"),
        "timestamp_original": mensaje.get("timestamp"),
    }

    with open(filename, "a") as f:
        f.write(json.dumps(entrada) + "\n")

def imprimir_tabla_dlq():
    """Muestra un resumen acumulado de la DLQ por modo."""
    modos = ["uniforme", "zipf"]
    table = Table(
        title="Resumen DLQ",
        title_style="bold white",
        show_header=True,
        header_style="bold white"
    )
    table.add_column("Modo",      style="dim")
    table.add_column("Total DLQ", justify="right", style="bold white")

    for modo in modos:
        total = r.get(f"{modo}:dlq_count") or "0"
        table.add_row(modo, total)

    console.print(table)

for msg in consumer:
    try:
        mensaje = msg.value
        key     = mensaje.get("cache_key", "???")
        modo    = mensaje.get("modo", "uniforme")
        intentos = mensaje.get("retry_count", "?")

        # Registrar en Redis para métricas (por si el monitor arranca tarde
        # y dlq_count ya fue incrementado por principal/reintentos)
        r.incr(f"{modo}:dlq_count")

        # Guardar en archivo .jsonl para análisis
        registrar_en_archivo(mensaje)

        console.print(
            f"  [bold white]💀 DLQ[/bold white]  [white]{key}[/white] "
            f"[dim]modo={modo} | intentos={intentos}[/dim]"
        )

        # Cada 10 mensajes imprime el resumen acumulado
        total_dlq = int(r.get(f"{modo}:dlq_count") or 0)
        if total_dlq % 10 == 0:
            imprimir_tabla_dlq()

    except KeyError as e:
        console.print(f"  [white]Error de clave en DLQ: {e}[/white]")
    except Exception as e:
        console.print(f"  [white]Error inesperado en DLQ: {e}[/white]")