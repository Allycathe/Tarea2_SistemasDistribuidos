from kafka import KafkaConsumer, KafkaProducer
from rich.console import Console
from rich.panel import Panel
import json
import redis
import time
import os

console = Console()

r = redis.Redis(
    host=os.getenv("REDIS_HOST", "cache"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True
)

consumer = KafkaConsumer(
    "consultas-reintento",
    bootstrap_servers=os.getenv("KAFKA_HOST", "kafka:9092"),
    # grupo propio para no interferir con el consumer principal
    group_id="grupo-reintentos",
    value_deserializer=lambda m: json.loads(m.decode())
)

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_HOST", "kafka:9092")
)

MAX_REINTENTOS = int(os.getenv("MAX_REINTENTOS", 3))

def manejar_fallo(mensaje):
    """Reencola o manda a DLQ según retry_count."""
    mensaje["retry_count"] += 1
    modo = mensaje.get("modo", "uniforme")
    key  = mensaje["cache_key"]

    if mensaje["retry_count"] >= MAX_REINTENTOS:
        producer.send(
            "dlq",
            key=mensaje["id"].encode(),
            value=json.dumps(mensaje).encode()
        )
        r.incr(f"{modo}:dlq_count")
        console.print(f"  [bold red]✗ DLQ[/bold red]        [dim]{key}[/dim] [red](agotó {mensaje['retry_count']} intentos)[/red]")
    else:
        producer.send(
            "consultas-reintento",
            key=mensaje["id"].encode(),
            value=json.dumps(mensaje).encode()
        )
        r.incr(f"{modo}:retry_count")
        console.print(f"  [bold yellow]↩ REINTENTO {mensaje['retry_count']}[/bold yellow]  [dim]{key}[/dim] [yellow](vuelve a consultas-reintento)[/yellow]")

console.print(Panel(
    "[bold cyan]CONSUMER DE REINTENTOS ONLINE[/bold cyan]\n"
    f"[dim]Escuchando: consultas-reintento | MAX_REINTENTOS={MAX_REINTENTOS}[/dim]",
    border_style="cyan"
))

for msg in consumer:
    try:
        mensaje = msg.value
        key     = mensaje["cache_key"]
        modo    = mensaje.get("modo", "uniforme")
        intento = mensaje.get("retry_count", 1)

        console.rule(f"[dim]Reintento #{intento} — {key}[/dim]")

        t0        = time.perf_counter()
        respuesta = r.get(key)

        if respuesta:
            # Ya estaba en caché (el engine lo procesó mientras esperaba)
            latencia = (time.perf_counter() - t0) * 1000
            r.incr(f"{modo}:hits")
            r.incr(f"{modo}:recovered_count")
            r.rpush(f"{modo}:latencies", latencia)
            r.rpush(f"{modo}:timestamps", time.time())
            console.print(f"  [bold green]✓ RECUPERADO[/bold green]  [white]{key}[/white] [green]({latencia:.2f}ms — estaba en caché)[/green]")
        else:
            # Todavía no está: reenvía al engine y espera un poco
            r.lpush("cola:consultas", json.dumps(mensaje))
            console.print(f"  [bold yellow]· MISS[/bold yellow]        [white]{key}[/white] [yellow]→ reenviado al engine[/yellow]")

            # Espera acotada: máximo 2s para no bloquear indefinidamente
            deadline = time.perf_counter() + 2.0
            while time.perf_counter() < deadline:
                respuesta = r.get(key)
                if respuesta:
                    break
                time.sleep(0.1)

            latencia = (time.perf_counter() - t0) * 1000

            if respuesta:
                # No contamos como miss nuevo, solo como recovered
                r.incr(f"{modo}:recovered_count")
                r.rpush(f"{modo}:latencies", latencia)
                r.rpush(f"{modo}:timestamps", time.time())
                console.print(f"  [bold green]✓ RECUPERADO[/bold green]  [white]{key}[/white] [green]({latencia:.2f}ms — engine respondió)[/green]")
            else:
                console.print(f"  [bold red]✗ TIMEOUT[/bold red]     [white]{key}[/white] [red]({latencia:.2f}ms — engine no respondió)[/red]")
                manejar_fallo(mensaje)

    except KeyError as e:
        console.print(f"  [red]Error de clave: {e}[/red]")
    except Exception as e:
        console.print(f"  [red]Error inesperado: {e}[/red]")
        try:
            manejar_fallo(mensaje)
        except Exception as e2:
            console.print(f"  [red]Error al manejar fallo: {e2}[/red]")