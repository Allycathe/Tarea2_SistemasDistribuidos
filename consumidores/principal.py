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
    "consultas-principales",
    bootstrap_servers=os.getenv("KAFKA_HOST", "kafka:9092"),
    group_id="grupo-consumidores",
    auto_offset_reset="earliest",
    enable_auto_commit=True,          # commit automático para que el lag se calcule bien
    auto_commit_interval_ms=1000,
    value_deserializer=lambda m: json.loads(m.decode())
)

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_HOST", "kafka:9092")
)

MAX_REINTENTOS = int(os.getenv("MAX_REINTENTOS", 3))
ENGINE_TIMEOUT = float(os.getenv("ENGINE_TIMEOUT_S", 5.0))   # espera máx al engine


def manejar_fallo(mensaje):
    """Envía al tópico de reintento o a la DLQ según retry_count."""
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
        console.print(f"  [bold white]✗ DLQ[/bold white]        [dim]{key}[/dim] [white](agotó {mensaje['retry_count']} intentos)[/white]")
    else:
        producer.send(
            "consultas-reintento",
            key=mensaje["id"].encode(),
            value=json.dumps(mensaje).encode()
        )
        r.incr(f"{modo}:retry_count")
        console.print(f"  [bold white]↩ REINTENTO {mensaje['retry_count']}[/bold white]  [dim]{key}[/dim]")


console.print(Panel(
    "[bold white]CONSUMER PRINCIPAL ONLINE[/bold white]\n"
    f"[dim]Escuchando: consultas-principales | MAX_REINTENTOS={MAX_REINTENTOS}[/dim]",
    border_style="white"
))

for msg in consumer:
    try:
        mensaje = msg.value
        key  = mensaje["cache_key"]
        modo = mensaje.get("modo", "uniforme")

        t0        = time.perf_counter()
        respuesta = r.get(key)
        latencia  = (time.perf_counter() - t0) * 1000

        if respuesta:
            # Cache hit: respuesta inmediata
            r.incr(f"{modo}:hits")
            r.rpush(f"{modo}:latencies",  latencia)
            r.rpush(f"{modo}:timestamps", time.time())
            console.print(f"  [bold white]✓ HIT[/bold white]         [white]{key}[/white] [white]({latencia:.2f}ms)[/white]")
        else:
            # Cache miss: delega al engine y ESPERA la respuesta
            r.incr(f"{modo}:misses")
            r.lpush("cola:consultas", json.dumps(mensaje))
            console.print(f"  [bold white]· MISS[/bold white]        [white]{key}[/white] → engine")

            # Esperar hasta ENGINE_TIMEOUT segundos a que el engine guarde en caché
            deadline = time.perf_counter() + ENGINE_TIMEOUT
            respuesta = None
            while time.perf_counter() < deadline:
                respuesta = r.get(key)
                if respuesta:
                    break
                time.sleep(0.05)

            latencia = (time.perf_counter() - t0) * 1000

            if respuesta:
                # Miss resuelto: latencia end-to-end incluye tiempo del engine
                r.rpush(f"{modo}:latencies",  latencia)
                r.rpush(f"{modo}:timestamps", time.time())
                console.print(f"  [bold white]✓ RESUELTO[/bold white]    [white]{key}[/white] [white]({latencia:.2f}ms)[/white]")
            else:
                # Engine no respondió a tiempo: reintento
                console.print(f"  [bold white]✗ TIMEOUT[/bold white]     [white]{key}[/white] [white]({latencia:.2f}ms)[/white]")
                manejar_fallo(mensaje)

    except KeyError as e:
        console.print(f"  [white]Error de clave: {e}[/white]")
    except Exception as e:
        console.print(f"  [white]Error inesperado: {e}[/white]")
        try:
            manejar_fallo(mensaje)
        except Exception as e2:
            console.print(f"  [white]Error al manejar fallo: {e2}[/white]")