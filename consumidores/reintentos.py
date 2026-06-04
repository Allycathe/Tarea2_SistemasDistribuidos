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
        console.print(f"  [bold white]✗ DLQ[/bold white]        [dim]{key}[/dim] [white](agotó {mensaje['retry_count']} intentos)[/white]")
    else:
        producer.send(
            "consultas-reintento",
            key=mensaje["id"].encode(),
            value=json.dumps(mensaje).encode()
        )
        r.incr(f"{modo}:retry_count")
        console.print(f"  [bold white]↩ REINTENTO {mensaje['retry_count']}[/bold white]  [dim]{key}[/dim] [white](vuelve a consultas-reintento)[/white]")

console.print(Panel(
    "[bold white]CONSUMER DE REINTENTOS ONLINE[/bold white]\n"
    f"[dim]Escuchando: consultas-reintento | MAX_REINTENTOS={MAX_REINTENTOS}[/dim]",
    border_style="white"
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
            console.print(f"  [bold white]✓ RECUPERADO[/bold white]  [white]{key}[/white] [white]({latencia:.2f}ms — estaba en caché)[/white]")
        else:
            # Todavía no está: reenvía al engine y espera un poco
            r.lpush("cola:consultas", json.dumps(mensaje))
            console.print(f"  [bold white]· MISS[/bold white]        [white]{key}[/white] [white]→ reenviado al engine[/white]")

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
                console.print(f"  [bold white]✓ RECUPERADO[/bold white]  [white]{key}[/white] [white]({latencia:.2f}ms — engine respondió)[/white]")
            else:
                console.print(f"  [bold white]✗ TIMEOUT[/bold white]     [white]{key}[/white] [white]({latencia:.2f}ms — engine no respondió)[/white]")
                manejar_fallo(mensaje)

    except KeyError as e:
        console.print(f"  [white]Error de clave: {e}[/white]")
    except Exception as e:
        console.print(f"  [white]Error inesperado: {e}[/white]")
        try:
            manejar_fallo(mensaje)
        except Exception as e2:
            console.print(f"  [white]Error al manejar fallo: {e2}[/white]")