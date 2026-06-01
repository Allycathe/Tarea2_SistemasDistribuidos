import redis
import numpy as np
import random
import time
import json
import os
import uuid

from rich.console import Console
from rich.panel import Panel

console = Console()

r = redis.Redis(
    host=os.getenv("REDIS_HOST", "cache"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True
)
try:
    r.ping()
    console.print(Panel("[bold green]✔[/bold green] Conexión exitosa con [bold cyan]Redis[/bold cyan]", border_style="green"))
except redis.ConnectionError:
    console.print(Panel("[bold red]✘[/bold red] No se pudo conectar con [bold yellow]Redis[/bold yellow]", border_style="red"))

# MODO_TRANSPORTE=sincrono → Escenario 1 (sin Kafka)
# MODO_TRANSPORTE=kafka    → Escenarios 2-7 (con Kafka)
MODO_TRANSPORTE = os.getenv("MODO_TRANSPORTE", "kafka")

producer = None
if MODO_TRANSPORTE == "kafka":
    from kafka import KafkaProducer

    def conectar_producer():
        while True:
            try:
                p = KafkaProducer(
                    bootstrap_servers=os.getenv("KAFKA_HOST", "kafka:9092"),
                    retries=5
                )
                console.print(Panel("[bold green]✔[/bold green] Conexión exitosa con [bold cyan]Kafka[/bold cyan]", border_style="green"))
                return p
            except Exception as e:
                console.print(f"[yellow]Esperando Kafka... ({e})[/yellow]")
                time.sleep(3)

    producer = conectar_producer()

ZONAS     = ["Z1", "Z2", "Z3", "Z4", "Z5"]
CONSULTAS = ["Q1", "Q2", "Q3", "Q4", "Q5"]

N_PEDIDOS = int(os.getenv("N_PEDIDOS", 5000))
DELAY_MS  = int(os.getenv("DELAY_MS", 5))

SPIKE_ENABLED   = os.getenv("SPIKE_ENABLED", "false").lower() == "true"
SPIKE_EN_PEDIDO = int(os.getenv("SPIKE_EN_PEDIDO", 2500))
SPIKE_DURACION  = int(os.getenv("SPIKE_DURACION", 500))
SPIKE_DELAY_MS  = int(os.getenv("SPIKE_DELAY_MS", 1))


def _construir_mensaje(key, tipo, zona, conf, modo, zona_b, bins):
    return {
        "id":             str(uuid.uuid4()),
        "timestamp":      time.time(),
        "retry_count":    0,
        "tipo":           tipo,
        "zona":           zona,
        "zona_b":         zona_b,
        "confidence_min": conf,
        "bins":           bins if bins else 5,
        "cache_key":      key,
        "modo":           modo
    }


def _publicar_kafka(key, tipo, zona, conf, modo, zona_b, bins):
    """Publica en el tópico principal de Kafka (Escenarios 2-7)."""
    mensaje = _construir_mensaje(key, tipo, zona, conf, modo, zona_b, bins)
    producer.send(
        "consultas-principales",
        key=zona.encode(),
        value=json.dumps(mensaje).encode()
    )
    console.print(f"[bold blue]→ ENVIADO[/bold blue] [white]{key}[/white]")


def _publicar_sincrono(key, tipo, zona, conf, modo, zona_b, bins):
    """
    Modo síncrono — Escenario 1 (sin Kafka).
    Consulta Redis directamente y, en caso de miss, espera al engine.
    Registra hits, misses y latencia end-to-end en Redis.
    """
    t0        = time.perf_counter()
    respuesta = r.get(key)
    latencia  = (time.perf_counter() - t0) * 1000

    if respuesta:
        r.incr(f"{modo}:hits")
        r.rpush(f"{modo}:latencies",  latencia)
        r.rpush(f"{modo}:timestamps", time.time())
        console.print(f"[bold green]✓ HIT[/bold green]  [white]{key}[/white] [green]({latencia:.2f}ms)[/green]")
    else:
        r.incr(f"{modo}:misses")
        mensaje = _construir_mensaje(key, tipo, zona, conf, modo, zona_b, bins)
        r.lpush("cola:consultas", json.dumps(mensaje))
        console.print(f"[bold yellow]· MISS[/bold yellow] [white]{key}[/white] → engine")

        # Esperar respuesta del engine (máx 5 s)
        deadline = time.perf_counter() + 5.0
        while time.perf_counter() < deadline:
            respuesta = r.get(key)
            if respuesta:
                break
            time.sleep(0.05)

        latencia = (time.perf_counter() - t0) * 1000
        # Registrar latencia end-to-end (incluye cómputo del engine)
        r.rpush(f"{modo}:latencies",  latencia)
        r.rpush(f"{modo}:timestamps", time.time())

    info = r.info("stats")
    r.rpush(f"{modo}:evictions", f"{time.time()}:{info['evicted_keys']}")


def publicar_consulta(key, tipo, zona, conf, modo, zona_b=None, bins=5):
    if MODO_TRANSPORTE == "sincrono":
        _publicar_sincrono(key, tipo, zona, conf, modo, zona_b, bins)
    else:
        _publicar_kafka(key, tipo, zona, conf, modo, zona_b, bins)


def ejecutar_simulacion(modo):
    console.print(
        f"\n[bold reverse] INICIANDO SIMULACIÓN: {modo.upper()} "
        f"| {N_PEDIDOS} consultas | delay={DELAY_MS}ms | transporte={MODO_TRANSPORTE} [/bold reverse]\n"
    )

    for i in range(N_PEDIDOS):
        en_spike = SPIKE_ENABLED and SPIKE_EN_PEDIDO <= i < SPIKE_EN_PEDIDO + SPIKE_DURACION
        if en_spike:
            if i == SPIKE_EN_PEDIDO:
                console.print(f"[bold yellow]⚡ Spike activado (pedidos {SPIKE_EN_PEDIDO}–{SPIKE_EN_PEDIDO + SPIKE_DURACION})[/bold yellow]")
            time.sleep(SPIKE_DELAY_MS / 1000.0)
        else:
            time.sleep(DELAY_MS / 1000.0)

        if modo == "zipf":
            idx  = (np.random.zipf(a=1.2) - 1) % len(ZONAS)
            zona = ZONAS[idx]
        else:
            zona = random.choice(ZONAS)

        tipo = random.choice(CONSULTAS)
        conf = round(random.uniform(0.0, 0.9), 4)

        zona_b = bins = None
        if tipo == "Q1":
            key = f"count:{zona}:conf={conf}"
        elif tipo == "Q2":
            key = f"area:{zona}:conf={conf}"
        elif tipo == "Q3":
            key = f"density:{zona}:conf={conf}"
        elif tipo == "Q4":
            zona_b = random.choice([z for z in ZONAS if z != zona])
            key    = f"compare:density:{zona}:{zona_b}:conf={conf}"
        elif tipo == "Q5":
            bins = random.choice([5, 10, 20])
            key  = f"confidence_dist:{zona}:bins={bins}"

        publicar_consulta(key, tipo, zona, conf, modo, zona_b, bins)

    if producer:
        producer.flush()

    console.print(f"\n[bold green]✔ Fin de la simulación {modo.upper()}[/bold green]")
    console.print("[dim]──────────────────────────────────────────────────[/dim]\n")


def esperar_engine():
    with console.status("[bold yellow]Esperando a que el Motor cargue el dataset...", spinner="bouncingBar"):
        while not r.get("status:engine_ready"):
            time.sleep(2)
    console.print(Panel(
        "[bold green]Dataset detectado[/bold green] — Preparando simulación...",
        border_style="bright_blue"
    ))


if __name__ == "__main__":
    esperar_engine()
    modo = os.getenv("SIMULATION_MODE", "uniforme")
    ejecutar_simulacion(modo)