import redis
import numpy as np
import random
import time
import json
from rich.console import Console
from rich.panel import Panel
import os
import uuid
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=os.getenv('KAFKA_HOST', 'kafka:9092'))
time.sleep(5)
console = Console()

try:
    r = redis.Redis(host=os.getenv('REDIS_HOST', 'cache'), port=6379, decode_responses=True)
    r.ping()
    console.print(Panel("[bold green]✔[/bold green] Conexión exitosa con [bold cyan]Redis[/bold cyan]", border_style="green"))
except redis.ConnectionError:
    console.print(Panel("[bold red]✘[/bold red] No se pudo conectar con [bold yellow]Redis[/bold yellow]", border_style="red"))

ZONAS = ["Z1", "Z2", "Z3", "Z4", "Z5"]
CONSULTAS = ["Q1", "Q2", "Q3", "Q4", "Q5"]
N_PEDIDOS = 10000

def enviar_a_sistema(key, tipo, zona, conf, modo, zona_b=None, bins=5):
    datos_consulta = {
        "id": str(uuid.uuid4()),
        "timestamp": time.time(),
        "retry_count": 0,
        "tipo": tipo,
        "zona": zona,
        "zona_b": zona_b,
        "confidence_min": conf,
        "bins": bins,
        "cache_key": key,
        "modo": modo
    }
    producer.send(
        'consultas-principales',   # ← corregido, antes era tipo (Q1, Q2...)
        key=zona.encode(),
        value=json.dumps(datos_consulta).encode()
    )
    console.print(f"[bold blue]→ ENVIADO[/bold blue] [white]{key}[/white]")

def ejecutar_simulacion(modo):
    console.print(f"\n[bold reverse] INICIANDO SIMULACIÓN: {modo.upper()} [/bold reverse]\n")

    for _ in range(N_PEDIDOS):
        if modo == "zipf":
            idx = (np.random.zipf(a=1.2) - 1) % len(ZONAS)
            zona = ZONAS[idx]
        else:
            zona = random.choice(ZONAS)

        tipo = random.choice(CONSULTAS)
        conf = round(random.uniform(0.0, 0.9), 1)

        zona_b = None
        bins = None
        if tipo == "Q1":
            key = f"count:{zona}:conf={conf}"
        elif tipo == "Q2":
            key = f"area:{zona}:conf={conf}"
        elif tipo == "Q3":
            key = f"density:{zona}:conf={conf}"
        elif tipo == "Q4":
            zona_b = random.choice([z for z in ZONAS if z != zona])
            key = f"compare:density:{zona}:{zona_b}:conf={conf}"
        elif tipo == "Q5":
            bins = random.choice([5, 10, 20])
            key = f"confidence_dist:{zona}:bins={bins}"

        enviar_a_sistema(key, tipo, zona, conf, modo, zona_b, bins)
        time.sleep(0.001)

    producer.flush()
    console.print(f"\n[bold green] Fin de la simulación {modo.upper()}[/bold green]")
    console.print("[dim]──────────────────────────────────────────────────[/dim]\n")

def esperar_engine():
    with console.status("[bold yellow]Esperando a que el Motor cargue el dataset...", spinner="bouncingBar"):
        while not r.get("status:engine_ready"):
            time.sleep(2)
    console.print(Panel(" [bold green]Dataset detectado[/bold green] Preparando simulación...", border_style="bright_blue"))

if __name__ == "__main__":
    esperar_engine()
    modo = os.getenv("SIMULATION_MODE", "uniforme")
    ejecutar_simulacion(modo)
