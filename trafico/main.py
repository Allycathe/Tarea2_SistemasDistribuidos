import redis
import numpy as np
import random
import time
import json
from rich.console import Console
from rich.progress import track
from rich.panel import Panel
import os

console = Console()
# Configuración inicial
try:
    r = redis.Redis(host='cache', port=6379, decode_responses=True)
    r.ping()
    console.print(Panel("[bold green]✔[/bold green] Conexión exitosa con [bold cyan]Redis[/bold cyan]", border_style="green"))
except redis.ConnectionError:
    console.print(Panel("[bold red]✘[/bold red] No se pudo conectar con [bold yellow]Redis[/bold yellow]", border_style="red"))

ZONAS = ["Z1", "Z2", "Z3", "Z4", "Z5"] 
CONSULTAS = ["Q1", "Q2", "Q3", "Q4", "Q5"] 
N_PEDIDOS = 100000 # Cantidad de consultas por experimento

def enviar_a_sistema(key, tipo, zona, conf, modo, zona_b=None, bins=5):
    t0 = time.perf_counter()
    respuesta = r.get(key)
    latencia_ms = (time.perf_counter() - t0) * 1000
    
    if respuesta:
        #CACHE HIT 
        r.incr(f"{modo}:hits") 
        r.rpush(f"{modo}:latencies", latencia_ms)
        r.rpush(f"{modo}:timestamps", time.time())
        console.print(f"[bold green]+ HIT [/bold green] [white]{key}[/white] [dim]({latencia_ms:.2f}ms)[/dim]") 
    else:
        #CACHE MISS
        r.incr(f"{modo}:misses")
        console.print(f"[bold red]- MISS[/bold red] [white]{key}[/white] [dim]→ Motor[/dim]")
        
        datos_consulta = {
            "tipo": tipo,
            "zona": zona,
            "zona_b": zona_b,
            "confidence_min": conf,
            "bins": bins,
            "cache_key": key,
            "modo": modo 
        }
        r.lpush("cola:consultas", json.dumps(datos_consulta))
    
    # Métricas de politicas de removición
    info = r.info("stats")
    r.rpush(f"{modo}:evictions", f"{time.time()}:{info['evicted_keys']}")

def ejecutar_simulacion(modo):
    console.print(f"\n[bold reverse] INICIANDO SIMULACIÓN: {modo.upper()} [/bold reverse]\n")
    #r.flushall() # Existe una variable que permite sincronizar el tráfico con las respuestas, si descomentá, el código podría no funcionar(deadlopck :()

    for _ in range(N_PEDIDOS):
        # 1. Selección de zona
        if modo == "zipf":
            idx = (np.random.zipf(a=1.2) - 1) % len(ZONAS)
            zona = ZONAS[idx]
        else:
            zona = random.choice(ZONAS)

        # 2. Selección de tipo
        tipo = random.choice(CONSULTAS)
        conf = round(random.uniform(0.0, 0.9), 4) #confianza entre 0.0 y 0.9 para evitar casos extremos que podrían no ser representativos

        # 3. Construcción de la Cache Key
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

        # 4. Enviar y esperar un poco
        enviar_a_sistema(key, tipo, zona, conf, modo, zona_b, bins)
        time.sleep(0.001) # 50ms para que el backend procese

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


