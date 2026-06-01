import pandas as pd
import redis
import json
import time
import os
import numpy as np
from collections import namedtuple
from statistics import mean
from rich.console import Console
from rich.status import Status
from rich.panel import Panel

console = Console()

print("Iniciando engine.py...")
 #cambie el host a "cache" para que se conecte al contenedor de redis, pero tiene que ir cache que es el nombre del servicio en el compose
r = redis.Redis(host=os.getenv("REDIS_HOST", "localhost"),port=int(os.getenv("REDIS_PORT", 6379)),decode_responses=True)

ZONAS = {
    "Z1": {"lat_min": -33.445, "lat_max": -33.420, "lon_min": -70.640, "lon_max": -70.600},
    "Z2": {"lat_min": -33.420, "lat_max": -33.390, "lon_min": -70.600, "lon_max": -70.550},
    "Z3": {"lat_min": -33.530, "lat_max": -33.490, "lon_min": -70.790, "lon_max": -70.740},
    "Z4": {"lat_min": -33.460, "lat_max": -33.430, "lon_min": -70.670, "lon_max": -70.630},
    "Z5": {"lat_min": -33.470, "lat_max": -33.430, "lon_min": -70.810, "lon_max": -70.760},
}

# calcular para la consulta q3
zone_area_km2 = {}
for zona_id, bbox in ZONAS.items():
    lat_km = (bbox["lat_max"] - bbox["lat_min"]) * 111
    lon_km = (bbox["lon_max"] - bbox["lon_min"]) * 111 * abs((bbox["lat_max"] + bbox["lat_min"]) / 2)
    zone_area_km2[zona_id] = lat_km * lon_km

data = {}

picos = namedtuple("picos", ["latitude", "longitude", "area", "confidence"])

def cargar_datos():
    with console.status("[bold yellow]Cargando y filtrando subconjunto de Santiago...", spinner="point"):
        ruta = os.getenv("DATASET_PATH", "../dataset/buildings.csv") #
        df = pd.read_csv(ruta) #

        #Definimos los límites globales basados en la tabla del PDF (Z1 a Z5)
        # Estos valores cubren desde Maipú hasta Las Condes y desde Pudahuel hasta Providencia
        lat_min, lat_max = -33.530, -33.390 # [cite: 95]
        lon_min, lon_max = -70.810, -70.550 # [cite: 95]

        # 3. Creamos un subconjunto que solo contenga los edificios de Santiago
        # Esto reduce el dataset de millones a solo los necesarios para la tarea
        df_santiago = df[
            (df["latitude"]  >= lat_min) & (df["latitude"]  <= lat_max) &
            (df["longitude"] >= lon_min) & (df["longitude"] <= lon_max)
        ].copy() #

        datos_por_zona = {}
        for zona_id, bbox in ZONAS.items(): #
            # 4. Ahora filtramos sobre el subconjunto pequeño, lo cual es mucho más rápido
            filtrado = df_santiago[
                (df_santiago["latitude"]  >= bbox["lat_min"]) & (df_santiago["latitude"]  <= bbox["lat_max"]) &
                (df_santiago["longitude"] >= bbox["lon_min"]) & (df_santiago["longitude"] <= bbox["lon_max"])
            ] #
            
            datos_por_zona[zona_id] = [
                picos(row.latitude, row.longitude, row.area_in_meters, row.confidence)
                for row in filtrado.itertuples()
            ] #
            
    # Al finalizar, informamos cuántos edificios quedaron en las zonas de estudio
    console.print(f"[bold green] Subconjunto cargado: {len(df_santiago)} edificios en las 5 zonas.[/bold green]")
    return datos_por_zona #

#consultas 

def q1_count(zone_id, confidence_min=0.0):
    records = data[zone_id]
    return sum(1 for r in records if r.confidence >= confidence_min)

def q2_area(zone_id, confidence_min=0.0):
    areas = [r.area for r in data[zone_id] if r.confidence >= confidence_min]
    return {"avg_area": mean(areas), "total_area": sum(areas), "n": len(areas)}

def q3_density(zone_id, confidence_min=0.0):
    count = q1_count(zone_id, confidence_min)
    area_km2 = zone_area_km2[zone_id]
    return count / area_km2

def q4_compare(zone_a, zone_b, confidence_min=0.0):
    da = q3_density(zone_a, confidence_min)
    db = q3_density(zone_b, confidence_min)
    return {"zone_a": da, "zone_b": db, "winner": zone_a if da > db else zone_b}

def q5_confidence_dist(zone_id, bins=5):
    scores = [r.confidence for r in data[zone_id]]
    counts, edges = np.histogram(scores, bins=bins, range=(0, 1))
    return [
        {"bucket": i, "min": float(edges[i]), "max": float(edges[i+1]), "count": int(counts[i])}
        for i in range(bins)
    ]

def procesar(consulta):
    tipo           = consulta["tipo"]
    zona           = consulta["zona"]
    zona_b         = consulta.get("zona_b")
    confidence_min = consulta.get("confidence_min", 0.0)
    bins = consulta.get("bins", 5)
    if bins is None:
        bins = 5
    bins = int(bins)
    modo = consulta.get("modo", "uniforme")
    cache_key      = consulta["cache_key"]

    t0 = time.perf_counter()

    if tipo == "Q1":
        resultado = q1_count(zona, confidence_min)
    elif tipo == "Q2":
        resultado = q2_area(zona, confidence_min)
    elif tipo == "Q3":
        resultado = q3_density(zona, confidence_min)
    elif tipo == "Q4":
        resultado = q4_compare(zona, zona_b, confidence_min)
    elif tipo == "Q5":
        resultado = q5_confidence_dist(zona, bins)

    r.rpush(f"{modo}:latencies", (time.perf_counter() - t0) * 1000)
    r.rpush(f"{modo}:timestamps", time.time())

    padding = "x"*15360        # 15 KB 
    payload = {
        "resultado": resultado,
        "padding": padding
    }

    r.set(cache_key, json.dumps(payload), ex=3600)  # TTL de 1 hora para evitar que la cache se llene con datos antiguos
    print(f"Calculado y guardado | {cache_key}")

def esperar_redis():
    while True:
        try:
            if r.ping():
                print("Conectado a Redis")
                break
        except redis.ConnectionError:
            print("Esperando a Redis...")
            time.sleep(2)

def main():
    esperar_redis()

    global data
    data = cargar_datos()

    r.set("status:engine_ready","1") # Indicamos al tráfico que el engine está listo para recibir consultas
    console.print(Panel("[bold cyan]MOTOR DE RESPUESTAS ONLINE[/bold cyan]\n[dim]Escuchando cola:consultas...[/dim]", border_style="cyan"))
    while True:
        try:
            resultado = r.blpop("cola:consultas", timeout=30)
            if resultado is None:
                continue

            mensaje_crudo = resultado[1]
            consulta = json.loads(mensaje_crudo)
            procesar(consulta)
        except Exception as e:
            print(f"Error al procesar: {e}")

if __name__ == "__main__":
    main()
