from kafka import KafkaConsumer, KafkaProducer
import json
import redis
import time
import os

# Conexiones
r = redis.Redis(host=os.getenv('REDIS_HOST', 'cache'), port=6379, decode_responses=True)

consumer = KafkaConsumer(
    'consultas-principales',
    bootstrap_servers=os.getenv('KAFKA_HOST', 'kafka:9092'),
    group_id='mi-grupo',
    value_deserializer=lambda m: json.loads(m.decode())
)

producer = KafkaProducer(
    bootstrap_servers=os.getenv('KAFKA_HOST', 'kafka:9092')
)

MAX_REINTENTOS = 3

def manejar_fallo(mensaje):
    mensaje["retry_count"] += 1

    if mensaje["retry_count"] >= MAX_REINTENTOS:
        producer.send(
            'dlq',
            key=mensaje["id"].encode(),
            value=json.dumps(mensaje).encode()
        )
        print(f" DLQ: {mensaje['cache_key']}")
    else:
        producer.send(
            'consultas-reintento',
            key=mensaje["id"].encode(),
            value=json.dumps(mensaje).encode()
        )
        print(f" Reintento {mensaje['retry_count']}: {mensaje['cache_key']}")

print(" Consumer iniciado, esperando mensajes...")

for msg in consumer:
    mensaje = msg.value
    key  = mensaje["cache_key"]
    modo = mensaje["modo"]

    t0 = time.perf_counter()

    # 1. Revisar caché
    respuesta = r.get(key)
    latencia  = (time.perf_counter() - t0) * 1000

    if respuesta:
        # Cache HIT
        r.incr(f"{modo}:hits")
        r.rpush(f"{modo}:latencies", latencia)
        r.rpush(f"{modo}:timestamps", time.time())
        print(f" ✓ HIT {key} ({latencia:.2f}ms)")

    else:
        # Cache MISS — empujar a cola Redis para que el engine procese
        r.lpush("cola:consultas", json.dumps(mensaje))
        print(f" ✗ MISS {key} → esperando engine...")

        # Esperar hasta que el engine guarde la respuesta en caché
        intentos = 0
        while intentos < 10:
            respuesta = r.get(key)
            if respuesta:
                break
            time.sleep(0.1)  # espera 100ms entre intentos
            intentos += 1

        if respuesta:
            # Engine procesó exitosamente
            latencia = (time.perf_counter() - t0) * 1000
            r.incr(f"{modo}:misses")
            r.rpush(f"{modo}:latencies", latencia)
            r.rpush(f"{modo}:timestamps", time.time())
            print(f" ✓ PROCESADO {key} ({latencia:.2f}ms)")
        else:
            # Engine no respondió → fallo
            print(f"Timeout esperando engine para {key}")
            manejar_fallo(mensaje)