from kafka import KafkaConsumer, KafkaProducer
import json
import redis
import time
import os

# Conexiones
r = redis.Redis(host=os.getenv('REDIS_HOST', 'cache'), port=6379, decode_responses=True)

consumer = KafkaConsumer(
    'consultas-reintento',           # ← tópico diferente
    bootstrap_servers=os.getenv('KAFKA_HOST', 'kafka:9092'),
    group_id='mi-grupo-retry',       # ← grupo diferente
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
        print(f" DLQ (agotados reintentos): {mensaje['cache_key']}")
    else:
        # Vuelve al mismo tópico de reintento
        producer.send(
            'consultas-reintento',
            key=mensaje["id"].encode(),
            value=json.dumps(mensaje).encode()
        )
        print(f" Reintento {mensaje['retry_count']}/{MAX_REINTENTOS}: {mensaje['cache_key']}")

print(" Consumer de reintento iniciado, esperando mensajes...")

for msg in consumer:
    mensaje = msg.value
    key  = mensaje["cache_key"]
    modo = mensaje["modo"]

    print(f" Procesando reintento {mensaje['retry_count']}/{MAX_REINTENTOS}: {key}")

    t0 = time.perf_counter()

    # 1. Revisar caché primero (puede que ya esté desde un intento anterior)
    respuesta = r.get(key)

    if respuesta:
        # Cache HIT — alguien más ya lo procesó
        latencia = (time.perf_counter() - t0) * 1000
        r.incr(f"{modo}:hits")
        r.rpush(f"{modo}:latencies", latencia)
        r.rpush(f"{modo}:timestamps", time.time())
        print(f" HIT en reintento {key} ({latencia:.2f}ms)")

    else:
        # Cache MISS — intentar de nuevo con el engine
        r.lpush("cola:consultas", json.dumps(mensaje))
        print(f" MISS reintento {key} → esperando engine...")

        intentos = 0
        while intentos < 10:
            respuesta = r.get(key)
            if respuesta:
                break
            time.sleep(0.1)
            intentos += 1

        if respuesta:
            latencia = (time.perf_counter() - t0) * 1000
            r.incr(f"{modo}:misses")
            r.rpush(f"{modo}:latencies", latencia)
            r.rpush(f"{modo}:timestamps", time.time())
            # Métrica de recuperación exitosa
            r.incr(f"{modo}:recovered")
            print(f" RECUPERADO en reintento {key} ({latencia:.2f}ms)")
        else:
            print(f" Timeout en reintento para {key}")
            manejar_fallo(mensaje)