from kafka import KafkaConsumer, KafkaProducer
import json
import redis
import time
import os

# Conexiones
r = redis.Redis(host=os.getenv('REDIS_HOST', 'cache'), port=6379, decode_responses=True)

consumer = KafkaConsumer(
    'consultas-principales',          # ← antes era 'Q1','Q2','Q3','Q4','Q5' (estructura vieja)
    bootstrap_servers=os.getenv('KAFKA_HOST', 'kafka:9092'),
    group_id='grupo-consumidores',    # mismo group_id para balanceo automático entre réplicas
    value_deserializer=lambda m: json.loads(m.decode())
)

producer = KafkaProducer(
    bootstrap_servers=os.getenv('KAFKA_HOST', 'kafka:9092')
)

MAX_REINTENTOS = 3

def manejar_fallo(mensaje):
    mensaje["retry_count"] += 1
    if mensaje["retry_count"] >= MAX_REINTENTOS:
        # Superó el límite → DLQ
        producer.send(
            'dlq',
            key=mensaje["id"].encode(),
            value=json.dumps(mensaje).encode()
        )
        modo = mensaje.get("modo", "uniforme")
        r.incr(f"{modo}:dlq_count")          # métrica DLQ rate
        print(f"→ DLQ: {mensaje['cache_key']} (intentos={mensaje['retry_count']})")
    else:
        # Todavía tiene intentos se va al tópico de reintento
        producer.send(
            'consultas-reintento',         
            key=mensaje["id"].encode(),
            value=json.dumps(mensaje).encode()
        )
        modo = mensaje.get("modo", "uniforme")
        r.incr(f"{modo}:retry_count")        # métrica retry rate
        print(f"→ Reintento {mensaje['retry_count']}: {mensaje['cache_key']}")


print("Consumer principal iniciado, esperando mensajes en 'consultas-principales'...")

for msg in consumer:
    try:
        mensaje = msg.value
        key  = mensaje["cache_key"]
        modo = mensaje.get("modo", "uniforme")

        t0 = time.perf_counter()

        # 1. Revisar caché
        respuesta = r.get(key)
        latencia  = (time.perf_counter() - t0) * 1000

        if respuesta:
            # Cache HIT, hace respuesta inmediata, sin tocar el engine
            r.incr(f"{modo}:hits")
            r.rpush(f"{modo}:latencies", latencia)
            r.rpush(f"{modo}:timestamps", time.time())
            print(f"[{msg.topic}/P{msg.partition}] ✓ HIT  {key} ({latencia:.2f}ms)")

        else:
            # Cache MISS -> delegar al engine via cola Redis
            r.incr(f"{modo}:misses")
            r.lpush("cola:consultas", json.dumps(mensaje))
            print(f"[{msg.topic}/P{msg.partition}] · MISS {key} → enviado al engine")

            # Esperar hasta que el engine guarde la respuesta en caché
            intentos = 0
            while intentos < 10:
                respuesta = r.get(key)
                if respuesta:
                    break
                time.sleep(0.1)
                intentos += 1

            latencia = (time.perf_counter() - t0) * 1000   # latencia total incluyendo cómputo

            if respuesta:
                r.rpush(f"{modo}:latencies", latencia)
                r.rpush(f"{modo}:timestamps", time.time())
                print(f"[{msg.topic}/P{msg.partition}] ✓ PROCESADO {key} ({latencia:.2f}ms)")
            else:
                # Engine no respondió en tiempo -> fallo temporal
                print(f"[{msg.topic}/P{msg.partition}] ✗ TIMEOUT {key} ({latencia:.2f}ms)")
                manejar_fallo(mensaje)

    except KeyError as e:
        print(f"[{msg.topic}/P{msg.partition}] Error de clave: {e} → {msg.value}")
    except Exception as e:
        print(f"[{msg.topic}/P{msg.partition}] Error inesperado: {e}")
        try:
            manejar_fallo(mensaje)
        except Exception as e2:
            print(f"[{msg.topic}/P{msg.partition}] Error al manejar fallo: {e2}")