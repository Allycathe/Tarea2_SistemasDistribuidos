from kafka import KafkaConsumer, KafkaProducer
import json
import redis
import time
import os

# Conexiones
r = redis.Redis(host=os.getenv('REDIS_HOST', 'cache'), port=6379, decode_responses=True)

consumer = KafkaConsumer(
    'consultas-reintento',            # tópico exclusivo de reintentos
    bootstrap_servers=os.getenv('KAFKA_HOST', 'kafka:9092'),
    group_id='grupo-consumidores',    # mismo grupo para pertenecer al mismo consumer group
    value_deserializer=lambda m: json.loads(m.decode())
)

producer = KafkaProducer(
    bootstrap_servers=os.getenv('KAFKA_HOST', 'kafka:9092')
)

MAX_REINTENTOS = 3

def manejar_fallo(mensaje):
    mensaje["retry_count"] += 1
    modo = mensaje.get("modo", "uniforme")
    if mensaje["retry_count"] >= MAX_REINTENTOS:
        producer.send(
            'dlq',
            key=mensaje["id"].encode(),
            value=json.dumps(mensaje).encode()
        )
        r.incr(f"{modo}:dlq_count")
        print(f"→ DLQ: {mensaje['cache_key']} (intentos={mensaje['retry_count']})")
    else:
        producer.send(
            'consultas-reintento',
            key=mensaje["id"].encode(),
            value=json.dumps(mensaje).encode()
        )
        r.incr(f"{modo}:retry_count")
        print(f"→ Re-reintento {mensaje['retry_count']}: {mensaje['cache_key']}")


print("Consumer de reintento iniciado, esperando mensajes en 'consultas-reintento'...")

for msg in consumer:
    try:
        mensaje = msg.value
        key  = mensaje["cache_key"]
        modo = mensaje.get("modo", "uniforme")

        t0 = time.perf_counter()

        # En reintento siempre verificamos caché primero (pudo haberse llenado mientras esperaba)
        respuesta = r.get(key)

        if respuesta:
            latencia = (time.perf_counter() - t0) * 1000
            r.incr(f"{modo}:hits")
            r.incr(f"{modo}:recovered_count")   # métrica recovery rate
            r.rpush(f"{modo}:latencies", latencia)
            r.rpush(f"{modo}:timestamps", time.time())
            print(f"[RETRY/{msg.partition}] ✓ HIT (recuperado) {key} ({latencia:.2f}ms)")
        else:
            # Cache miss en reintento → intentar engine de nuevo
            r.lpush("cola:consultas", json.dumps(mensaje))
            print(f"[RETRY/{msg.partition}] · MISS {key} → reenviado al engine")

            intentos = 0
            while intentos < 10:
                respuesta = r.get(key)
                if respuesta:
                    break
                time.sleep(0.1)
                intentos += 1

            latencia = (time.perf_counter() - t0) * 1000

            if respuesta:
                r.incr(f"{modo}:misses")
                r.incr(f"{modo}:recovered_count")   # se recuperó tras fallo
                r.rpush(f"{modo}:latencies", latencia)
                r.rpush(f"{modo}:timestamps", time.time())
                print(f"[RETRY/{msg.partition}] ✓ PROCESADO (recuperado) {key} ({latencia:.2f}ms)")
            else:
                print(f"[RETRY/{msg.partition}] ✗ TIMEOUT {key} → fallo de nuevo")
                manejar_fallo(mensaje)

    except KeyError as e:
        print(f"[RETRY/{msg.partition}] Error de clave: {e}")
    except Exception as e:
        print(f"[RETRY/{msg.partition}] Error inesperado: {e}")
        try:
            manejar_fallo(mensaje)
        except Exception as e2:
            print(f"[RETRY/{msg.partition}] Error al manejar fallo: {e2}")