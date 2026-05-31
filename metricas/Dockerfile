FROM python:3.11-slim

WORKDIR /app

# Solo necesitamos redis para escuchar los eventos y registrarlos
RUN pip install --no-cache-dir redis rich kafka-python

COPY . .

# Comando para iniciar el registro de métricas
CMD ["python", "metricas.py"]

ENV PYTHONUNBUFFERED=1
