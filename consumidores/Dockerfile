FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir redis rich kafka-python

# Esto copia los 3 scripts (.py) de una sola vez
COPY . .

# Por defecto arranca el consumidor principal
CMD ["python", "-u", "principal.py"]

ENV PYTHONUNBUFFERED=1