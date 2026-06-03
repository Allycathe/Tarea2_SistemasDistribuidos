FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir numpy redis rich kafka-python

COPY . .

CMD ["python", "-u", "main.py"]

ENV PYTHONUNBUFFERED=1