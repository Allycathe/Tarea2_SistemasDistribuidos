FROM python:3.11-slim

WORKDIR /app
COPY . .

RUN pip install --no-cache-dir kafka-python redis

CMD ["python", "consumer.py"]