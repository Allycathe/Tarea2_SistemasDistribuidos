FROM python:3.11-slim

WORKDIR /app
COPY . .

RUN pip install kafka-python redis

CMD ["python", "consumer.py"]