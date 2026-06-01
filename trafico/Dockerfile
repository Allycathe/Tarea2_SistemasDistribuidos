FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir redis numpy rich

COPY . .

CMD ["python","main.py"]

ENV PYTHONUNBUFFERED=1
