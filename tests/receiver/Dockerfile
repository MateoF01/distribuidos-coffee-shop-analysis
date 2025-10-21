# tests/receiver/Dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY tests/receiver/receiver_transactions.py .
COPY middleware/ /app/middleware/
COPY shared/ /app/shared/

RUN pip install --no-cache-dir pika

ENV PYTHONPATH=/app
VOLUME ["/app/output"]

CMD ["python", "-u", "receiver_transactions.py"]
