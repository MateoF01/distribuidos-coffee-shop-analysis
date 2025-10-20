FROM python:3.9-slim

WORKDIR /app

COPY tests/sender/sender_transactions.py .

COPY middleware/ /app/middleware/
COPY shared/ /app/shared/
COPY data/ /app/data/
COPY WSM/ /app/WSM/

RUN pip install --no-cache-dir pika

ENV PYTHONPATH=/app

CMD ["python", "-u", "sender_transactions.py"]
