FROM python:3.9-slim

WORKDIR /app

COPY cleaner/cleaner.py .
COPY cleaner/wsm_config.py .
COPY cleaner/config.ini .
COPY middleware/ /app/middleware/
COPY shared/ /app/shared/
COPY WSM/ /app/WSM/

RUN pip install --no-cache-dir pika

ENV PYTHONPATH=/app

CMD ["python", "-u", "cleaner.py"]
