FROM python:3.9-slim

WORKDIR /app

COPY coordinator/coordinator.py .
COPY coordinator/config.ini .
COPY middleware/ /app/middleware/
COPY shared/ /app/shared/

RUN pip install pika

CMD ["python", "coordinator.py"]