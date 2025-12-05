FROM python:3.9-slim

WORKDIR /app

COPY coordinator/coordinator.py .
COPY coordinator/config.ini .
COPY coordinator/wsm_config.py .
COPY middleware/ /app/middleware/
COPY shared/ /app/shared/
COPY WSM/ /app/WSM/

RUN pip install pika

CMD ["python", "coordinator.py"]