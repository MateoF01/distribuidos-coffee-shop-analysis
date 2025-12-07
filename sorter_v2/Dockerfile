FROM python:3.9-slim
WORKDIR /app

COPY sorter_v2/sorter_v2.py .
COPY sorter_v2/wsm_config.py .
COPY middleware/ /app/middleware/
COPY shared/ /app/shared/
COPY WSM/ /app/WSM/

RUN mkdir -p /app/temp && pip install pika

CMD ["python", "sorter_v2.py"]
