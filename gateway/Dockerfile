FROM python:3.9-slim
WORKDIR /app
COPY gateway/gateway.py .
COPY gateway/wsm_config.py .
COPY middleware/ /app/middleware/
COPY shared/ /app/shared/
COPY WSM/ /app/WSM/
RUN pip install pika
CMD ["python", "gateway.py"]