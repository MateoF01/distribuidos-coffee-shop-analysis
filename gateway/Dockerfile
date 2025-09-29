FROM python:3.9-slim
WORKDIR /app
COPY gateway/gateway.py .
COPY middleware/ /app/middleware/
COPY shared/ /app/shared/
RUN pip install pika
CMD ["python", "gateway.py"]