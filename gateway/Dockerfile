FROM python:3.11-slim
WORKDIR /app
COPY gateway.py .
RUN pip install pika
CMD ["python", "gateway.py"]