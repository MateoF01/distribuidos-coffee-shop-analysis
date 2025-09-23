FROM python:3.11-slim
WORKDIR /app
COPY cleaner/cleaner.py .
COPY middleware ./middleware
RUN pip install pika
CMD ["python", "cleaner.py"]