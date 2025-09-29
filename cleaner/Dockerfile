FROM python:3.9-slim
WORKDIR /app
COPY cleaner/cleaner.py .
COPY cleaner/config.ini .
COPY middleware/ /app/middleware/
COPY shared/ /app/shared/
RUN pip install pika
CMD ["python", "cleaner.py"]