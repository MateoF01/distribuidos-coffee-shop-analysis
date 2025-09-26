FROM python:3.9-slim
WORKDIR /app
COPY joiner/joiner.py .
COPY joiner/config.ini .
COPY middleware/ /app/middleware/
RUN pip install pika
CMD ["python", "joiner.py"]
