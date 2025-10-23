FROM python:3.9-slim
WORKDIR /app
COPY joiner_v2/joiner_v2.py .
COPY joiner_v2/config.ini .
COPY middleware/ /app/middleware/
COPY shared/ /app/shared/
RUN pip install pika
CMD ["python", "joiner_v2.py"]
