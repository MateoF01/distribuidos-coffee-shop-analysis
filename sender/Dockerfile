FROM python:3.9-slim
WORKDIR /app
COPY sender/sender.py .
COPY sender/config.ini .
COPY middleware/ /app/middleware/
COPY shared/ /app/shared/
RUN pip install pika
CMD ["python", "sender.py"]
