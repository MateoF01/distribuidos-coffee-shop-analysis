FROM python:3.9-slim
WORKDIR /app
COPY reducer/reducer.py .
COPY reducer/config.ini .
COPY middleware/ /app/middleware/
COPY shared/ /app/shared/
RUN pip install pika
CMD ["python", "reducer.py"]
