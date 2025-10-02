FROM python:3.9-slim
WORKDIR /app
COPY sort/sort.py .
COPY sort/config.ini .
COPY middleware/ /app/middleware/
COPY shared/ /app/shared/
RUN pip install pika
CMD ["python", "sort.py"]
