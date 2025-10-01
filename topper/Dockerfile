FROM python:3.9-slim
WORKDIR /app
COPY topper/topper.py .
COPY topper/config.ini .
COPY middleware/ /app/middleware/
COPY shared/ /app/shared/
RUN pip install pika
CMD ["python", "topper.py"]
