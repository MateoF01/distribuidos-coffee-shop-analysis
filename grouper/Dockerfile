FROM python:3.9-slim
WORKDIR /app
COPY grouper/grouper.py .
COPY grouper/config.ini .
COPY middleware/ /app/middleware/
COPY shared/ /app/shared/
RUN pip install pika
CMD ["python", "grouper.py"]
