FROM python:3.9-slim
WORKDIR /app
COPY reducer/reducer.py .
COPY reducer/wsm_config.py .
COPY reducer/config.ini .
COPY middleware/ /app/middleware/
COPY shared/ /app/shared/
COPY WSM/ /app/WSM/

RUN pip install pika
CMD ["python", "reducer.py"]
