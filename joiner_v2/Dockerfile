FROM python:3.9-slim
WORKDIR /app
COPY joiner_v2/joiner_v2.py .
COPY joiner_v2/config.ini .
COPY joiner_v2/wsm_config.py .
COPY middleware/ /app/middleware/
COPY shared/ /app/shared/
COPY SHM/ /app/SHM/
COPY WSM/ /app/WSM/
RUN pip install pika
CMD ["python", "joiner_v2.py"]
