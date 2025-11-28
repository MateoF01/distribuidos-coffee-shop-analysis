FROM python:3.9-slim
WORKDIR /app
COPY grouper_v2/grouper_v2.py .
COPY grouper_v2/config.ini .
COPY grouper_v2/wsm_config.py .
COPY middleware/ /app/middleware/
COPY shared/ /app/shared/
COPY WSM/ /app/WSM/
RUN pip install pika
CMD ["python", "grouper_v2.py"]
