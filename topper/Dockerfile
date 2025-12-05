FROM python:3.9-slim
WORKDIR /app
COPY topper/topper.py .
COPY topper/config.ini .
COPY topper/wsm_config.py .
COPY middleware/ /app/middleware/
COPY shared/ /app/shared/
COPY WSM/ /app/WSM/
RUN pip install pika
CMD ["python", "topper.py"]
