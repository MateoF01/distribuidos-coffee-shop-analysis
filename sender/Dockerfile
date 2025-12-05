FROM python:3.9-slim
WORKDIR /app
COPY sender/sender.py .
COPY sender/config.ini .
COPY sender/wsm_config.py .
COPY middleware/ /app/middleware/
COPY shared/ /app/shared/
COPY WSM/ /app/WSM/

RUN pip install pika
CMD ["python", "sender.py"]
