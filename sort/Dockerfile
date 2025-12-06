FROM python:3.9-slim
WORKDIR /app
COPY sort/sort.py .
COPY sort/config.ini .
COPY sort/wsm_config.py .
COPY middleware/ /app/middleware/
COPY shared/ /app/shared/
COPY WSM/ /app/WSM/

RUN pip install pika
CMD ["python", "sort.py"]
