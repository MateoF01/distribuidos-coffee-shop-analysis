FROM python:3.9-slim
WORKDIR /app
COPY filter/filter.py .
COPY filter/config.ini .
COPY filter/temporal_filter_config.ini .
COPY middleware/ /app/middleware/
RUN pip install pika
CMD ["python", "filter.py"]