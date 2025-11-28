FROM python:3.9-slim
WORKDIR /app
COPY filter/filter.py .
COPY filter/config.ini .
COPY filter/temporal_filter_config.ini .
COPY filter/wsm_config.py .
COPY middleware/ /app/middleware/
COPY shared/ /app/shared/
COPY WSM/ /app/WSM/
RUN pip install pika
CMD ["python", "filter.py"]