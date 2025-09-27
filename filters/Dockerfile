FROM python:3.9-slim

WORKDIR /app

# Copiamos código de filtros y middleware
COPY filters/ /app/filters/
COPY middleware/ /app/middleware/

# Instalamos dependencias
RUN pip install pika

# Arrancamos con el entrypoint configurable
CMD ["python", "filters/entrypoint.py"]
