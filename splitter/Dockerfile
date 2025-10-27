FROM python:3.9-slim

WORKDIR /app

# Copiamos el código fuente
COPY splitter/splitter.py .
COPY middleware/ /app/middleware/
COPY shared/ /app/shared/
COPY WSM/ /app/WSM/

# Creamos la carpeta temp dentro del contenedor
RUN mkdir -p /app/temp

# Instalamos dependencias mínimas
RUN pip install pika

CMD ["python", "splitter.py"]
