FROM python:3.9-slim

WORKDIR /app

# Copiamos los módulos del WSM y el protocolo compartido
COPY WSM /app/WSM
COPY shared /app/shared

# (Opcional) Instalar logging o debug tools si lo necesitás
# RUN pip install --no-cache-dir some-library

# Configuramos el PYTHONPATH para permitir import desde la raíz
ENV PYTHONPATH=/app

# Puerto TCP del WSM
EXPOSE 9000

# Ejecuta el servidor
CMD ["python", "-u", "WSM/wsm_server.py"]
