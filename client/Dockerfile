FROM python:3.9-slim
WORKDIR /app

# Copiar código del cliente
COPY client/ /app/
# Copiar shared (está al mismo nivel que client/)
COPY shared/ /app/shared/

CMD ["python", "main.py"]
