FROM python:3.9-slim

WORKDIR /app

# Optional: add curl/netcat for debugging inside container
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     netcat-openbsd curl && \
#     rm -rf /var/lib/apt/lists/*

# -------------------------------
# 📦 Copy only what's needed
# -------------------------------
# assuming hashmap_server.py is in the same folder as this Dockerfile

COPY SHM /app/SHM
COPY shared /app/shared

# -------------------------------
# ⚙️ Default environment variables
# -------------------------------
# ENV HM_HOST=0.0.0.0
# ENV HM_PORT=9100
# ENV HM_STATE_PATH=/app/output/hashmap_state.json

ENV PYTHONPATH=/app
# create output dir for persistence
# RUN mkdir -p /app/output

# -------------------------------
# 🚀 Start the server
# -------------------------------
EXPOSE 9100
CMD ["python", "hashmap_server.py"]
