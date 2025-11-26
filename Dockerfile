FROM python:3.11-slim

# 'build-essential' contains GCC, Make, etc.
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . .

RUN pip install .

CMD ["python", "-m", "cluster.node"]