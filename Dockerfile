FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive \
    JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

RUN apt-get update && apt-get install -y \
    openjdk-21-jre-headless \
    procps \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY etl_accidents/ ./etl_accidents/
COPY tests/ ./tests/
COPY pytest.ini .

RUN mkdir -p /app/input /app/output

ENTRYPOINT ["python", "etl_accidents/etl.py"]
CMD ["--input-dir", "/app/input", "--output-dir", "/app/output"]
