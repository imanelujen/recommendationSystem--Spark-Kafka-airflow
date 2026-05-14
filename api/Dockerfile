FROM python:3.11-slim

WORKDIR /app

# Java est requis pour PySpark, ainsi que procps (qui fournit ps)
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    procps \
    curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV PYSPARK_PYTHON=python3

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]