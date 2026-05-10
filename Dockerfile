FROM apache/spark:4.1.1-python3

USER root
WORKDIR /app

# Install curl (for healthchecks)
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

ENV PYTHONPATH=/app/src
ENV PYSPARK_PYTHON=python3
ENV SPARK_HOME=/opt/spark
# hadoop/config is mounted at runtime via docker-compose volume
ENV HADOOP_CONF_DIR=/opt/hadoop/conf

EXPOSE 8000

HEALTHCHECK --interval=20s --timeout=10s --retries=10 --start-period=120s \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["python3", "src/app.py"]
