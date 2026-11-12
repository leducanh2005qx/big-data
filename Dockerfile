FROM apache/spark:4.2.0

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.9-src.zip

USER root

WORKDIR /app
COPY requirements-docker.txt ./
RUN python3 -m pip install --no-cache-dir -r requirements-docker.txt
COPY producer ./producer
COPY spark_jobs ./spark_jobs
COPY scripts ./scripts
COPY schemas ./schemas
COPY data ./data
RUN mkdir -p /app/.spark_checkpoints /app/.ivy2 /app/data/quarantine \
    && chown -R spark:spark /app

USER spark
CMD ["python3", "spark_jobs/stream_processor.py"]
