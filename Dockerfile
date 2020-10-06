FROM python:3.8-slim

WORKDIR /app

COPY setup.py .
COPY fast_elasticsearch_reindex fast_elasticsearch_reindex
RUN pip install .

ENTRYPOINT ["python", "-m", "fast_elasticsearch_reindex"]
