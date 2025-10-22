FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        openjdk-17-jdk-headless \
        python3-dev \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN chmod +x paradocker/*.sh || true \
    && mkdir -p paradocker/lib

ENV INFORMIX_JDBC_CLASSPATH=/app/paradocker/lib/ifxjdbc.jar

ENTRYPOINT ["/app/paradocker/entrypoint.sh"]
CMD ["server"]
