# Builder stage
FROM python:3.11-slim as builder

RUN apt-get update && apt-get install -y \
    build-essential \
    gfortran \
    libopenblas-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir poetry

WORKDIR /app

COPY ../pyproject.toml ../poetry.lock ./

RUN poetry config virtualenvs.create false \
    && poetry install --only=main --no-interaction --no-ansi --no-root

# Runner stage
FROM python:3.11-slim as runner

RUN apt-get update && apt-get install -y \
    libgomp1 \
    libopenblas0 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local /usr/local

COPY ml/ /app/ml/

WORKDIR /app

ENV OPENBLAS_NUM_THREADS=1
ENV OMP_NUM_THREADS=1

ENTRYPOINT ["python", "/app/ml/als_train.py"]
