# Builder stage
FROM python:3.11-slim as builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN pip install poetry==1.7.1

# Set Poetry config
RUN poetry config virtualenvs.in-project true

# Copy project files
WORKDIR /app
COPY pyproject.toml poetry.lock ./
COPY backend/app/core/db.py backend/app/core/
COPY backend/app/models/ backend/app/models/
COPY backend/app/utils/ backend/app/utils/
COPY etl/ etl/
COPY streaming/ streaming/

# Install dependencies
RUN poetry install --only main

# Runner stage
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    OPENBLAS_NUM_THREADS=1 \
    OMP_NUM_THREADS=1

# Create app user
RUN useradd -m -s /bin/bash app

# Copy virtual environment from builder
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/backend/app/core/db.py /app/backend/app/core/
COPY --from=builder /app/backend/app/models/ /app/backend/app/models/
COPY --from=builder /app/backend/app/utils/ /app/backend/app/utils/
COPY --from=builder /app/etl/ /app/etl/
COPY --from=builder /app/streaming/ /app/streaming/

# Copy wait script
COPY infra/scripts/wait_for.sh /app/wait_for.sh
RUN chmod +x /app/wait_for.sh

# Set working directory and user
WORKDIR /app
USER app

# Add virtual environment to path
ENV PATH="/app/.venv/bin:$PATH"

# Set entrypoint
ENTRYPOINT ["python", "-m", "streaming.orders_worker"]
