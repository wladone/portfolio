# Builder stage
FROM python:3.11-slim as builder

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -

ENV PATH="/root/.local/bin:$PATH"

WORKDIR /app

COPY pyproject.toml poetry.lock ./

RUN poetry config virtualenvs.create false && poetry install --only=main --no-interaction --no-ansi --no-root

# Runner stage
FROM python:3.11-slim as runner

RUN groupadd --gid 10001 app && useradd --uid 10001 --gid app --shell /bin/bash --create-home app

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy etl code
COPY etl/ ./etl/

# Copy backend models and core for shared dependencies
COPY backend/app/models/ ./backend/app/models/
COPY backend/app/core/ ./backend/app/core/

# Copy infra scripts
COPY infra/scripts/ ./infra/scripts/

# Make scripts executable
RUN chmod +x ./infra/scripts/entrypoint_etl.sh ./infra/scripts/wait_for.sh

USER app

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["/app/infra/scripts/entrypoint_etl.sh"]
CMD ["all", "--source", "/data"]
