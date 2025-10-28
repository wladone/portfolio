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

# Copy backend code
COPY backend/ ./backend/

# Copy alembic config and scripts
COPY infra/alembic.ini ./infra/alembic.ini
COPY infra/alembic/ ./infra/alembic/
COPY infra/scripts/ ./infra/scripts/

# Make scripts executable
RUN chmod +x ./infra/scripts/entrypoint_api.sh ./infra/scripts/wait_for.sh

USER app

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

ENTRYPOINT ["/app/infra/scripts/entrypoint_api.sh"]
