# Dockerfile
# Full image wtih application code and dependencies

# Stage 1: Install Dependencies
FROM python:3.11-slim AS builder

WORKDIR /opt/dagster/app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy dependency files from project root
COPY pyproject.toml uv.lock ./

# Install dependencies using uv
RUN uv sync --frozen --no-dev


# Stage 2: Runtime image
FROM python:3.11-slim

WORKDIR /opt/dagster/app

# Copy virtual environment from builder
COPY --from=builder /opt/dagster/app/.venv /opt/dagster/app/.venv

# Copy source code from project root
COPY . .

# Expose the container port (we use 4000)
EXPOSE 4000

# Set environment
ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV PATH="/opt/dagster/app/.venv/bin:$PATH"
ENV PYTHONPATH="/opt/dagster/app/src:$PYTHONPATH"

# Health Check
HEALTHCHECK --timeout=30s --start-period=3s --interval=3s --retries=20 CMD ["dagster", "api", "grpc-health-check", "-p", "4000"]

# Start the gRPC code location server
# CMD allows this to be overridden from run launchers or executors to execute runs and steps
CMD ["dagster", "code-server", "start", "-h", "0.0.0.0", "-p", "4000", "-f", "src/klangscribe_orchestration/definitions.py"]