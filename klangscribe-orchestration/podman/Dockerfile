FROM python:3.11-slim

# Install uv
RUN pip install uv

# Install apt dependencies
RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*

# Copy dependency files from project root
COPY pyproject.toml uv.lock ./

# Install dependencies using uv
RUN uv sync --frozen --no-dev

COPY . .
CMD ["uv", "run", "dg", "dev"]