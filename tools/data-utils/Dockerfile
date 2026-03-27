FROM python:3.12-slim

# Install uv for fast dependency resolution
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy project files
COPY pyproject.toml uv.lock ./
COPY src/ src/

# Sync dependencies into a venv (uv manages the venv automatically)
RUN uv sync --frozen --no-dev

# All commands run through uv so the venv is activated
ENTRYPOINT ["uv", "run"]
