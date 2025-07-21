# --- Builder Stage
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV UV_PYTHON_DOWNLOADS=0

# Install the project's dependencies using the lockfile and settings
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev

# Copy the pyproject.toml and lockfile to install dependencies
COPY ./pyproject.toml /app
COPY ./uv.lock /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

# --- Final Stage
FROM python:3.13-slim-bookworm AS final

# Set working directory for the app
WORKDIR /app

# Copy installed venv from build stage
COPY --from=builder /app /app
# and the app code directly
COPY /app /app

# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"

# Use relative CMD, clean and readable
CMD ["streamlit", "run", "app.py"]
