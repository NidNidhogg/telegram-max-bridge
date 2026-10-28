# syntax=docker/dockerfile:1
ARG PYTHON_IMAGE=python:3.12.16-slim-bookworm@sha256:62ed8e07e544e8555c5d93748d5275015136b7f5c52b5e8679b4de339c3779d3
ARG UV_IMAGE=ghcr.io/astral-sh/uv:0.9.5@sha256:f459f6f73a8c4ef5d69f4e6fbbdb8af751d6fa40ec34b39a1ab469acd6e289b7

FROM ${UV_IMAGE} AS uv

FROM ${PYTHON_IMAGE} AS deps
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app/.venv
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project --no-cache

FROM ${PYTHON_IMAGE} AS runtime
RUN groupadd --system --gid 10001 bridge \
    && useradd --system --uid 10001 --gid bridge --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin bridge \
    && install -d -o bridge -g bridge -m 0700 /state
COPY --from=deps /app/.venv /app/.venv
COPY 02-messenger-sync/poc /app/02-messenger-sync/poc
ENV PATH=/app/.venv/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    BRIDGE_STATE_DIR=/state
WORKDIR /app/02-messenger-sync/poc
USER 10001:10001
VOLUME ["/state"]
ENTRYPOINT ["python", "/app/02-messenger-sync/poc/bridge.py"]
CMD ["run"]
