FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:$PATH"

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir uv==0.12.9

WORKDIR /app
COPY . .
RUN uv sync --frozen --no-dev --python /usr/local/bin/python \
    && useradd --system --uid 10001 tracker \
    && chown -R tracker:tracker /app

USER tracker
EXPOSE 8000
CMD ["uvicorn", "cinema_tracker.app:create_application", "--factory", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]

