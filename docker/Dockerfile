FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=Asia/Shanghai

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    tzdata \
    libffi-dev \
    libssl-dev \
    libxml2-dev \
    libxslt1-dev \
    libfreetype6-dev \
    libpng-dev \
    fontconfig \
    fonts-noto-cjk \
    fonts-wqy-zenhei \
    fonts-wqy-microhei \
    pkg-config \
    && fc-cache -f \
    && rm -rf /var/lib/apt/lists/*

COPY docker/requirements-docker.txt /tmp/requirements.txt

RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --upgrade pip \
    && /opt/venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt

ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONPATH=/app
# Matplotlib headless + UTF-8 locale for logs
ENV MPLBACKEND=Agg
ENV MPLCONFIGDIR=/tmp/matplotlib
ENV LANG=C.UTF-8

# Ensure Matplotlib config dir exists and is writable
RUN mkdir -p /tmp/matplotlib && chmod 777 /tmp/matplotlib

COPY . /app

RUN adduser --disabled-password --gecos "" appuser \
    && chown -R appuser:appuser /app \
    && chmod +x docker/docker-entrypoint.sh docker/scripts/init-db.sh docker/scripts/update-cron.sh docker/build.sh docker/run.sh

USER appuser

VOLUME ["/app/data", "/app/reports"]

ENTRYPOINT ["/bin/bash", "/app/docker/docker-entrypoint.sh"]
CMD ["main"]
