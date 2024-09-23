FROM lapig/app_gdalpoetry:base

RUN mkdir /app /logs

WORKDIR /app

COPY requirements.txt /tmp/requirements.txt

RUN apt-get update && apt-get install -y \
    coreutils \
    git \
    libpq-dev \
    screen \
    python3-venv && \
    python3 -m venv /opt/venv && /opt/venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt && \
    cd /app && git clone https://github.com/lapig-ufg/polygonize.git && \
    echo "Logger Sys" > /logs/logger.log

ENV PATH="/opt/venv/bin:$PATH"

CMD ["tail", "-f", "/logs/logger.log"]