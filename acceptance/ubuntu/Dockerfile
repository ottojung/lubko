FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates git python3 python3-venv && \
    rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.10.12 /uv /uvx /bin/

COPY . /src/lubko
WORKDIR /src/lubko

RUN uv sync --frozen

RUN mkdir -p /root/.config/lubko && \
    printf 'host=127.0.0.1\nport=5432\ndbname=lubko\nuser=lubko\npassword=lubko\n' \
        > /root/.config/lubko/database.conf && \
    chmod 600 /root/.config/lubko/database.conf && \
    printf 'server=acceptance-test\n' \
        > /root/.config/lubko/worker.conf && \
    chmod 600 /root/.config/lubko/worker.conf

RUN uv run lubko-install --repo /src/lubko

RUN uv sync --frozen --extra dev

COPY acceptance/ubuntu/acceptance.sh /usr/local/bin/acceptance.sh
RUN chmod +x /usr/local/bin/acceptance.sh
