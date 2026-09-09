FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates curl git python3 python3-venv && \
    rm -rf /var/lib/apt/lists/*

ARG UV_VERSION=0.10.12
RUN curl -LsSf "https://astral.sh/uv/${UV_VERSION}/uv-x86_64-unknown-linux-gnu.tar.gz" | \
    tar -xzf - -C /usr/local/bin

COPY . /src/lubko
WORKDIR /src/lubko

RUN mkdir -p /root/.config/lubko && \
    printf 'host=127.0.0.1\nport=5432\ndbname=lubko\nuser=lubko\npassword=lubko\n' \
        > /root/.config/lubko/database.conf && \
    chmod 600 /root/.config/lubko/database.conf && \
    printf 'server=acceptance-test\n' \
        > /root/.config/lubko/worker.conf && \
    chmod 600 /root/.config/lubko/worker.conf

RUN lubko-install --repo /src/lubko

COPY acceptance/ubuntu/acceptance.sh /usr/local/bin/acceptance.sh
RUN chmod +x /usr/local/bin/acceptance.sh
