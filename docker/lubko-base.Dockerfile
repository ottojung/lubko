FROM debian:13-slim AS shell

FROM ghcr.io/astral-sh/uv:0.10.12 AS uv

FROM gcr.io/distroless/cc-debian13

COPY --from=shell /bin/dash /bin/sh
COPY --from=uv /uv /usr/local/bin/uv

ENV USER="lubko"
ENV HOME="/home/lubko"

WORKDIR /workspace
USER 65532:65532
