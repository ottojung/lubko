FROM ghcr.io/astral-sh/uv:0.10.12 AS uv

FROM gcr.io/distroless/cc-debian13

COPY --from=uv /uv /usr/local/bin/uv

ENV USER="lubko"
ENV HOME="/home/lubko"

WORKDIR /workspace
USER ${USER}
