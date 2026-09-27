FROM alpine:3.22 AS s6

ARG S6_OVERLAY_VERSION=3.2.3.2
ARG TARGETARCH=amd64

RUN apk add --no-cache ca-certificates curl xz
RUN set -eux; \
    case "${TARGETARCH}" in \
        amd64) s6_arch=x86_64 ;; \
        arm64) s6_arch=aarch64 ;; \
        *) echo "unsupported Docker architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    mkdir -p /rootfs; \
    curl -fsSL -o /tmp/s6-overlay-noarch.tar.xz \
        "https://github.com/just-containers/s6-overlay/releases/download/v${S6_OVERLAY_VERSION}/s6-overlay-noarch.tar.xz"; \
    curl -fsSL -o /tmp/s6-overlay-noarch.tar.xz.sha256 \
        "https://github.com/just-containers/s6-overlay/releases/download/v${S6_OVERLAY_VERSION}/s6-overlay-noarch.tar.xz.sha256"; \
    curl -fsSL -o "/tmp/s6-overlay-${s6_arch}.tar.xz" \
        "https://github.com/just-containers/s6-overlay/releases/download/v${S6_OVERLAY_VERSION}/s6-overlay-${s6_arch}.tar.xz"; \
    curl -fsSL -o "/tmp/s6-overlay-${s6_arch}.tar.xz.sha256" \
        "https://github.com/just-containers/s6-overlay/releases/download/v${S6_OVERLAY_VERSION}/s6-overlay-${s6_arch}.tar.xz.sha256"; \
    cd /tmp; \
    sha256sum -c s6-overlay-noarch.tar.xz.sha256; \
    sha256sum -c "s6-overlay-${s6_arch}.tar.xz.sha256"; \
    tar -C /rootfs -Jxpf /tmp/s6-overlay-noarch.tar.xz; \
    tar -C /rootfs -Jxpf "/tmp/s6-overlay-${s6_arch}.tar.xz"

FROM debian:13-slim AS shell

FROM ghcr.io/astral-sh/uv:0.10.12 AS uv

FROM gcr.io/distroless/cc-debian13

COPY --from=s6 /rootfs /
COPY --from=shell /bin/dash /bin/sh
COPY --from=uv /uv /usr/local/bin/uv

ENV USER="lubko"
ENV HOME="/home/lubko"
ENV S6_KEEP_ENV="1"

WORKDIR /workspace
USER 65532:65532

ENTRYPOINT ["/init"]
