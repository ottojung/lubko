FROM debian:13-slim AS runtime-tools

RUN apt-get update \
    && apt-get install -y --no-install-recommends uidmap \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /uidmap-rootfs/usr/local/bin \
    && cp /usr/bin/newuidmap /usr/bin/newgidmap /uidmap-rootfs/usr/local/bin/ \
    && chmod 4755 /uidmap-rootfs/usr/local/bin/newuidmap /uidmap-rootfs/usr/local/bin/newgidmap \
    && for binary in /usr/bin/newuidmap /usr/bin/newgidmap; do \
         ldd "$binary" | awk '/=> \/.*\(/ { print $3 } /^\// { print $1 }'; \
       done | sort -u | while read -r library; do \
         case "$library" in \
           /lib/*) destination="/usr$library" ;; \
           /lib64/*) destination="/usr$library" ;; \
           *) destination="$library" ;; \
         esac; \
         mkdir -p "/uidmap-rootfs$(dirname "$destination")"; \
         cp -L "$library" "/uidmap-rootfs$destination"; \
       done

FROM ghcr.io/astral-sh/uv:0.10.12 AS uv

FROM gcr.io/distroless/cc-debian13

COPY --from=runtime-tools /bin/dash /bin/sh
COPY --from=runtime-tools /uidmap-rootfs/usr/ /usr/
COPY --from=uv /uv /usr/local/bin/uv

ENV USER="lubko"
ENV HOME="/home/lubko"

WORKDIR /workspace
USER 65532:65532
