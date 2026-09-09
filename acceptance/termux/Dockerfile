FROM termux/termux-docker:aarch64

RUN apt update && \
    apt install -y python uv git libpq libtermux-exec \
        clang make cmake && \
    rm -rf /data/data/com.termux/cache/apt/*

ENV PATH="/data/data/com.termux/files/usr/bin:${PATH}"
ENV LD_PRELOAD="/data/data/com.termux/files/usr/lib/libtermux-exec.so"

WORKDIR /workspace
COPY --chown=1000:1000 . /workspace

CMD ["/data/data/com.termux/files/usr/bin/bash", "/workspace/acceptance/termux/acceptance.sh"]
