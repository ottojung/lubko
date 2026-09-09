FROM termux/termux-docker:aarch64

WORKDIR /workspace
COPY --chown=system:system . /workspace

CMD ["sh", "/workspace/acceptance/termux/acceptance.sh"]
