FROM mcr.microsoft.com/devcontainers/python:3.12-bookworm

ARG RPK_VERSION=v24.3.5

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        postgresql-client \
        unzip \
        curl \
        ca-certificates \
        librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

RUN ARCH=$(dpkg --print-architecture) \
    && case "$ARCH" in \
        amd64) RPK_ARCH=amd64 ;; \
        arm64) RPK_ARCH=arm64 ;; \
        *) echo "unsupported arch $ARCH" && exit 1 ;; \
       esac \
    && curl -L -o /tmp/rpk.zip "https://github.com/redpanda-data/redpanda/releases/download/${RPK_VERSION}/rpk-linux-${RPK_ARCH}.zip" \
    && unzip /tmp/rpk.zip -d /usr/local/bin \
    && chmod +x /usr/local/bin/rpk \
    && rm /tmp/rpk.zip

RUN curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin sh

ENV KAFKA_BROKERS=redpanda:29092 \
    RISINGWAVE_HOST=risingwave \
    RISINGWAVE_PORT=4566 \
    RISINGWAVE_USER=root \
    RISINGWAVE_DB=dev
