FROM mcr.microsoft.com/devcontainers/python:3.12-bookworm

ARG RPK_VERSION=v24.3.5

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        postgresql-client \
        unzip \
        curl \
        ca-certificates \
        gnupg \
        librdkafka-dev \
    && install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg \
        | gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
    && chmod a+r /etc/apt/keyrings/docker.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian bookworm stable" \
        > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends docker-ce-cli docker-compose-plugin \
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
