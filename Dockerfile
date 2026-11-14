# ---- Build stage ----
FROM python:3.12-alpine AS builder

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apk add --no-cache g++ linux-headers

WORKDIR /build

# Install dependencies from the fully pinned lock (requirements.in is its source; regenerate with uv pip compile)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Generate proto stubs
COPY proto/ ./proto/
RUN mkdir -p generated \
    && python -m grpc_tools.protoc \
        -I proto \
        --python_out=generated \
        --grpc_python_out=generated \
        proto/email.proto proto/mailbox.proto \
    && touch generated/__init__.py \
    && sed -i 's/^import email_pb2 as email__pb2/from . import email_pb2 as email__pb2/' generated/email_pb2_grpc.py \
    && sed -i 's/^import mailbox_pb2 as mailbox__pb2/from . import mailbox_pb2 as mailbox__pb2/' generated/mailbox_pb2_grpc.py

# ---- Runtime stage ----
FROM python:3.12-alpine AS runtime

ARG SERVICE_VERSION=dev
ARG SERVICE_COMMIT=unknown

LABEL org.opencontainers.image.title="email-service" \
      org.opencontainers.image.source="https://github.com/platform-boutique/app-email-service" \
      org.opencontainers.image.revision="${SERVICE_COMMIT}" \
      org.opencontainers.image.version="${SERVICE_VERSION}" \
      org.opencontainers.image.licenses="Apache-2.0"

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV SERVICE_VERSION=${SERVICE_VERSION} \
    SERVICE_COMMIT=${SERVICE_COMMIT}

RUN apk add --no-cache libstdc++ \
    && addgroup -S -g 10001 appgroup && adduser -S -u 10001 -G appgroup appuser \
    && mkdir /data && chown 10001:10001 /data

# The mailbox (SQLite, WAL; MAILBOX_DB defaults to /data/mailbox.db). Mount a volume here; a named volume inherits the ownership above.
VOLUME /data

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy generated proto stubs
COPY --from=builder --chown=appuser:appgroup /build/generated ./src/generated/

# Copy application code
COPY --chown=appuser:appgroup src/ ./src/
COPY --chown=appuser:appgroup templates/ ./templates/

# Numeric so Kubernetes runAsNonRoot can verify it (IMG-02: UID:GID 10001)
USER 10001:10001

EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import grpc; ch=grpc.insecure_channel('localhost:${PORT:-8080}'); grpc.channel_ready_future(ch).result(timeout=3)" || exit 1

ENTRYPOINT ["python", "src/server.py"]