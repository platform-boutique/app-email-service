# Container image

Multi-stage [`Dockerfile`](https://github.com/platform-boutique/app-email-service/tree/main/Dockerfile):

| Stage | Base | What happens |
|---|---|---|
| builder | `python:3.12-alpine` | installs `g++`/`linux-headers`, `pip install -r requirements.txt` (fully pinned), **compiles the protos** (`email.proto`, `mailbox.proto`) with `grpc_tools.protoc` into `generated/` and patches the relative import in `email_pb2_grpc.py` and `mailbox_pb2_grpc.py` (both import their `_pb2` module absolutely) |
| runtime | `python:3.12-alpine` | copies site-packages, generated stubs (`/app/src/generated`), `src/`, `templates/`; creates `/data` owned by `100:101` (`VOLUME /data`, the mailbox); runs as non-root |

- **Entrypoint:** `python src/server.py` (working dir `/app`; `src/` is on `sys.path`, so `from generated import …` resolves).
- **User:** numeric `USER 100:101` (`appuser`/`appgroup`, created with these fixed ids; `id` in the container prints `uid=100(appuser) gid=101(appgroup)`). Matches the chart's `runAsUser: 100` / `runAsGroup: 101`.
- **EXPOSE:** `8080` only.
- **Docker `HEALTHCHECK`:** Python `grpc.channel_ready_future` against `localhost:${PORT:-8080}` (honours the default).
- **Image size:** 184 MB uncompressed (`docker images`, local build of this branch on 2026-10-31, was 177 MB before `kafka-python`; the earlier ~41 MB figure was not re-measured and is probably the compressed registry size).
- **Kafka client:** `kafka-python` 3.0.11 (pure Python, no dependencies). `confluent-kafka` was rejected because PyPI has no `musllinux` wheel for it (checked with `pip download --only-binary=:all: --platform musllinux_1_2_x86_64`; only `manylinux`), so it would need a source build against `librdkafka` on `python:3.12-alpine`. `kafka-python` 3.0.12 is newer but was released 2026-10-26, less than 7 days before the pin.
- **Reproducibility:** the image installs from the fully pinned `requirements.txt` (compiled from `requirements.in` with `uv`), so transitive versions no longer float between builds. The lock was not regenerated in this change, so its transitive pins are older than what the unpinned build used to pull (e.g. `wrapt 1.17.3` instead of `2.5.1`).
- **Build context:** [`.dockerignore`](https://github.com/platform-boutique/app-email-service/tree/main/.dockerignore) keeps `.git`, tests, docs, caches and locally compiled stubs out of the context.
- **Volume:** the image declares `VOLUME /data`; a named volume inherits the `100:101` ownership (verified: the mailbox file survived a container restart and was writable as uid 100). A bind mount or a Kubernetes volume must be writable for uid 100 / gid 101.
- The generated stubs are **not** in git; they only exist after the Docker build (see local dev below).
