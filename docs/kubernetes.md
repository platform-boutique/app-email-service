# Kubernetes

**Workload:** with Kafka off, stateless (scale freely). **With Kafka on it holds state:** every replica has its own SQLite mailbox and the consumer group splits the 3 partitions between them, so with more than one replica a mail lives only in the pod that consumed its order, and `ListOrderEmails` / `GetOrderEmail` (balanced across pods) would answer inconsistently. Run **one replica** with a persistent or emptyDir `/data` until the mailbox moves to shared storage. The Helm chart (2 replicas, HPA, read-only root filesystem, no volume) is **not updated**; that is Deferred work.

**Ports to declare**

| Name | Port | Protocol | Expose via Service? |
|---|---|---|---|
| `grpc` | `8080` | TCP (HTTP/2) | Yes — `checkout-service` and `frontend` dial `email-service:8080` |

**Service DNS name expected by callers:** `email-service` (`checkout-service`: `EMAIL_SERVICE_ADDR=email-service:8080`). `recommendation-service` also listens on `8080` — different Service, no clash.

**Probes**

```yaml
readinessProbe:
  grpc: { port: 8080 }
  initialDelaySeconds: 3
  periodSeconds: 5
livenessProbe:
  grpc: { port: 8080 }
  periodSeconds: 10
  failureThreshold: 3
```

**Graceful shutdown:** `SIGTERM` (and `SIGINT`) is handled even as PID 1: the Kafka consumer (if on) finishes its current message, commits and closes (bounded at 2 s, in parallel with the drain; a stuck consumer thread is abandoned and its message is redelivered), the server stops accepting new RPCs, gives in-flight ones up to 2 s (`server.stop(2)`), flushes telemetry when it is on (bounded at 2 s, also when the collector is unreachable) and exits `0`. The worst case (4 s) fits the chart's `terminationGracePeriodSeconds: 5`. Measured: `docker stop` on the idle container returned in 0.36 s with exit code 0; running from source against a stub OTLP receiver, the final traces, metrics and logs batches arrived after `SIGTERM` and the process exited 0; with nothing listening on the endpoint it exited 0 after the 2 s flush bound (a test covers this). With the consumer on (this branch): `docker stop` returned with exit code 0 in about 0.6 s with Kafka up, and the log shows the consumer stopped within 20 ms of `SIGTERM` with Kafka down; a test starts the real server with an unreachable broker and checks health `SERVING`, exit 0 and under 5 s.

**Security context**

```yaml
securityContext:
  runAsNonRoot: true
  runAsUser: 100
  runAsGroup: 101
  allowPrivilegeEscalation: false
  capabilities: { drop: ["ALL"] }
  readOnlyRootFilesystem: true   # verified in-cluster before the mailbox existed; with Kafka on, /data must be a writable volume (not yet in the chart)
```

**Resources** — requests sized from measured idle usage in the test stack (see the chart's `values.yaml` for limits):

```yaml
resources:
  requests: { cpu: 100m, memory: 64Mi }
  limits:   { cpu: "500m", memory: 192Mi }
```

**Config / secrets:** `KAFKA_BROKERS`, `KAFKA_CONSUMER_GROUP`, `MAILBOX_DB` (no secrets; Kafka is plaintext without auth). If this ever becomes a real mailer, SMTP credentials belong in a `Secret`. The mailbox holds recipient addresses and order contents in clear text on the volume.

**Telemetry:** pushed, not scraped — OTLP/gRPC to Alloy (`global.monitoring.otlpEndpoint`), profiles to Alloy `:4040`. Metrics carry `job="email-service"` and `instance=<pod>`; no scrape annotations or `ServiceMonitor` are needed.

**Helm chart:** [`platform-demo-helm/email-service`](https://github.com/platform-boutique/platform-demo-helm/tree/main/email-service) (built on the shared `platform-common` library chart). It ships with HA defaults — 2 replicas,
HPA, PodDisruptionBudget, topology spread, NetworkPolicy, restricted pod security context, probes and the full env contract (see Configuration) —
and was verified under the `restricted` Pod Security Standard on the earlier local 3-node kind cluster (since retired). The platform now targets AWS EKS, where this chart has not been deployed or verified yet.

**Deployment and admission policies:** Argo CD deploys the chart from `platform-demo-helm` (`envs/<env>/values/email-service.yaml` pins the image digest; promotion is a pull request). In the cluster Kyverno enforces, for this service: pods named `app.kubernetes.io/name: email-service` that run under the ServiceAccount of the same name (Cilium derives the network identity from that label), an image from this repository pinned by digest and signed by our release pipeline, a read-only root filesystem, resource requests and a memory limit, readiness and liveness probes and no API token. The Helm CI evaluates the rendered chart against these policies before merge; see [`platform-demo-helm/policies`](https://github.com/platform-boutique/platform-demo-helm/tree/main/policies).
