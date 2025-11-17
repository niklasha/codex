# Codex App Server Kafka Transport Guide

This guide describes how to enable, configure, and operate the Kafka transport for `codex-app-server`. Use it when you need the agent to communicate over Kafka topics instead of (or in addition to) the default stdio channel.

## 1. Requirements

- Apache Kafka 2.8+ or a compatible managed service.
- Network reachability from the host running `codex-app-server` to the Kafka brokers.
- A dedicated consumer group ID for the app server so offsets are isolated.
- TLS and SASL can be configured via standard `librdkafka` environment variables if required by your cluster (e.g., `KAFKA_SSL_CA`, `KAFKA_SASL_USERNAME`).

## 2. Command-line configuration

Run `codex-app-server` with the Kafka flags:

```
codex-app-server \
  --kafka-bootstrap-servers broker1:9092,broker2:9092 \
  --kafka-input-topic codex.requests \
  --kafka-output-topic codex.responses \
  --kafka-group-id codex-app-server \
  --kafka-client-id codex-app-server \
  --stdio  # optional: keep stdin/stdout JSON transport enabled
```

Flag summary:

| Flag | Description |
| --- | --- |
| `--kafka-bootstrap-servers` | Comma-separated broker list. Required once any Kafka flag is set. |
| `--kafka-input-topic` | Topic carrying inbound JSON-RPC messages (requests, notifications). |
| `--kafka-output-topic` | Topic for outbound responses/notifications. |
| `--kafka-group-id` | Consumer group. Each running app server instance must have a unique group or share one intentionally. |
| `--kafka-client-id` | Optional identifier applied to both consumer and producer. Useful for metrics. |
| `--stdio` | Keep stdio JSON transport active alongside Kafka. Omit to disable stdio entirely. |

The CLI treats the presence of any Kafka flag as intent to enable the transport. All required flags must be present; otherwise the CLI exits with validation errors.

## 3. How messages flow

- Inbound: the consumer streams messages from `--kafka-input-topic`. Each payload must be UTF-8 JSON encoding a `JSONRPCMessage`. Valid requests, responses, errors, and notifications are forwarded to the `MessageProcessor` pipeline.
- Outbound: `codex-app-server` emits responses and notifications to `--kafka-output-topic`. Responses/errors include the JSON-RPC request ID as the Kafka message key; notifications have no key.
- Offset management: the consumer commits offsets asynchronously after successfully enqueueing messages. If a commit fails, the app server logs a warning but continues processing.

## 4. Operations checklist

1. **Provision topics**: Create the input/output topics with sufficient partitions. Even a single partition works for initial testing.
2. **Configure ACLs**: Grant the configured group/client read access to the input topic and write access to the output topic.
3. **Deploy**: Launch `codex-app-server` with the Kafka flags (see Section 2). Use systemd or your process supervisor to restart on failure.
4. **Observe logs**: Use `journalctl -u codex-app-server` (or equivalent) to monitor for `kafka` warnings. Payload parsing issues usually indicate malformed JSON; broker errors normally show up as `rdkafka` warnings.
5. **Offset monitoring**: Use Kafka tooling (e.g., `kafka-consumer-groups.sh`) to confirm the consumer group is keeping up.
6. **Graceful shutdown**: On SIGTERM/SIGINT the server drains in-flight Kafka tasks and flushes the producer before exiting.

## 5. Testing the transport

The repo now includes unit tests that cover payload parsing and serialization logic (`cargo test -p codex-app-server kafka`). To perform an end-to-end test:

1. Start a local Kafka broker (e.g., using `docker compose` or `redpanda`).
2. Create two topics: `codex.requests` and `codex.responses`.
3. Launch the app server with the Kafka flags pointing at the local broker.
4. Publish a JSON-RPC request to the input topic. Example:
   ```
   kafka-console-producer --bootstrap-server localhost:9092 --topic codex.requests <<'JSON'
   {"jsonrpc":"2.0","id":1,"method":"noop","params":{}}
   JSON
   ```
5. Consume from `codex.responses` to verify the server responds.

## 6. Troubleshooting

| Symptom | Likely cause | Resolution |
| --- | --- | --- |
| `invalid payload from kafka` warnings | Non UTF-8 or malformed JSON in the topic | Ensure upstream publishers send valid JSON-RPC payloads. |
| `failed to send kafka message` warnings | Producer cannot reach brokers | Check broker connectivity, TLS/SASL config, and `bootstrap.servers`. |
| Consumer stuck at old offsets | Another process using same `--kafka-group-id` | Give each deployment a unique group ID. |
| `librdkafka` errors on startup | Missing OpenSSL/kerberos libs | Install `librdkafka` dependencies (see OpenBSD build notes) and verify `LD_LIBRARY_PATH`. |

## 7. Maintenance tips

- Keep `rdkafka` up to date across all branches (connectivity and OpenBSD variants). The OpenBSD fork lives at `niklasha/rust-rdkafka` in branch `niklasha/openbsd/buildability`.
- Automate topic creation and ACL configuration in your infra-as-code so new environments are consistent.
- Capture metrics: `rdkafka` emits statistics via logs. For deeper insights enable `statistics.interval.ms` and scrape the JSON blobs.

This document lives at `docs/kafka.md`. Update it whenever CLI flags or operational expectations change.
