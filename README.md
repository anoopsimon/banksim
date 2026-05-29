# BankSim — OpenTelemetry + Grafana End-to-End Demo

A fake banking API instrumented with **all three OTEL signals** (traces, metrics, logs),
visualised in Grafana. Built to demonstrate production-grade observability on a resume.

```
gateway (REST) ──► fraud-svc ──► ledger-svc
     │                │               │
     └────────────────┴───────────────┘
                      │ OTLP gRPC
               OTEL Collector
              /        |        \
         Tempo      Prometheus   Loki
              \        |        /
                  Grafana :3000
```

---

## Quick Start

```bash
docker compose up --build
```

That's it. All services start, load generator begins hammering the API,
dashboards auto-provision. Open http://localhost:3000 (admin/admin).

---

## Run Integration Tests

```bash
# In a separate terminal while the stack is running:
docker compose run --rm tests
```

Results push to Pushgateway → Prometheus → Grafana **Test Results panel** automatically.

---

## What's Running

| Service         | Port  | Role                                    |
|----------------|-------|-----------------------------------------|
| gateway         | 8080  | REST API — transfers, balance, history  |
| fraud-svc       | 8081  | Fraud scoring (4 fake rules + ML score) |
| ledger-svc      | 8082  | Ledger commit (3 fake DB ops)           |
| loadgen         | —     | Continuous realistic traffic            |
| otel-collector  | 4317  | OTLP receiver → Tempo/Prometheus/Loki   |
| prometheus      | 9090  | Metrics store                           |
| tempo           | 3200  | Trace store                             |
| loki            | 3100  | Log store                               |
| pushgateway     | 9091  | Test result metrics                     |
| grafana         | 3000  | Dashboards                              |

---

## Grafana Dashboards

Navigate to http://localhost:3000 → Dashboards → BankSim folder.

### 1. RED Dashboard
- Request rate per endpoint (req/s)
- Error rate by reason (fraud_detected, ledger_error, fraud_svc_down)
- Latency percentiles: p50 / p95 / p99
- Fraud block rate with alert threshold
- Active in-flight requests gauge

### 2. Operations Dashboard
- **DB section**: query duration per operation+table, connection pool wait, active connections
- **Fraud section**: checks/s, flagged/s, flagged by reason, per-rule evaluation latency
- **Test Results**: pass/fail counts, last run timestamp, per-test duration bar chart

---

## OTEL Signals in Detail

### Traces
Every request creates a distributed trace spanning all 3 services:
```
gateway.transfer
  ├── gateway.call_fraud_svc
  │     └── fraud.check
  │           ├── fraud.rule.large_amount
  │           ├── fraud.rule.bad_actor
  │           ├── fraud.rule.velocity
  │           └── fraud.rule.ml_model
  └── gateway.call_ledger_svc
        └── ledger.commit
              ├── db.debit_source
              ├── db.credit_destination
              └── db.write_audit
```
Click any trace in Grafana Tempo → jump directly to correlated Loki logs.

### Metrics (Prometheus)
| Metric | Type | Description |
|--------|------|-------------|
| `banksim_gateway_requests_total` | Counter | Requests by endpoint+outcome |
| `banksim_gateway_errors_total` | Counter | Errors by reason |
| `banksim_gateway_request_duration_seconds` | Histogram | Latency with buckets |
| `banksim_gateway_requests_inflight` | UpDownCounter | Current in-flight |
| `banksim_fraud_checks_total` | Counter | Fraud evaluations |
| `banksim_fraud_flagged_total` | Counter | Blocked transactions |
| `banksim_fraud_score_distribution` | Histogram | Score distribution |
| `banksim_fraud_rule_duration_seconds` | Histogram | Per-rule latency |
| `banksim_ledger_commits_total` | Counter | Committed transactions |
| `banksim_ledger_db_query_duration_seconds` | Histogram | DB op latency |
| `banksim_ledger_db_pool_wait_seconds` | Histogram | Pool wait time |
| `banksim_ledger_db_pool_active` | UpDownCounter | Active DB connections |
| `banksim_test_passed_total` | Gauge | Tests passed (pushed) |
| `banksim_test_failed_total` | Gauge | Tests failed (pushed) |
| `banksim_test_duration_seconds` | Gauge | Per-test duration |

### Logs (Loki)
All logs are structured JSON with trace_id + span_id injected automatically
by the OTEL slog bridge. Every significant event is logged:
- Transfer initiated / completed / blocked
- Per fraud-rule outcome (triggered or passed) with all rule fields
- DB operations: debit/credit amounts, slow query warnings
- Pool acquisition wait times

---

## Manual curl Examples

```bash
# Health
curl http://localhost:8080/health

# Transfer (normal)
curl -X POST http://localhost:8080/v1/transfer \
  -H "Content-Type: application/json" \
  -d '{"from_account":"ACC1001","to_account":"ACC1002","amount":500,"currency":"AUD"}'

# Transfer (will trigger fraud — bad actor account)
curl -X POST http://localhost:8080/v1/transfer \
  -H "Content-Type: application/json" \
  -d '{"from_account":"ACC0666","to_account":"ACC1001","amount":100,"currency":"AUD"}'

# Transfer (will likely trigger fraud — large amount)
curl -X POST http://localhost:8080/v1/transfer \
  -H "Content-Type: application/json" \
  -d '{"from_account":"ACC1001","to_account":"ACC1002","amount":9500,"currency":"AUD"}'

# Balance
curl "http://localhost:8080/v1/balance?account=ACC1001"

# Transaction history
curl "http://localhost:8080/v1/transactions?account=ACC1001"
```

---

## OTEL Collector Internals

`otel-collector/config.yaml` defines three pipelines:

```
traces  pipeline: otlp receiver → memory_limiter → batch → tempo
metrics pipeline: otlp receiver → memory_limiter → resource enrichment → batch → prometheus
logs    pipeline: otlp receiver → memory_limiter → label extraction → batch → loki
```

The **zPages debug UI** at http://localhost:55679 shows live pipeline stats —
useful for understanding what the collector is doing.

---

## Key Learning Points

1. **OTEL SDK bootstrap** — `initOTEL()` in each service wires TracerProvider + MeterProvider
2. **Context propagation** — `otelhttp.NewTransport` injects W3C `traceparent` header automatically
3. **Trace → Log correlation** — OTEL slog bridge injects trace_id/span_id into every log line
4. **Collector pipelines** — receivers → processors → exporters; each signal is independent
5. **Histogram buckets** — explicit buckets chosen for latency ranges that make sense for banking
6. **Pushgateway pattern** — for short-lived jobs (tests) that can't be scraped
7. **Tempo service graph** — derived from trace data, shows service dependency map automatically

---


