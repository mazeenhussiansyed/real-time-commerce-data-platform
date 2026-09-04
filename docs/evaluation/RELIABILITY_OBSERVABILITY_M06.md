# M06 Reliability and Observability Evaluation

**Evaluation date:** 2026-09-04
**Environment:** Local Docker Desktop and WSL2
**Status:** Complete

## Scope

M06 adds reliability policies, live reconciliation, freshness and latency measurements, service-health reporting, event-order and schema-change scenarios, checkpoint recovery evidence, quarantine reporting, and automated tests.

## Final operational profile

| Result | Verified value |
|---|---:|
| Operational checks | 12/12 passed |
| Live Bronze events | 26,291 |
| Warehouse events | 26,291 |
| Unique warehouse event IDs | 26,291 |
| Missing or unexpected events | 0 |
| Duplicate event IDs | 0 |
| Null required metadata values | 0 |
| Quarantined records | 8 |
| Quarantine reason | `unsupported_operation` |
| Observed baseline out-of-order events | 0 |
| Freshness SLO | 86,400 seconds |
| Warehouse data age | 97.584 seconds |
| Latest successful pipeline age | 25,660.734 seconds |
| Average connector latency | 869.283 milliseconds |
| Maximum connector latency | 1,460 milliseconds |
| Measured latency events | 26,291 |

All six current-state counts reconciled exactly: 1,000 customers, 250 products, 5,000 orders, 12,500 order items, 5,000 payments, and 2,499 shipments.

## Operational health

The source and warehouse PostgreSQL services, Kafka, the Debezium connector and task, and the Airflow metadata database, scheduler, and DAG processor were healthy. All six expected Kafka topics were present.

## Event-order and schema scenarios

The policy uses a five-minute allowed-lateness threshold.

| Scenario | Result |
|---|---|
| On-time event, zero-second delay | `on_time`; no backfill |
| Out-of-order event, 60-second delay | `out_of_order`; no backfill |
| Late event, 7,200-second delay | `late`; backfill required |
| Unchanged customer schema | Compatible |
| Additive `loyalty_tier` field | Compatible |
| Missing `customer_id` key | Incompatible |
| String value for numeric key | Incompatible |
| Simulated three-record target deficit | Detected |

## Controlled checkpoint recovery

A temporary customer event was created after a clean checkpoint baseline. Kafka was stopped, and Spark ingestion failed with a nonzero exit code without advancing its committed checkpoint.

After Kafka restarted, Spark resumed from the existing checkpoint and processed exactly one pending create event with zero duplicates. Recovery from Kafka restart through successful Spark completion took 33.221 seconds.

The temporary customer was deleted and its delete event was processed exactly once. The source and current-state model returned to 1,000 customers while both events remained as CDC history. Final synchronization produced 26,291 matching Bronze and warehouse events with zero duplicates.

## Test evidence

| Test group | Result |
|---|---:|
| Reliability policy unit tests | 15/15 passed |
| Reliability live integration tests | 6/6 passed in 14.254 seconds |
| Complete M01-M06 Python suite | 68/68 passed in 51.457 seconds |
| Final dbt build | 115/115 passed in 2.93 seconds |

## Evidence commands

```bash
python scripts/verify_reliability_scenarios.py
python scripts/profile_reliability.py
python -m unittest discover -s tests -p 'test_reliability.py' -v
RUN_RELIABILITY_INTEGRATION=1 python -m unittest discover -s tests -p 'test_reliability_integration.py' -v
```

## Interpretation and limitations

The 97.584-second warehouse age is the final operational freshness measurement. Historical Bronze, warehouse-load, and end-to-end latency aggregates include the original snapshot and events intentionally left at rest between milestone runs; they are not steady-state production latency measurements.

This local evaluation does not prove distributed-cluster fault tolerance, managed-service behavior, multi-region recovery, or internet-scale performance.
