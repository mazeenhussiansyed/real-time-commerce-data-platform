# Real-Time Commerce CDC and Analytics Platform

A Data Engineering portfolio project that captures commerce database changes in real time, transports them through an event-streaming system, processes them reliably and publishes tested analytics models for operational reporting.

## Current status

Milestones M00 and M01 are complete:

- project scope and evidence contract;
- local Python package;
- PostgreSQL operational source;
- six-table relational commerce model;
- deterministic synthetic-data generation;
- atomic source loading;
- overwrite protection;
- source profiling and integrity validation;
- automated unit and PostgreSQL integration tests.

Change Data Capture and Kafka event transport remain planned for M02.

## Verified M01 source results

| Result | Verified value |
|---|---:|
| Customers | 1,000 |
| Products | 250 |
| Orders | 5,000 |
| Order items | 12,500 |
| Payments | 5,000 |
| Shipments | 2,499 |
| Total stored rows | 26,249 |
| Total order value | $5,053,882.86 |
| Average order value | $1,010.78 |
| Referential-integrity failures | 0 |
| Order-total mismatches | 0 |
| Payment-total mismatches | 0 |
| Automated tests | 10/10 passed |
| PostgreSQL WAL level | logical |
| Baseline load duration | 1.451 seconds |

The controlled baseline is reproducible with seed `20260902` and SHA-256 fingerprint:

```text
9616a65028c5f571e5cc4d4c5ced080977fbea1bf3f1117d57b80b67b49d83e5