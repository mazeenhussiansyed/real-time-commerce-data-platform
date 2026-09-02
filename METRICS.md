# P03 Verified Metrics

Only rows marked **Yes** may be used as measured project evidence.

## M00 environment verification

| Metric | Configuration | Result | Evidence command | Verified |
|---|---|---:|---|---|
| Python runtime | local virtual environment | 3.14.4 | `python --version` | Yes, 2026-09-02 |
| Package import | editable installation | 0.1.0 | package import command | Yes, 2026-09-02 |

## M01 operational source verification

| Metric | Configuration | Result | Evidence command | Verified |
|---|---|---:|---|---|
| PostgreSQL source tables | `commerce` schema | 6 | `python scripts/wait_for_postgres.py` | Yes, 2026-09-02 |
| PostgreSQL WAL level | CDC-ready configuration | logical | `python scripts/profile_source.py` | Yes, 2026-09-02 |
| Customers | deterministic seed `20260902` | 1,000 | `python scripts/profile_source.py` | Yes, 2026-09-02 |
| Products | deterministic seed `20260902` | 250 | `python scripts/profile_source.py` | Yes, 2026-09-02 |
| Orders | deterministic seed `20260902` | 5,000 | `python scripts/profile_source.py` | Yes, 2026-09-02 |
| Order items | one to four items per order | 12,500 | `python scripts/profile_source.py` | Yes, 2026-09-02 |
| Payments | one payment per order | 5,000 | `python scripts/profile_source.py` | Yes, 2026-09-02 |
| Shipments | processing, shipped and delivered orders | 2,499 | `python scripts/profile_source.py` | Yes, 2026-09-02 |
| Total stored rows | all six source tables | 26,249 | source profile counts | Yes, 2026-09-02 |
| Total order value | synthetic USD orders | $5,053,882.86 | `python scripts/profile_source.py` | Yes, 2026-09-02 |
| Average order value | 5,000 orders | $1,010.78 | `python scripts/profile_source.py` | Yes, 2026-09-02 |
| Referential-integrity failures | five relationship checks | 0 | `python scripts/profile_source.py` | Yes, 2026-09-02 |
| Order-total mismatches | orders compared with item totals | 0 | `python scripts/profile_source.py` | Yes, 2026-09-02 |
| Payment-total mismatches | orders compared with payments | 0 | `python scripts/profile_source.py` | Yes, 2026-09-02 |
| Dataset fingerprint | SHA-256 | `9616a650...b49d83e5` | `python scripts/seed_source.py --reset` | Yes, 2026-09-02 |
| Baseline load duration | local Docker PostgreSQL | 1.451 seconds | `python scripts/seed_source.py --reset` | Yes, 2026-09-02 |
| Existing-data protection | seed without `--reset` | overwrite rejected | `python scripts/seed_source.py` | Yes, 2026-09-02 |
| Complete M01 test suite | unit and live PostgreSQL tests | 10/10 passed | `RUN_POSTGRES_INTEGRATION=1 python -m unittest discover -s tests -v` | Yes, 2026-09-02 |

## Planned CDC and streaming metrics

| Metric | Result | Evidence command | Verified |
|---|---:|---|---|
| CDC events produced | Not measured | future M02 command | No |
| Source-to-topic completeness | Not measured | future M02 evaluation | No |
| Duplicate business records after replay | Not measured | future M03 evaluation | No |
| Invalid events quarantined | Not measured | future M03 evaluation | No |
| Streaming throughput | Not measured | future M03 benchmark | No |
| Median end-to-end latency | Not measured | future M03 benchmark | No |
| P95 end-to-end latency | Not measured | future M03 benchmark | No |

## Planned warehouse and reliability metrics

| Metric | Result | Evidence command | Verified |
|---|---:|---|---|
| Source-to-warehouse reconciliation | Not measured | future M04 evaluation | No |
| dbt tests passed | Not measured | future M04 command | No |
| Backfill duplicate records | Not measured | future M05 evaluation | No |
| Controlled failure recovery time | Not measured | future M06 benchmark | No |
| Complete automated suite | Not measured | future final test command | No |

## Integrity rules

- Do not replace missing results with targets.
- Do not report throughput without the event count and environment.
- Do not report latency without stating whether the run was cold or warm.
- A replay is successful only when it creates no duplicate business records.
- CDC completeness requires reconciling source changes with consumed events.
- A quarantined record must contain a reason and pipeline-run identifier.
- Cloud services may be listed only after they are used successfully.
- Local benchmarks must not be presented as universal production performance.

## Scope warning

This project uses controlled synthetic commerce data and a local development environment.

The results verify source modeling, deterministic generation, integrity controls and reproducibility. They do not prove internet-scale performance or production readiness.