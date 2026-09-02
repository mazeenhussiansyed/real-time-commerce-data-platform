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
| Shipments | eligible order statuses | 2,499 | `python scripts/profile_source.py` | Yes, 2026-09-02 |
| Total stored rows | all six source tables | 26,249 | source profile counts | Yes, 2026-09-02 |
| Total order value | synthetic USD orders | $5,053,882.86 | `python scripts/profile_source.py` | Yes, 2026-09-02 |
| Average order value | 5,000 orders | $1,010.78 | `python scripts/profile_source.py` | Yes, 2026-09-02 |
| Referential-integrity failures | five relationship checks | 0 | `python scripts/profile_source.py` | Yes, 2026-09-02 |
| Order-total mismatches | orders compared with items | 0 | `python scripts/profile_source.py` | Yes, 2026-09-02 |
| Payment-total mismatches | orders compared with payments | 0 | `python scripts/profile_source.py` | Yes, 2026-09-02 |
| Dataset fingerprint | SHA-256 | `9616a650...b49d83e5` | `python scripts/seed_source.py --reset` | Yes, 2026-09-02 |
| Baseline load duration | local Docker PostgreSQL | 1.451 seconds | `python scripts/seed_source.py --reset` | Yes, 2026-09-02 |
| Existing-data protection | seed without `--reset` | overwrite rejected | `python scripts/seed_source.py` | Yes, 2026-09-02 |
| Complete M01 test suite | unit and live PostgreSQL tests | 10/10 passed | live test command | Yes, 2026-09-02 |

## M02 CDC and Kafka verification

| Metric | Configuration | Result | Evidence command | Verified |
|---|---|---:|---|---|
| Kafka version | local KRaft service | 4.1.2 | `docker compose ps` | Yes, 2026-09-02 |
| Debezium version | PostgreSQL source connector | 3.5.2.Final | connector status endpoint | Yes, 2026-09-02 |
| Connector state | `commerce-postgres-cdc` | RUNNING | `python scripts/register_connector.py` | Yes, 2026-09-02 |
| Connector task state | one source task | RUNNING | connector status endpoint | Yes, 2026-09-02 |
| CDC business topics | one per source table | 6 | Kafka topic-list command | Yes, 2026-09-02 |
| Customer-topic partitions | Kafka topic description | 3 | Kafka topic-description command | Yes, 2026-09-02 |
| Replication slot | PostgreSQL logical slot | active | `python scripts/profile_cdc.py` | Yes, 2026-09-02 |
| Publication tables | Debezium publication | 6 | `python scripts/profile_cdc.py` | Yes, 2026-09-02 |
| Initial source rows | six PostgreSQL tables | 26,249 | `python scripts/profile_cdc.py` | Yes, 2026-09-02 |
| Initial Kafka snapshot events | six business topics | 26,249 | `python scripts/profile_cdc.py` | Yes, 2026-09-02 |
| Initial snapshot match | before live probes | exact | `python scripts/profile_cdc.py` | Yes, 2026-09-02 |
| Minimum source-to-topic completeness | six table comparisons | 100% | `python scripts/profile_cdc.py` | Yes, 2026-09-02 |
| Live CDC operations | temporary customer | 3 events | `python scripts/verify_cdc_operations.py --timeout 30` | Yes, 2026-09-02 |
| Live CDC operation order | create, update, delete | `c`, `u`, `d` | live verification command | Yes, 2026-09-02 |
| Probe rows remaining | after delete | 0 | live verification command | Yes, 2026-09-02 |
| Live verification duration | local warm services | 930.544 ms | live verification command | Yes, 2026-09-02 |
| Final source rows | after three probe runs | 26,249 | `python scripts/profile_cdc.py` | Yes, 2026-09-02 |
| Final Kafka events | snapshot plus nine changes | 26,258 | `python scripts/profile_cdc.py` | Yes, 2026-09-02 |
| Historical probe events retained | three create/update/delete runs | 9 | CDC profile difference | Yes, 2026-09-02 |
| Complete M02 test suite | unit, PostgreSQL and CDC integration | 17/17 passed | complete live test command | Yes, 2026-09-02 |
| Complete M02 test duration | local warm services | 2.816 seconds | complete live test command | Yes, 2026-09-02 |

Complete live test command:

```bash
RUN_POSTGRES_INTEGRATION=1 \
RUN_CDC_INTEGRATION=1 \
python -m unittest discover -s tests -v