# M01 Evaluation: Operational Commerce Source

**Evaluation date:** 2026-09-02

## Objective

Create a reproducible PostgreSQL operational source for later Change Data Capture and streaming milestones.

## Environment

| Component | Configuration |
|---|---|
| Python | 3.14.4 |
| PostgreSQL | `postgres:17-alpine` |
| Database | `commerce` |
| Schema | `commerce` |
| Host port | 5433 |
| WAL level | logical |
| Data type | deterministic synthetic commerce data |
| Seed | 20260902 |

## Source model

The relational source contains:

- customers;
- products;
- orders;
- order items;
- payments;
- shipments.

The schema includes primary keys, foreign keys, uniqueness constraints, business checks, indexes and automatic update timestamps.

## Reproducible load

```bash
docker compose up -d postgres
python scripts/wait_for_postgres.py
python scripts/seed_source.py --reset
python scripts/profile_source.py