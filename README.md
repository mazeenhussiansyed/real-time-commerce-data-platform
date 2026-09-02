# Real-Time Commerce CDC and Analytics Platform

A Data Engineering portfolio project that captures commerce database changes in real time, transports them through an event-streaming system, processes them reliably and publishes tested analytics models for operational reporting.

## Current status

Milestone M00 is in progress.

The initial repository and Python environment have been created. No pipeline performance or data-quality metrics have been measured yet.

## Business problem

An online retailer continuously creates and updates:

- customers;
- products;
- orders;
- order items;
- payments;
- shipments.

Analytics teams need trustworthy and timely warehouse tables without querying the operational application database directly.

The platform will capture database changes, process them incrementally and publish analytics-ready datasets while handling duplicates, invalid records, late events and pipeline failures.

## Planned architecture

```text
PostgreSQL commerce database
            |
            v
       Debezium CDC
            |
            v
        Kafka topics
            |
            v
Spark Structured Streaming
            |
            v
Bronze data lake storage
            |
            v
Snowflake analytics warehouse
            |
            v
dbt models, tests and documentation
            |
            v
Power BI operations dashboard