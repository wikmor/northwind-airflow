# Northwind Airflow ETL Pipeline

An end-to-end ETL data pipeline built with Apache Airflow, PostgreSQL, and Docker. It transforms the classic Northwind OLTP database into an analytics-ready star schema data warehouse.

## Architecture

Three-layer pipeline: **Source** (raw OLTP data) → **Staging** (cleansed temp tables) → **Star** (dimensional model).

The star schema has five dimensions (`time`, `customer`, `product`, `supplier`, `employee`) and one fact table (`orders_facts`) tracking price and quantity at the order-line grain.

## Tech Stack

| Tool | Version | Role |
|------|---------|------|
| Apache Airflow | 2.9.1 | Orchestration & scheduling |
| PostgreSQL | 13 | Source, staging, and star databases |
| Redis | 7.2 | Celery message broker |
| Docker & Docker Compose | — | Containerized local environment |
| Python | 3.x | DAG logic and transformations |
| Faker | — | Synthetic data generation |
| CeleryExecutor | — | Distributed task execution |

## How It Works

The main `etl_dag` runs daily and has 37 tasks in three phases:

1. **Truncate** — clears staging and star tables for idempotent reruns
2. **Transform** — for each of 7 entities: fetch from Source, clean, write to Staging
3. **Load** — read from Staging, write to Star dimensions and fact table

A second DAG, `new_data_simulator_dag`, can be triggered manually to insert random suppliers with NULL countries, simulating messy real-world data.

Key design choices:
- `BaseDataTransfer` abstract class keeps ETL logic DRY; `Transform` and `Load` subclasses override only what differs
- Three `PostgresHook` wrapper classes (`SourceUtil`, `StagingUtil`, `StarUtil`) keep connection details out of business logic
- Truncating before each run makes the DAG safely re-triggerable without duplicating data

## Getting Started

Requires Docker and Docker Compose.

```bash
git clone https://github.com/wikmor/northwind-airflow.git
cd northwind-airflow
docker compose up -d
```

Open **http://localhost:8080** (username: `airflow`, password: `airflow`), then enable and trigger `etl_dag`.

Databases are accessible at `localhost:5432` (user/password: `airflow`): `source`, `staging`, `star`.

## What I Learned

- **Apache Airflow** — DAG authoring, task dependencies, operators, scheduling, and idempotency
- **Data modelling** — star schema design, fact grain selection, manager hierarchy resolution with self-joins
- **ETL patterns** — layered architecture, separating extract/clean/load, reusable OOP base classes
- **PostgreSQL** — multi-database setup, `INSERT … SELECT` patterns
- **Docker** — multi-service Compose stack with health checks and volume-mounted DAGs
- **Distributed computing** — CeleryExecutor with Redis, parallel task execution across workers
- **Data quality** — NULL handling in pipeline logic, synthetic data for edge-case testing
