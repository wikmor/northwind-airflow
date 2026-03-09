# Northwind Airflow ETL Pipeline

A production-style **end-to-end ETL data pipeline** built with Apache Airflow, PostgreSQL, and Docker. This project transforms the classic Northwind OLTP database into an analytics-ready star schema data warehouse, demonstrating core data engineering skills.

---

## 🗂️ Project Overview

The pipeline ingests data from a transactional source database, cleanses and transforms it through a staging layer, and loads it into a dimensional data warehouse (star schema) — all orchestrated automatically by Apache Airflow on a daily schedule.

```
┌─────────────────┐     ┌─────────────────┐     ┌──────────────────┐
│   SOURCE DB     │────▶│   STAGING DB    │────▶│    STAR DB       │
│  (OLTP/Northwind│     │  (Cleaned data  │     │  (Star schema /  │
│   12 tables)    │     │   7 tmp tables) │     │  Data Warehouse) │
└─────────────────┘     └─────────────────┘     └──────────────────┘
         Extract                Transform                  Load
```

---

## 🏗️ Architecture

### Three-Layer Data Architecture

| Layer | Database | Purpose |
|-------|----------|---------|
| **Source** | `source` | Raw Northwind OLTP data (customers, orders, products, etc.) |
| **Staging** | `staging` | Cleansed and standardized temporary tables |
| **Star** | `star` | Analytics-ready dimensional model for BI/reporting |

### Star Schema (Data Warehouse)

```
                    ┌────────────┐
                    │    time    │
                    └─────┬──────┘
                          │
┌──────────┐    ┌─────────┴────────┐    ┌──────────┐
│ customer │───▶│  orders_facts    │◀───│ employee │
└──────────┘    │  (fact table)    │    └──────────┘
                │                  │
┌──────────┐    │  - price         │    ┌──────────┐
│ product  │───▶│  - quantity      │◀───│ supplier │
└──────────┘    └──────────────────┘    └──────────┘
```

**Dimensions:** `time`, `customer`, `product`, `supplier`, `employee`  
**Fact table:** `orders_facts` — measures price and quantity at the order-line grain

---

## 🛠️ Tech Stack

| Tool | Version | Role |
|------|---------|------|
| **Apache Airflow** | 2.9.1 | Pipeline orchestration & scheduling |
| **PostgreSQL** | 13 | Source, staging, and star databases |
| **Redis** | 7.2 | Celery message broker |
| **Docker & Docker Compose** | — | Full containerised local environment |
| **Python** | 3.x | DAG logic, data transformations |
| **Faker** | — | Synthetic data generation for testing |
| **CeleryExecutor** | — | Distributed, scalable task execution |

---

## 📁 Repository Structure

```
northwind-airflow/
├── dags/
│   ├── etl_dag.py                  # Main ETL DAG (daily schedule, 37 tasks)
│   ├── new_data_simulator_dag.py   # Manually triggered fake-data generator
│   ├── entities.py                 # All ETL entity definitions (SQL + clean fns)
│   ├── base_data_transfer.py       # Abstract base class for ETL steps
│   ├── transform_data.py           # Source → Staging transform class
│   ├── load_data.py                # Staging → Star load class
│   └── utils/
│       └── postgres_util.py        # PostgresHook wrappers (Source/Staging/Star)
└── init/
    ├── 01_create_databases.sql     # Creates source, staging, star databases
    ├── 02_northwind_ddl.sql        # Source schema (12 tables)
    ├── 03_northwind_data.sql       # Northwind seed data
    ├── 04_staging_ddl.sql          # Staging schema (7 tmp tables)
    └── 05_star_ddl.sql             # Star schema (5 dimensions + 1 fact table)
```

---

## ⚙️ How It Works

### ETL DAG — `etl_dag` (Daily Schedule)

The main DAG contains **37 tasks** grouped into three phases:

1. **Truncate** — Clears all staging and star tables for idempotent reruns
2. **Transform** (7 entities × 3 tasks each) — For every entity:
   - `fetch_{entity}` → reads from Source DB
   - `clean_{entity}` → applies Python cleaning function
   - `transfer_{entity}` → writes to Staging DB
3. **Load** (6 tasks) — Reads from Staging and writes to Star schema dimensions and fact table

Task dependencies are wired so dimension tables are always populated before the fact table loads.

### Data Simulator DAG — `new_data_simulator_dag` (Manual Trigger)

Inserts 1–5 random new suppliers into the source database (with a 75% chance of a NULL country) to simulate real-world messy data and validate the pipeline's NULL-handling logic.

---

## 💡 Key Engineering Decisions

- **Object-oriented DAG code** — `BaseDataTransfer` abstract class keeps fetch/clean/load logic DRY; `Transform` and `Load` subclasses override only what differs.
- **Three utility classes** (`SourceUtil`, `StagingUtil`, `StarUtil`) wrap `PostgresHook` behind a consistent interface, keeping connection details out of business logic.
- **Idempotent runs** — Truncating staging and star tables before each run means the DAG can be safely re-triggered without duplicating data.
- **Celery + Redis** — CeleryExecutor allows tasks to run in parallel across multiple workers, mirroring production Airflow setups.
- **Containerized environment** — Docker Compose spins up the entire stack (Airflow webserver, scheduler, worker, triggerer, Postgres, Redis) with a single command.

---

## 🚀 Getting Started

### Prerequisites
- Docker and Docker Compose installed

### Run

```bash
# Clone the repository
git clone https://github.com/wikmor/northwind-airflow.git
cd northwind-airflow

# Start the stack
docker compose up -d

# Wait ~60 seconds for initialization to complete
```

Then open **http://localhost:8080** (username: `airflow`, password: `airflow`).

Enable and trigger the `etl_dag` DAG to run the full pipeline, or trigger `new_data_simulator_dag` to insert fresh test data.

### Optional: Celery Flower monitoring UI

```bash
docker compose --profile flower up -d
# Open http://localhost:5555
```

### Database access

Connect to `localhost:5432` with any PostgreSQL client:
- user: `airflow` / password: `airflow`
- Databases: `source`, `staging`, `star`

---

## 📚 What I Learned

| Area | Takeaways |
|------|-----------|
| **Apache Airflow** | Defining DAGs with complex task dependencies; using `PythonOperator` and `PostgresOperator`; scheduling, backfill strategy, and idempotency |
| **Data modelling** | Designing a star schema from an OLTP source; choosing fact grain; handling slowly changing dimensions (manager hierarchies in employee dim) |
| **ETL patterns** | Layered architecture (source → staging → warehouse); separating extract, clean, and load concerns; building reusable base classes |
| **PostgreSQL** | Multi-database setup in a single Postgres instance; `INSERT … SELECT` patterns; self-joins for hierarchy resolution |
| **Docker & Docker Compose** | Running a multi-service Airflow stack locally; health checks; volume mounting for DAG hot-reload |
| **Distributed computing** | CeleryExecutor with Redis broker; understanding how Airflow schedules tasks across workers |
| **Data quality** | Handling NULL values in pipeline logic; using synthetic data (Faker) to test edge cases |
| **OOP in Python** | Abstract base classes; inheritance for code reuse across ETL steps |

---

## 📄 License

This project uses the [Northwind sample dataset](https://github.com/pthom/northwind_psql) which is freely available for educational use.
