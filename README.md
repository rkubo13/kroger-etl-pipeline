# Kroger-Style Grocery ETL Pipeline

A **production-grade ETL pipeline** that ingests raw grocery transaction data, transforms it into analytics-ready tables, and loads it into a data warehouse — orchestrated by Apache Airflow, containerized with Docker, and tested with pytest.

Built to mirror the types of data engineering challenges at retail analytics companies like [84.51°](https://www.8451.com/), where layered ETL pipelines power personalization, promotion optimization, and media targeting for 62M+ U.S. households.

## Business Context

Grocery retailers generate massive transaction-level data every time a customer swipes a loyalty card. Turning that raw data into business value requires:

1. **Ingesting** raw transaction logs and product catalogs reliably
2. **Transforming** them into household-level purchase summaries, category affinities, and promotion lift metrics
3. **Loading** clean, validated tables into a warehouse where data scientists can query them
4. **Monitoring** data quality at every stage so downstream models don't silently break

## Architecture

```
[Raw CSV] ──► Extract ──► Transform ──► Load ──► [PostgreSQL]
                 │            │           │
              validate     aggregate    upsert
              log errors   enrich       enforce schema
                 │            │           │
              ┌──────────────────────────────────┐
              │         Apache Airflow            │
              │    (orchestration + retry)        │
              └──────────────────────────────────┘
```

## Tech Stack

| Layer            | Tool                        |
|------------------|-----------------------------|
| Orchestration    | Apache Airflow 2.x          |
| Compute          | Python 3.11                 |
| Data Processing  | pandas, SQLAlchemy          |
| Warehouse        | PostgreSQL                  |
| Containerization | Docker + docker-compose     |
| Testing          | pytest                      |
| CI               | GitHub Actions              |

## Quick Start

```bash
git clone https://github.com/<you>/kroger-etl-pipeline.git
cd kroger-etl-pipeline
docker compose up -d

# Airflow UI: http://localhost:8080 (airflow / airflow)
# Trigger the DAG: grocery_etl_daily
```

## Project Structure

```
kroger-etl-pipeline/
├── dags/
│   └── grocery_etl_dag.py          # Airflow DAG definition
├── etl/
│   ├── extractors/
│   │   └── csv_extractor.py        # Ingest raw files with validation
│   ├── transformers/
│   │   ├── clean.py                # Null handling, type casting, dedup
│   │   ├── enrich.py               # Category rollups, household aggregation
│   │   └── metrics.py              # Basket size, promo lift calculations
│   ├── loaders/
│   │   └── postgres_loader.py      # Upsert into warehouse tables
│   └── quality/
│       └── checks.py               # Data quality assertions
├── tests/
│   ├── test_extractor.py
│   ├── test_transforms.py
│   ├── test_loader.py
│   └── test_quality.py
├── config/
│   └── pipeline_config.yaml
├── docker-compose.yaml
├── Dockerfile
├── requirements.txt
└── README.md
```

## Data Model

### Source (Raw)
- `transactions.csv` — order_id, household_id, product_id, quantity, price, discount, timestamp
- `products.csv` — product_id, name, category, subcategory, brand

### Target (Warehouse)
- `fact_transactions` — cleaned, deduplicated transaction records
- `dim_products` — product dimension with category hierarchy
- `agg_household_weekly` — weekly spend, basket size, category mix per household
- `agg_promo_lift` — incremental sales from promotions vs. baseline

## Running Tests

```bash
docker compose exec airflow pytest tests/ -v --tb=short
```

## What This Demonstrates

- **Production patterns**: retry logic, idempotent loads (upserts), config-driven design
- **Data quality as code**: automated checks that halt the pipeline if data is bad
- **Containerized reproducibility**: `docker compose up` runs the full stack
- **Testability**: unit tests for every ETL stage with fixture data
- **Business awareness**: transforms map to real retail analytics (basket analysis, promo lift, segmentation)
