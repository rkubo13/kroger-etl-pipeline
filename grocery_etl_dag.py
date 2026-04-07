"""
Airflow DAG: grocery_etl_daily

Orchestrates: extract → quality_check → clean → enrich → compute_metrics → load
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import yaml
import pandas as pd
import logging

logger = logging.getLogger(__name__)
CONFIG_PATH = "/opt/airflow/config/pipeline_config.yaml"


def _load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def extract(**context):
    from etl.extractors.csv_extractor import CSVExtractor
    config = _load_config()
    extractor = CSVExtractor(
        raw_data_path=config["paths"]["raw_data"],
        expected_columns=config["extract"]["expected_columns"],
    )
    transactions = extractor.extract_transactions(config["extract"]["transactions_file"])
    products = extractor.extract_products(config["extract"]["products_file"])
    context["ti"].xcom_push(key="transactions", value=transactions.to_json())
    context["ti"].xcom_push(key="products", value=products.to_json())


def quality_check(**context):
    from etl.quality.checks import run_all_checks
    config = _load_config()
    df = pd.read_json(context["ti"].xcom_pull(task_ids="extract", key="transactions"))
    run_all_checks(df, config)


def clean(**context):
    from etl.transformers.clean import clean_transactions
    df = pd.read_json(context["ti"].xcom_pull(task_ids="extract", key="transactions"))
    cleaned = clean_transactions(df)
    context["ti"].xcom_push(key="cleaned", value=cleaned.to_json())


def enrich(**context):
    from etl.transformers.enrich import join_product_catalog, aggregate_household_weekly
    cleaned = pd.read_json(context["ti"].xcom_pull(task_ids="clean", key="cleaned"))
    products = pd.read_json(context["ti"].xcom_pull(task_ids="extract", key="products"))
    enriched = join_product_catalog(cleaned, products)
    agg = aggregate_household_weekly(enriched)
    context["ti"].xcom_push(key="enriched", value=enriched.to_json())
    context["ti"].xcom_push(key="agg_household", value=agg.to_json())


def compute_metrics(**context):
    from etl.transformers.metrics import compute_promo_lift
    config = _load_config()
    enriched = pd.read_json(context["ti"].xcom_pull(task_ids="enrich", key="enriched"))
    threshold = config["transform"]["promo_discount_threshold"]
    lift = compute_promo_lift(enriched, discount_threshold=threshold)
    context["ti"].xcom_push(key="promo_lift", value=lift.to_json())


def load(**context):
    from etl.loaders.postgres_loader import PostgresLoader
    config = _load_config()
    loader = PostgresLoader(config["database"])
    loader.create_tables()

    enriched = pd.read_json(context["ti"].xcom_pull(task_ids="enrich", key="enriched"))
    enriched["line_total"] = enriched["quantity"] * enriched["unit_price"] - enriched["discount"]
    fact_cols = ["order_id","household_id","product_id","quantity","unit_price","discount","transaction_date","line_total"]
    loader.load_dataframe(enriched[fact_cols], "fact_transactions")

    products = pd.read_json(context["ti"].xcom_pull(task_ids="extract", key="products"))
    loader.load_dataframe(products, "dim_products")

    agg = pd.read_json(context["ti"].xcom_pull(task_ids="enrich", key="agg_household"))
    loader.load_dataframe(agg, "agg_household_weekly")

    lift = pd.read_json(context["ti"].xcom_pull(task_ids="compute_metrics", key="promo_lift"))
    loader.load_dataframe(lift, "agg_promo_lift")
    logger.info("All tables loaded successfully")


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="grocery_etl_daily",
    default_args=default_args,
    description="Daily ETL: raw grocery data → analytics warehouse",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "grocery", "production"],
) as dag:
    t_extract = PythonOperator(task_id="extract", python_callable=extract)
    t_quality = PythonOperator(task_id="quality_check", python_callable=quality_check)
    t_clean = PythonOperator(task_id="clean", python_callable=clean)
    t_enrich = PythonOperator(task_id="enrich", python_callable=enrich)
    t_metrics = PythonOperator(task_id="compute_metrics", python_callable=compute_metrics)
    t_load = PythonOperator(task_id="load", python_callable=load)

    t_extract >> t_quality >> t_clean >> t_enrich >> t_metrics >> t_load
