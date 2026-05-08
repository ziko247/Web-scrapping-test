import os
import sys
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__))))
from utils.minio_client import list_objects, download_json_from_minio
from utils.postgres_client import init_db, upsert_articles

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

SOURCES = ['hespress', 'cnn']


def load_silver_to_gold(**kwargs):
    """
    Reads cleaned Silver articles (all sources), computes Gold metrics,
    and upserts everything into the PostgreSQL Data Warehouse.
    """
    init_db()

    date_prefix = datetime.now().strftime('%Y/%m/%d')
    gold_articles = []

    for source in SOURCES:
        silver_prefix = f"silver/{source}/{date_prefix}"
        logging.info(f"Querying Silver data at: {silver_prefix}")
        object_names = list_objects('silver-data', silver_prefix)

        if not object_names:
            logging.warning(f"No Silver files for '{source}' today.")
            continue

        for obj_name in object_names:
            try:
                data = download_json_from_minio('silver-data', obj_name)
                content = data.get("content", "")
                word_count = len(content.split()) if content else 0

                gold_articles.append({
                    "source":       data.get("source"),
                    "country":      data.get("country", "Unknown"),
                    "url":          data.get("url"),
                    "title":        data.get("title"),
                    "author":       data.get("author", "Unknown"),
                    "category":     data.get("category", "general"),
                    "language":     data.get("language", "unknown"),
                    "published_at": data.get("published_at"),
                    "scraped_at":   data.get("scraped_at"),
                    "word_count":   word_count,
                    "content":      content,
                })
                logging.info(f"[{source}] Prepared Gold: '{data.get('title')}' | {word_count} words")

            except Exception as e:
                logging.error(f"Failed to process {obj_name}: {e}")
                continue

    # Deduplicate by URL — same article may exist across multiple Bronze runs
    seen_urls = set()
    unique_articles = []
    for a in gold_articles:
        url = a.get("url")
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique_articles.append(a)

    logging.info(f"Deduplicated {len(gold_articles)} → {len(unique_articles)} unique articles.")
    upsert_articles(unique_articles)
    logging.info(f"Gold loading complete. {len(unique_articles)} articles loaded into warehouse.")


with DAG(
    dag_id='gold_warehouse_loading',
    default_args=default_args,
    description='Load cleaned Silver articles (all sources) into PostgreSQL Gold layer',
    schedule=timedelta(hours=1),
    catchup=False,
    tags=['warehouse', 'gold', 'hespress', 'cnn']
) as dag:

    silver_to_gold_task = PythonOperator(
        task_id='load_silver_to_gold',
        python_callable=load_silver_to_gold,
    )
