import os
import sys
import re
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from bs4 import BeautifulSoup

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__))))
from utils.minio_client import list_objects, download_json_from_minio, upload_json_to_minio

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Sources to process (Bronze bucket slug)
SOURCES = ['hespress', 'cnn']


def clean_html(raw_html: str) -> str:
    """Strips scripts/styles/nav and extracts visible text."""
    if not raw_html:
        return ""
    soup = BeautifulSoup(raw_html, 'html.parser')
    for tag in soup(['script', 'style', 'header', 'footer', 'nav', 'aside']):
        tag.decompose()
    text = soup.get_text(separator=' ')
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    return ' '.join(chunk for chunk in chunks if chunk)


def normalize_text(text: str) -> str:
    """
    Text normalization:
    - Collapse multiple spaces/newlines into a single space
    - Remove non-printable control characters
    """
    if not text:
        return ""
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', text)  # control chars
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def detect_language(text: str) -> str:
    """Detects the language of a text snippet. Returns ISO 639-1 code or 'unknown'."""
    try:
        from langdetect import detect
        if text and len(text) > 20:
            return detect(text[:500])
    except Exception:
        pass
    return 'unknown'


def process_bronze_to_silver(**kwargs):
    date_prefix = datetime.now().strftime('%Y/%m/%d')
    total_processed = 0

    for source in SOURCES:
        bronze_prefix = f"bronze/{source}/{date_prefix}"
        logging.info(f"Looking for Bronze data at prefix: {bronze_prefix}")
        object_names = list_objects('raw-data', bronze_prefix)

        if not object_names:
            logging.info(f"No files found for source '{source}' today.")
            continue

        logging.info(f"Found {len(object_names)} files for '{source}'. Cleaning...")

        for obj_name in object_names:
            try:
                silver_obj_name = obj_name.replace("bronze/", "silver/")
                raw_data = download_json_from_minio('raw-data', obj_name)

                # 1. Clean HTML
                raw_html = raw_data.get('raw_html', '')
                clean_text = clean_html(raw_html)

                # 2. Normalize text
                clean_text = normalize_text(clean_text)

                # 3. Detect language
                language = detect_language(clean_text)

                silver_data = {
                    "source":      raw_data.get("source"),
                    "country":     raw_data.get("country", "Unknown"),
                    "url":         raw_data.get("url"),
                    "title":       raw_data.get("title"),
                    "author":      raw_data.get("author", "Unknown"),
                    "category":    raw_data.get("category", "general"),
                    "language":    language,
                    "published_at": raw_data.get("published_at"),
                    "scraped_at":  raw_data.get("scraped_at"),
                    "cleaned_at":  datetime.now().isoformat(),
                    "content":     clean_text,
                }

                upload_json_to_minio('silver-data', silver_obj_name, silver_data)
                total_processed += 1
                logging.info(f"Cleaned [{source}] '{raw_data.get('title')}' | lang={language}")

            except Exception as e:
                logging.error(f"Failed to process {obj_name}: {e}")
                continue


from utils.postgres_client import log_quality_check

# ---------------------------------------------------------------------------
# Quality rules (spec §9)
# ---------------------------------------------------------------------------
MIN_WORD_COUNT = 50 

def _run_quality_checks(article: dict) -> list:
    """
    Returns a list of tuples for the database: (url, source, dimension, test, status)
    """
    results = []
    url = article.get("url")
    src = article.get("source")

    # Dimension: COMPLÉTUDE
    results.append((url, src, "COMPLÉTUDE", "MISSING_TITLE", 
                   "FAIL" if (not article.get("title") or article.get("title") == "Unknown Title") else "PASS"))
    results.append((url, src, "COMPLÉTUDE", "MISSING_DATE", 
                   "FAIL" if not article.get("published_at") else "PASS"))

    # Dimension: VALIDITÉ
    content = article.get("content", "")
    word_count = len(content.split()) if content else 0
    results.append((url, src, "VALIDITÉ", "EMPTY_CONTENT", 
                   "FAIL" if word_count == 0 else "PASS"))
    results.append((url, src, "VALIDITÉ", "CONTENT_TOO_SHORT", 
                   "FAIL" if word_count < MIN_WORD_COUNT else "PASS"))

    # Dimension: COHÉRENCE
    results.append((url, src, "COHÉRENCE", "LANGUAGE_UNDETECTED", 
                   "FAIL" if article.get("language") == "unknown" else "PASS"))

    return results


def validate_silver_data(**kwargs):
    date_prefix = datetime.now().strftime('%Y/%m/%d')
    all_logs = []

    for source in SOURCES:
        silver_prefix = f"silver/{source}/{date_prefix}"
        object_names = list_objects('silver-data', silver_prefix)

        for obj_name in object_names:
            try:
                article = download_json_from_minio('silver-data', obj_name)
                checks = _run_quality_checks(article)
                all_logs.extend(checks)
                
                # If any check failed, quarantine it
                if any(c[4] == "FAIL" for c in checks):
                    quarantine_name = obj_name.replace("silver/", "quarantine/")
                    article["quality_failures"] = [c[3] for c in checks if c[4] == "FAIL"]
                    upload_json_to_minio('silver-data', quarantine_name, article)

            except Exception as e:
                logging.error(f"Error checking {obj_name}: {e}")

    # Write all quality audit results to Postgres for the Dashboard
    log_quality_check(all_logs)
    logging.info(f"Quality audit complete. Logged {len(all_logs)} tests to Data Warehouse.")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id='silver_cleaning_processing',
    default_args=default_args,
    description='Clean Bronze HTML → structured Silver data + quality checks (all sources)',
    schedule=timedelta(hours=1),
    catchup=False,
    tags=['processing', 'silver', 'quality', 'hespress', 'cnn']
) as dag:

    bronze_to_silver_task = PythonOperator(
        task_id='process_bronze_to_silver',
        python_callable=process_bronze_to_silver,
    )

    validate_silver_task = PythonOperator(
        task_id='validate_silver_data',
        python_callable=validate_silver_data,
    )

    # Chain: Clean → Validate
    bronze_to_silver_task >> validate_silver_task
