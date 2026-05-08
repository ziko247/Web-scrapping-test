import os
import sys
import uuid
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import requests
from bs4 import BeautifulSoup

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__))))
from utils.minio_client import upload_json_to_minio
from utils.kafka_client import send_news_event

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

# ---------------------------------------------------------------------------
# Source configurations
# Each entry: (source_name, category_name, category_url, scrape_fn)
# ---------------------------------------------------------------------------

def _upload(source_slug: str, document: dict):
    date_prefix = datetime.now().strftime('%Y/%m/%d')
    object_name = f"bronze/{source_slug}/{date_prefix}/{uuid.uuid4()}.json"
    upload_json_to_minio('raw-data', object_name, document)
    # Stream the event to Kafka
    send_news_event(document)


# ---- Hespress ----------------------------------------------------------------

def scrape_hespress():
    category_url = "https://fr.hespress.com/politique"
    logging.info(f"[Hespress] Scraping category page: {category_url}")

    resp = requests.get(category_url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.content, 'html.parser')

    # Collect article links (.html on hespress.com)
    article_links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        if href.endswith('.html') and 'hespress.com' in href and href not in article_links:
            article_links.append(href)
        if len(article_links) >= 5:
            break

    logging.info(f"[Hespress] Found {len(article_links)} articles.")
    count = 0
    for url in article_links:
        try:
            art = requests.get(url, headers=HEADERS, timeout=15)
            art.raise_for_status()
            art_soup = BeautifulSoup(art.content, 'html.parser')

            title = art_soup.find('h1')
            author_tag = art_soup.find('span', class_='author') or art_soup.find('a', class_='author') or art_soup.find('div', class_='author')
            time_tag = art_soup.find('time')
            content_div = art_soup.find('div', class_='article-content') or art_soup.find('body')

            document = {
                "source": "Hespress",
                "country": "Morocco",
                "url": url,
                "title": title.text.strip() if title else "Unknown Title",
                "author": author_tag.text.strip() if author_tag else "Unknown",
                "category": "politique",
                "published_at": time_tag.text.strip() if time_tag else datetime.now().isoformat(),
                "scraped_at": datetime.now().isoformat(),
                "raw_html": str(content_div),
            }
            _upload('hespress', document)
            count += 1
        except Exception as e:
            logging.error(f"[Hespress] Failed on {url}: {e}")
    logging.info(f"[Hespress] Uploaded {count} articles.")
    return count


# ---- CNN ---------------------------------------------------------------------

def scrape_cnn():
    category_url = "https://edition.cnn.com/world"
    logging.info(f"[CNN] Scraping category page: {category_url}")

    resp = requests.get(category_url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.content, 'html.parser')

    # CNN article links look like /YYYY/MM/DD/world/... or /world/...
    article_links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        # Make absolute
        if href.startswith('/'):
            href = 'https://edition.cnn.com' + href
        if 'edition.cnn.com' in href and '/20' in href and href not in article_links:
            article_links.append(href)
        if len(article_links) >= 5:
            break

    logging.info(f"[CNN] Found {len(article_links)} articles.")
    count = 0
    for url in article_links:
        try:
            art = requests.get(url, headers=HEADERS, timeout=15)
            art.raise_for_status()
            art_soup = BeautifulSoup(art.content, 'html.parser')

            title_tag = art_soup.find('h1')
            # CNN author selectors
            author_tag = (
                art_soup.find('span', class_='byline__name') or
                art_soup.find('div', class_='byline__names') or
                art_soup.find('span', {'data-component': 'byline-name'})
            )
            time_tag = art_soup.find('div', class_='timestamp') or art_soup.find('time')
            content_div = (
                art_soup.find('div', class_='article__content') or
                art_soup.find('div', class_='l-container') or
                art_soup.find('body')
            )

            document = {
                "source": "CNN",
                "country": "USA",
                "url": url,
                "title": title_tag.text.strip() if title_tag else "Unknown Title",
                "author": author_tag.text.strip() if author_tag else "Unknown",
                "category": "world",
                "published_at": time_tag.text.strip() if time_tag else datetime.now().isoformat(),
                "scraped_at": datetime.now().isoformat(),
                "raw_html": str(content_div),
            }
            _upload('cnn', document)
            count += 1
        except Exception as e:
            logging.error(f"[CNN] Failed on {url}: {e}")
    logging.info(f"[CNN] Uploaded {count} articles.")
    return count


# ---- Main task ---------------------------------------------------------------

def scrape_all_sources(**kwargs):
    total = 0
    total += scrape_hespress()
    total += scrape_cnn()
    logging.info(f"Batch scraping complete. Total articles uploaded: {total}")


with DAG(
    dag_id='batch_scraper_ingestion',
    default_args=default_args,
    description='Hourly batch scraper for Hespress and CNN → Bronze layer',
    schedule=timedelta(hours=1),
    catchup=False,
    tags=['ingestion', 'bronze', 'hespress', 'cnn']
) as dag:

    scrape_to_minio_task = PythonOperator(
        task_id='scrape_and_upload_to_minio',
        python_callable=scrape_all_sources,
    )
