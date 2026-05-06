import os
import sys
import uuid
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import requests
from bs4 import BeautifulSoup

# Ensure the 'dags' folder is in sys.path so we can import our custom utils
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__))))
from utils.minio_client import upload_json_to_minio

# Define default arguments for the DAG
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# The target URLs to scrape for the architectural PoC
# We are using a static category page to keep it simple and avoid IP bans.
TARGET_URLS = [
    "https://en.hespress.com/category/politics"
]

def scrape_articles_task(**kwargs):
    """
    Scrapes the target category pages, extracts a small subset of articles,
    and uploads the raw metadata + HTML to the MinIO raw-data bucket.
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    }
    
    total_scraped = 0

    for category_url in TARGET_URLS:
        logging.info(f"Initiating scraping for category page: {category_url}")
        
        try:
            response = requests.get(category_url, headers=headers, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find the top 3 article links to avoid heavy scraping / IP blocks
            # Adjust selectors based on Hespress's actual DOM structure
            article_links = []
            for h3 in soup.find_all('h3', class_='card-title')[:3]:
                a_tag = h3.find('a')
                if a_tag and a_tag.get('href'):
                    article_links.append(a_tag['href'])
            
            # Fallback if the first selector didn't work
            if not article_links:
                for a_tag in soup.find_all('a')[:10]:
                    href = a_tag.get('href', '')
                    # Heuristic to identify an article link
                    if '-news' in href or '/politics/' in href:
                        article_links.append(href)
                        if len(article_links) >= 3:
                            break

            logging.info(f"Found {len(article_links)} articles to scrape from {category_url}")
            
            for url in article_links:
                # Ensure the URL is absolute
                article_url = url if url.startswith('http') else 'https://en.hespress.com' + url
                
                try:
                    logging.info(f"Scraping article: {article_url}")
                    art_resp = requests.get(article_url, headers=headers, timeout=15)
                    art_resp.raise_for_status()
                    art_soup = BeautifulSoup(art_resp.content, 'html.parser')
                    
                    # Extract basic metadata
                    title_tag = art_soup.find('h1')
                    title = title_tag.text.strip() if title_tag else "Unknown Title"
                    
                    time_tag = art_soup.find('time')
                    timestamp = time_tag.text.strip() if time_tag else datetime.now().isoformat()
                    
                    # Store the raw HTML body (can refine the selector if we only want the article text div)
                    content_div = art_soup.find('div', class_='article-content')
                    # If not found, just store the whole body to ensure we don't lose data in Bronze layer
                    raw_html = str(content_div) if content_div else str(art_soup.find('body'))
                    
                    # Prepare the document payload
                    document = {
                        "source": "Hespress",
                        "url": article_url,
                        "title": title,
                        "published_at": timestamp,
                        "scraped_at": datetime.now().isoformat(),
                        "raw_html": raw_html
                    }
                    
                    # Upload to MinIO
                    date_prefix = datetime.now().strftime('%Y/%m/%d')
                    object_name = f"bronze/hespress/{date_prefix}/{uuid.uuid4()}.json"
                    
                    upload_json_to_minio('raw-data', object_name, document)
                    total_scraped += 1
                    
                except Exception as inner_e:
                    logging.error(f"Failed to scrape or upload article at {article_url}: {inner_e}")
                    # Skip the failing article, do not crash the DAG
                    continue
                    
        except Exception as e:
            logging.error(f"Failed to fetch category page {category_url}: {e}")
            # Skip this category but continue if there are others
            continue
            
    logging.info(f"Batch scraping completed. Successfully scraped and uploaded {total_scraped} articles.")

# Define the DAG
with DAG(
    dag_id='batch_scraper_ingestion',
    default_args=default_args,
    description='Hourly batch scraper for extracting raw news articles into the Data Lake (Bronze)',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['ingestion', 'bronze', 'hespress']
) as dag:
    
    scrape_to_minio_task = PythonOperator(
        task_id='scrape_and_upload_to_minio',
        python_callable=scrape_articles_task,
    )

    # If we had more tasks (e.g., triggering a Spark job), we would link them here:
    # scrape_to_minio_task >> trigger_silver_processing_task
