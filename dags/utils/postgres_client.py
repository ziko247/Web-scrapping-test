import os
import logging
import psycopg2
from psycopg2.extras import execute_values


def get_pg_connection():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "postgres"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DATABASE", "data_warehouse"),
        user=os.getenv("PG_USER", "airflow"),
        password=os.getenv("PG_PASSWORD", "airflow"),
    )


def init_db():
    """
    Ensures the gold_articles table and analytical views exist.
    Safe to call on every DAG run.
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS gold_articles (
        id           SERIAL PRIMARY KEY,
        source       VARCHAR(100),
        country      VARCHAR(100),
        url          TEXT UNIQUE NOT NULL,
        title        TEXT,
        author       VARCHAR(255),
        category     VARCHAR(100),
        language     VARCHAR(20),
        published_at VARCHAR(255),
        scraped_at   VARCHAR(255),
        word_count   INTEGER,
        content      TEXT,
        loaded_at    TIMESTAMP DEFAULT NOW()
    );
    """

    # Analytical views for the Gold layer
    create_by_day_sql = """
    CREATE OR REPLACE VIEW articles_by_day AS
    SELECT DATE(loaded_at) AS day, COUNT(*) AS article_count
    FROM gold_articles
    GROUP BY DATE(loaded_at)
    ORDER BY day DESC;
    """

    create_by_source_sql = """
    CREATE OR REPLACE VIEW articles_by_source AS
    SELECT source, COUNT(*) AS article_count, AVG(word_count)::INT AS avg_word_count
    FROM gold_articles
    GROUP BY source
    ORDER BY article_count DESC;
    """

    create_by_category_sql = """
    CREATE OR REPLACE VIEW articles_by_category AS
    SELECT category, COUNT(*) AS article_count
    FROM gold_articles
    GROUP BY category
    ORDER BY article_count DESC;
    """

    create_by_country_sql = """
    CREATE OR REPLACE VIEW articles_by_country AS
    SELECT country, COUNT(*) AS article_count
    FROM gold_articles
    GROUP BY country
    ORDER BY article_count DESC;
    """

    create_quality_logs_sql = """
    CREATE TABLE IF NOT EXISTS quality_logs (
        id           SERIAL PRIMARY KEY,
        article_url  TEXT,
        source       VARCHAR(100),
        dimension    VARCHAR(50), -- Completeness, Validity, Coherence
        test_name    VARCHAR(100), -- MISSING_TITLE, CONTENT_TOO_SHORT, etc.
        status       VARCHAR(20),  -- PASS, FAIL
        check_date   TIMESTAMP DEFAULT NOW()
    );
    """

    conn = None
    try:
        conn = get_pg_connection()
        with conn.cursor() as cur:
            cur.execute(create_table_sql)
            cur.execute(create_by_day_sql)
            cur.execute(create_by_source_sql)
            cur.execute(create_by_category_sql)
            cur.execute(create_by_country_sql)
            cur.execute(create_quality_logs_sql)
        conn.commit()
        logging.info("Gold layer table, views, and quality logs are ready.")
    except Exception as e:
        logging.error(f"Failed to initialize database: {e}")
        raise
    finally:
        if conn:
            conn.close()


def log_quality_check(checks: list):
    """
    Saves a batch of quality audit results.
    Each check: (url, source, dimension, test_name, status)
    """
    if not checks:
        return
    
    insert_sql = """
    INSERT INTO quality_logs (article_url, source, dimension, test_name, status)
    VALUES %s
    """
    conn = None
    try:
        conn = get_pg_connection()
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, checks)
        conn.commit()
    except Exception as e:
        logging.error(f"Failed to log quality checks: {e}")
    finally:
        if conn:
            conn.close()


def upsert_articles(articles: list):
    """
    Batch-upserts articles into gold_articles.
    ON CONFLICT (url) ensures idempotency across reruns.
    """
    if not articles:
        logging.info("No articles to insert.")
        return

    upsert_sql = """
    INSERT INTO gold_articles
        (source, country, url, title, author, category, language, published_at, scraped_at, word_count, content)
    VALUES %s
    ON CONFLICT (url) DO UPDATE SET
        title        = EXCLUDED.title,
        author       = EXCLUDED.author,
        category     = EXCLUDED.category,
        language     = EXCLUDED.language,
        country      = EXCLUDED.country,
        word_count   = EXCLUDED.word_count,
        content      = EXCLUDED.content,
        loaded_at    = NOW();
    """
    rows = [
        (
            a.get("source"),
            a.get("country", "Unknown"),
            a.get("url"),
            a.get("title"),
            a.get("author", "Unknown"),
            a.get("category", "general"),
            a.get("language", "unknown"),
            a.get("published_at"),
            a.get("scraped_at"),
            a.get("word_count", 0),
            a.get("content"),
        )
        for a in articles
    ]

    conn = None
    try:
        conn = get_pg_connection()
        with conn.cursor() as cur:
            execute_values(cur, upsert_sql, rows)
        conn.commit()
        logging.info(f"Successfully upserted {len(rows)} articles into gold_articles.")
    except Exception as e:
        logging.error(f"Failed to upsert articles: {e}")
        raise
    finally:
        if conn:
            conn.close()
