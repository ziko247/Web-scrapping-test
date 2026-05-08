-- Create a separate database for our data warehouse
CREATE DATABASE data_warehouse;

-- Connect to it and create the Gold Layer table
\connect data_warehouse;

CREATE TABLE IF NOT EXISTS gold_articles (
    id          SERIAL PRIMARY KEY,
    source      VARCHAR(100),
    url         TEXT UNIQUE NOT NULL,       -- UNIQUE enforces idempotency (no duplicates)
    title       TEXT,
    published_at VARCHAR(255),
    scraped_at  VARCHAR(255),
    word_count  INTEGER,
    content     TEXT,
    loaded_at   TIMESTAMP DEFAULT NOW()
);
