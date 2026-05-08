# 🌐 Press Analytics - Medallion Architecture

A professional Big Data platform designed to collect, transform, and analyze news articles automatically from multiple sources.

## 🚀 Architecture Overview
This project follows a **Medallion Architecture** implemented using Docker, Airflow, MinIO, and PostgreSQL.

- **Bronze Layer**: Raw ingestion of articles (HTML + Metadata) from **Hespress** and **CNN**.
- **Silver Layer**: Data cleaning, HTML stripping, text normalization, and **language detection** using `langdetect`.
- **Gold Layer**: Analytical warehouse in PostgreSQL with metrics (word count) and pre-aggregated views.

## 🛠️ Technology Stack
- **Orchestration**: Apache Airflow
- **Data Lake**: MinIO (S3 Compatible)
- **Data Warehouse**: PostgreSQL
- **Processing**: Python (BeautifulSoup, Pandas, Langdetect)
- **Visualization**: Streamlit & Plotly

## 📦 Getting Started

### 1. Start the Infrastructure
Ensure Docker is running and execute:
```bash
docker-compose up -d
```

### 2. Run the Pipelines
Access the Airflow UI at `http://localhost:8080` (Credentials: `admin` / `admin`) and trigger the DAGs in order:
1. `batch_scraper_ingestion` (Bronze)
2. `silver_cleaning_processing` (Silver)
3. `gold_warehouse_loading` (Gold)

### 3. Launch the Dashboard
Ensure your local virtual environment is active and the requirements are installed:
```bash
.\venv\Scripts\streamlit run dashboard.py
```

## 📈 Key Features
- **Multi-Source Support**: Automated scraping logic for both local and international news.
- **Data Quality Control**: Automated validation task that quarantines articles with missing titles, dates, or short content.
- **Deduplication**: Intelligent upsert logic in the Gold layer to handle duplicate URLs gracefully.
- **Analytical Views**: Pre-computed SQL views for fast reporting on news trends.

---
**Date Limite**: 10 Mai 2026 à Minuit.
**Filière**: EMSI - IADATA 2025/2026
