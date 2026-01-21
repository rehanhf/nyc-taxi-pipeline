# NYC Taxi Batch Analytics Pipeline

## Architecture
**Airflow** (Orchestrator) $\to$ **MinIO** (Data Lake) $\to$ **Spark** (Processing) $\to$ **Postgres** (Warehouse) $\to$ **Metabase** (Viz).

## Infrastructure (Docker)
*   **Custom Airflow Image:** Built on `apache/airflow:2.8.1`, patched with OpenJDK 17 and Spark Providers.
*   **Spark Cluster:** One Master, One Worker (2GB RAM).
*   **MinIO:** S3-compatible storage for Raw, Cleaned, and Aggregated layers.

## Key Features
*   **Bypassed Anti-Bot:** Custom request headers to scrape CloudFront-protected Parquet files.
*   **Distributed Transformation:** PySpark cleans nulls, calculates trip duration, and filters outliers.
*   **Geospatial Aggregation:** Aggregates ~100M rows into zone-based metrics.
*   **Visualization:** Metabase Heatmap using Custom GeoJSON hosted on MinIO.

## Windows Setup (Critical)
If running on Windows Docker Desktop, you must configure `.wslconfig`:
```ini
[wsl2]
appendWindowsPath=false
Then run:
code
Bash
docker-compose up -d