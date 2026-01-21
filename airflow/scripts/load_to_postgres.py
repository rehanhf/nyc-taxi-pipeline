import sys
import os
import duckdb

def load_to_postgres(year_month):
    #minio configuration
    minio_endpoint = os.getenv('MINIO_ENDPOINT', 'minio:9000')
    minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
    minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin123')
    
    #postgres configuration
    postgres_host = os.getenv('POSTGRES_HOST', 'postgres')
    postgres_port = os.getenv('POSTGRES_PORT', '5432')
    postgres_user = os.getenv('POSTGRES_USER', 'airflow')
    postgres_password = os.getenv('POSTGRES_PASSWORD', 'airflow123')
    postgres_db = os.getenv('POSTGRES_DB', 'taxi_warehouse')
    
    #read actual parquet files
    s3_path = f"s3://nyc-taxi-aggregated/{year_month}.parquet/*.parquet"
    
    print(f"loading data from {s3_path}")
    
    #create duckdb connection
    conn = duckdb.connect()
    
    #configure s3 access
    conn.execute(f"SET s3_endpoint='{minio_endpoint}'")
    conn.execute(f"SET s3_access_key_id='{minio_access_key}'")
    conn.execute(f"SET s3_secret_access_key='{minio_secret_key}'")
    conn.execute("SET s3_use_ssl=false")
    conn.execute("SET s3_url_style='path'")
    
    #install and load postgres extension
    conn.execute("INSTALL postgres")
    conn.execute("LOAD postgres")
    
    #attach postgres database
    postgres_conn_string = f"host={postgres_host} port={postgres_port} dbname={postgres_db} user={postgres_user} password={postgres_password}"
    conn.execute(f"ATTACH '{postgres_conn_string}' AS pg (TYPE postgres)")
    
    #count existing rows before delete
    existing_count = conn.execute(f"""
        SELECT COUNT(*) 
        FROM pg.aggregated_zones 
        WHERE DATE_TRUNC('month', pickup_datetime) = DATE '{year_month}-01'
    """).fetchone()[0]
    
    print(f"found {existing_count} existing rows for {year_month}")
    
    #delete existing data for this month
    if existing_count > 0:
        print(f"deleting existing data for {year_month}")
        conn.execute(f"""
            DELETE FROM pg.aggregated_zones 
            WHERE DATE_TRUNC('month', pickup_datetime) = DATE '{year_month}-01'
        """)
        print(f"deleted {existing_count} rows")
    
    #insert new data from parquet
    print(f"inserting new data from parquet")
    conn.execute(f"""
        INSERT INTO pg.aggregated_zones 
        SELECT 
            zone_id,
            pickup_datetime,
            trip_count,
            avg_fare_amount,
            avg_trip_distance,
            avg_trip_duration
        FROM read_parquet('{s3_path}')
    """)
    
    #verify data in postgres
    verify_count = conn.execute(f"""
        SELECT COUNT(*) 
        FROM pg.aggregated_zones 
        WHERE DATE_TRUNC('month', pickup_datetime) = DATE '{year_month}-01'
    """).fetchone()[0]
    
    print(f"verification: {verify_count} rows now in postgres for {year_month}")
    
    conn.close()
    
    print(f"load complete for {year_month}")
    
    return True

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: python load_to_postgres.py YYYY-MM")
        sys.exit(1)
    
    year_month = sys.argv[1]
    
    try:
        load_to_postgres(year_month)
        print(f"SUCCESS: {year_month} loaded to postgres")
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)