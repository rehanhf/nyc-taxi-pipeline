import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, round as spark_round
from pyspark.sql.types import TimestampType

def create_spark_session():
    #create spark session with s3a configuration for minio
    spark = SparkSession.builder \
        .appName("nyc-taxi-transform") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    
    return spark

def transform_data(spark, year_month):
    #input and output paths
    input_path = f"s3a://nyc-taxi-raw/{year_month}.parquet"
    output_path = f"s3a://nyc-taxi-cleaned/{year_month}.parquet"
    
    print(f"reading raw data from {input_path}")
    
    #read raw parquet
    df = spark.read.parquet(input_path)
    
    print(f"raw data: {df.count()} rows")
    
    #select only necessary columns
    df_selected = df.drop(
    "store_and_fwd_flag",
    "extra",
    "mta_tax",
    "improvement_surcharge",
    "congestion_surcharge",
    "airport_fee"
)
    
    #filter invalid data
    df_clean = df_selected.filter(
        (col("tpep_pickup_datetime").isNotNull()) &
        (col("tpep_dropoff_datetime").isNotNull()) &
        (col("PULocationID").isNotNull()) &
        (col("DOLocationID").isNotNull()) &
        (col("passenger_count") > 0) &
        (col("passenger_count") <= 6) &
        (col("trip_distance") > 0) &
        (col("trip_distance") < 100) &
        (col("fare_amount") > 0) &
        (col("fare_amount") < 500) &
        (col("total_amount") > 0) &
        (col("total_amount") < 500) &
        (col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))
    )
    
    #calculate trip duration in minutes
    df_clean = df_clean.withColumn(
        "trip_duration_minutes",
        spark_round(
            (unix_timestamp(col("tpep_dropoff_datetime")) - 
             unix_timestamp(col("tpep_pickup_datetime"))) / 60,
            2
        )
    )
    
    #filter unrealistic trip durations (less than 1 min or more than 3 hours)
    df_clean = df_clean.filter(
        (col("trip_duration_minutes") >= 1) &
        (col("trip_duration_minutes") <= 180)
    )
    
    print(f"cleaned data: {df_clean.count()} rows")
    
    #write to minio
    print(f"writing cleaned data to {output_path}")
    
    df_clean.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    print(f"transform complete for {year_month}")
    
    return True

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: spark-submit transform_taxi.py YYYY-MM")
        sys.exit(1)
    
    year_month = sys.argv[1]
    
    spark = create_spark_session()
    
    try:
        transform_data(spark, year_month)
        print(f"SUCCESS: {year_month} transformed")
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()