import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_trunc, count, avg, round as spark_round

def create_spark_session():
    #create spark session with s3a configuration for minio
    spark = SparkSession.builder \
        .appName("nyc-taxi-aggregate") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    
    return spark

def aggregate_data(spark, year_month):
    #input and output paths
    input_path = f"s3a://nyc-taxi-cleaned/{year_month}.parquet"
    output_path = f"s3a://nyc-taxi-aggregated/{year_month}.parquet"
    
    print(f"reading cleaned data from {input_path}")
    
    #read cleaned parquet
    df = spark.read.parquet(input_path)
    
    print(f"cleaned data: {df.count()} rows")
    
    #truncate pickup datetime to hour
    df_hourly = df.withColumn(
        "pickup_hour",
        date_trunc("hour", col("tpep_pickup_datetime"))
    )
    
    #aggregate by zone and hour
    df_agg = df_hourly.groupBy("PULocationID", "pickup_hour").agg(
        count("*").alias("trip_count"),
        spark_round(avg("fare_amount"), 2).alias("avg_fare_amount"),
        spark_round(avg("trip_distance"), 2).alias("avg_trip_distance"),
        spark_round(avg("trip_duration_minutes"), 2).alias("avg_trip_duration")
    )
    
    #rename columns for clarity
    df_agg = df_agg.withColumnRenamed("PULocationID", "zone_id") \
                   .withColumnRenamed("pickup_hour", "pickup_datetime")
    
    #sort by datetime and zone for readability
    df_agg = df_agg.orderBy("pickup_datetime", "zone_id")
    
    print(f"aggregated data: {df_agg.count()} rows")
    
    #write to minio
    print(f"writing aggregated data to {output_path}")
    
    df_agg.coalesce(1).write \
        .mode("overwrite") \
        .parquet(output_path)
    
    print(f"aggregation complete for {year_month}")
    
    return True

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: spark-submit aggregate_zones.py YYYY-MM")
        sys.exit(1)
    
    year_month = sys.argv[1]
    
    spark = create_spark_session()
    
    try:
        aggregate_data(spark, year_month)
        print(f"SUCCESS: {year_month} aggregated")
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()