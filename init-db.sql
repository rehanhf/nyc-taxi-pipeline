-- 1. Create databases (Safe Fix for Postgres Error)
CREATE DATABASE airflow;
CREATE DATABASE metabase;

-- 2. Create tables for aggregated data in the default DB
CREATE TABLE IF NOT EXISTS aggregated_zones (
    zone_id INTEGER,
    pickup_datetime TIMESTAMP,
    trip_count INTEGER,
    avg_fare_amount NUMERIC(10,2),
    avg_trip_distance NUMERIC(10,2),
    avg_trip_duration INTEGER
);

CREATE INDEX idx_zones_datetime ON aggregated_zones(pickup_datetime);
CREATE INDEX idx_zones_zone ON aggregated_zones(zone_id);