-- ============================================================
-- FlightPulse: Athena Validation Queries
-- Run these against the raw Glue Catalogue table to verify
-- data integrity before running the ETL job.
-- ============================================================

-- 1. Row count by year (verify all years loaded)
SELECT year, COUNT(*) as flight_count
FROM "flightpulse_db"."raw_flightpulse_raw_ak19"
GROUP BY year
ORDER BY year;

-- 2. Null analysis on key delay columns
SELECT 
    COUNT(*) as total_rows,
    SUM(CASE WHEN depdelay IS NULL THEN 1 ELSE 0 END) as null_dep_delay,
    SUM(CASE WHEN arrdelay IS NULL THEN 1 ELSE 0 END) as null_arr_delay,
    SUM(CASE WHEN cancelled IS NULL THEN 1 ELSE 0 END) as null_cancelled,
    SUM(CASE WHEN distance IS NULL THEN 1 ELSE 0 END) as null_distance
FROM "flightpulse_db"."raw_flightpulse_raw_ak19";

-- 3. Distinct carriers per year
SELECT year, COUNT(DISTINCT reporting_airline) as carrier_count
FROM "flightpulse_db"."raw_flightpulse_raw_ak19"
GROUP BY year
ORDER BY year;

-- 4. Monthly row distribution (check for missing months)
SELECT year, month, COUNT(*) as flights
FROM "flightpulse_db"."raw_flightpulse_raw_ak19"
GROUP BY year, month
ORDER BY year, month;
