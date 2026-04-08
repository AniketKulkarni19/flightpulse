-- ============================================================
-- FlightPulse: Materialized Views (Pre-Aggregated KPIs)
-- These power the QuickSight dashboards with fast query times
-- ============================================================

-- MV 1: Daily Performance by Carrier
-- Used by: Executive Summary dashboard
CREATE MATERIALIZED VIEW analytics.mv_daily_carrier_performance AS
SELECT
    f.flight_date,
    f.year,
    f.month,
    f.month_name,
    f.quarter,
    f.day_name,
    f.is_weekend,
    f.carrier_code,
    c.carrier_name,
    COUNT(*) AS total_flights,
    SUM(CASE WHEN f.is_on_time THEN 1 ELSE 0 END) AS on_time_flights,
    ROUND(100.0 * SUM(CASE WHEN f.is_on_time THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS on_time_pct,
    ROUND(AVG(f.arr_delay_minutes), 2) AS avg_arr_delay,
    ROUND(AVG(f.dep_delay_minutes), 2) AS avg_dep_delay,
    SUM(CASE WHEN f.is_cancelled THEN 1 ELSE 0 END) AS cancellations,
    ROUND(100.0 * SUM(CASE WHEN f.is_cancelled THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS cancellation_rate,
    SUM(f.carrier_delay) AS total_carrier_delay,
    SUM(f.weather_delay) AS total_weather_delay,
    SUM(f.nas_delay) AS total_nas_delay,
    SUM(f.security_delay) AS total_security_delay,
    SUM(f.late_aircraft_delay) AS total_late_aircraft_delay
FROM analytics.fact_flights f
LEFT JOIN analytics.dim_carrier c ON f.carrier_code = c.carrier_code
GROUP BY 1,2,3,4,5,6,7,8,9;

-- MV 2: Route Performance Summary
-- Used by: Operational Deep Dive dashboard
CREATE MATERIALIZED VIEW analytics.mv_route_performance AS
SELECT
    f.route,
    f.origin,
    f.dest,
    f.origin_city,
    f.origin_state,
    f.dest_city,
    f.dest_state,
    f.year,
    COUNT(*) AS total_flights,
    ROUND(AVG(f.arr_delay_minutes), 2) AS avg_arr_delay,
    ROUND(AVG(f.distance), 0) AS avg_distance,
    SUM(CASE WHEN f.is_on_time THEN 1 ELSE 0 END) AS on_time_flights,
    ROUND(100.0 * SUM(CASE WHEN f.is_on_time THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS on_time_pct,
    SUM(CASE WHEN f.is_cancelled THEN 1 ELSE 0 END) AS cancellations,
    SUM(f.total_delay_minutes) AS total_delay_minutes
FROM analytics.fact_flights f
GROUP BY 1,2,3,4,5,6,7,8;

-- MV 3: Monthly Delay Cause Breakdown
-- Used by: Delay Root Cause Analysis dashboard
CREATE MATERIALIZED VIEW analytics.mv_monthly_delay_causes AS
SELECT
    f.year,
    f.month,
    f.month_name,
    f.carrier_code,
    c.carrier_name,
    f.primary_delay_cause,
    f.delay_category,
    COUNT(*) AS flight_count,
    ROUND(AVG(f.arr_delay_minutes), 2) AS avg_delay,
    SUM(f.carrier_delay) AS carrier_delay_total,
    SUM(f.weather_delay) AS weather_delay_total,
    SUM(f.nas_delay) AS nas_delay_total,
    SUM(f.security_delay) AS security_delay_total,
    SUM(f.late_aircraft_delay) AS late_aircraft_delay_total
FROM analytics.fact_flights f
LEFT JOIN analytics.dim_carrier c ON f.carrier_code = c.carrier_code
GROUP BY 1,2,3,4,5,6,7;

-- MV 4: Hourly Heatmap Data
-- Used by: Operational Deep Dive dashboard (heatmap visual)
CREATE MATERIALIZED VIEW analytics.mv_hourly_heatmap AS
SELECT
    f.dayofweek,
    f.day_name,
    f.dep_hour,
    f.year,
    COUNT(*) AS total_flights,
    ROUND(AVG(f.arr_delay_minutes), 2) AS avg_delay,
    ROUND(100.0 * SUM(CASE WHEN f.is_on_time THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS on_time_pct
FROM analytics.fact_flights f
GROUP BY 1,2,3,4;

-- Verify all objects
SELECT table_schema, table_name, table_type
FROM information_schema.tables
WHERE table_schema = 'analytics'
ORDER BY table_type, table_name;
