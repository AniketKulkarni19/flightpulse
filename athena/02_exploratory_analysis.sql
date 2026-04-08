-- ============================================================
-- FlightPulse: Athena Exploratory Analysis
-- Ad-hoc queries run on curated Parquet data for the dashboard
-- design and insight discovery.
-- ============================================================

-- 1. On-time percentage by carrier (2023)
SELECT 
    carrier_code,
    COUNT(*) as total_flights,
    SUM(CASE WHEN is_on_time = true THEN 1 ELSE 0 END) as on_time,
    ROUND(100.0 * SUM(CASE WHEN is_on_time = true THEN 1 ELSE 0 END) / COUNT(*), 1) as on_time_pct
FROM "flightpulse_db"."curated_fact_flights"
WHERE year = '2023'
GROUP BY carrier_code
ORDER BY on_time_pct DESC;

-- 2. Delay category distribution
SELECT 
    delay_category,
    COUNT(*) as flights,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) as pct
FROM "flightpulse_db"."curated_fact_flights"
WHERE year = '2023'
GROUP BY delay_category
ORDER BY flights DESC;

-- 3. Primary delay causes a breakdown
SELECT 
    primary_delay_cause,
    COUNT(*) as flights,
    ROUND(AVG(arr_delay_minutes), 1) as avg_delay_min
FROM "flightpulse_db"."curated_fact_flights"
WHERE year = '2023' AND arr_delay_minutes > 0
GROUP BY primary_delay_cause
ORDER BY flights DESC;

-- 4. Top 10 busiest routes
SELECT origin, dest, COUNT(*) as flights
FROM "flightpulse_db"."curated_fact_flights"
WHERE year = '2023'
GROUP BY origin, dest
ORDER BY flights DESC
LIMIT 10;

-- 5. Parquet vs CSV performance comparison
-- Run the same query on both tables and compare "Data scanned"
SELECT carrier_code, AVG(arr_delay_minutes) as avg_delay
FROM "flightpulse_db"."curated_fact_flights"
WHERE year = '2023'
GROUP BY carrier_code;
