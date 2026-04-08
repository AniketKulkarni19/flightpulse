-- ============================================================
-- FlightPulse: Populate Dimension Tables
-- ============================================================

-- ── dim_date ──
INSERT INTO analytics.dim_date
SELECT DISTINCT
    date_key,
    flight_date AS full_date,
    year,
    quarter,
    month,
    month_name,
    dayofmonth AS day_of_month,
    dayofweek AS day_of_week,
    day_name,
    is_weekend,
    EXTRACT(WEEK FROM flight_date) AS week_of_year
FROM analytics.fact_flights
WHERE date_key IS NOT NULL
ORDER BY date_key;

-- ── dim_carrier ──
INSERT INTO analytics.dim_carrier
SELECT DISTINCT carrier_code, NULL as carrier_name
FROM analytics.fact_flights
WHERE carrier_code IS NOT NULL;

-- Update with airline names
UPDATE analytics.dim_carrier SET carrier_name = 'American Airlines' WHERE carrier_code = 'AA';
UPDATE analytics.dim_carrier SET carrier_name = 'Alaska Airlines' WHERE carrier_code = 'AS';
UPDATE analytics.dim_carrier SET carrier_name = 'JetBlue Airways' WHERE carrier_code = 'B6';
UPDATE analytics.dim_carrier SET carrier_name = 'Delta Air Lines' WHERE carrier_code = 'DL';
UPDATE analytics.dim_carrier SET carrier_name = 'Frontier Airlines' WHERE carrier_code = 'F9';
UPDATE analytics.dim_carrier SET carrier_name = 'Allegiant Air' WHERE carrier_code = 'G4';
UPDATE analytics.dim_carrier SET carrier_name = 'Hawaiian Airlines' WHERE carrier_code = 'HA';
UPDATE analytics.dim_carrier SET carrier_name = 'Spirit Airlines' WHERE carrier_code = 'NK';
UPDATE analytics.dim_carrier SET carrier_name = 'Southwest Airlines' WHERE carrier_code = 'WN';
UPDATE analytics.dim_carrier SET carrier_name = 'United Airlines' WHERE carrier_code = 'UA';
UPDATE analytics.dim_carrier SET carrier_name = 'SkyWest Airlines' WHERE carrier_code = 'OO';
UPDATE analytics.dim_carrier SET carrier_name = 'Republic Airways' WHERE carrier_code = 'YX';
UPDATE analytics.dim_carrier SET carrier_name = 'PSA Airlines' WHERE carrier_code = 'OH';
UPDATE analytics.dim_carrier SET carrier_name = 'Envoy Air' WHERE carrier_code = 'MQ';
UPDATE analytics.dim_carrier SET carrier_name = 'Endeavor Air' WHERE carrier_code = '9E';
UPDATE analytics.dim_carrier SET carrier_name = 'Mesa Airlines' WHERE carrier_code = 'YV';
UPDATE analytics.dim_carrier SET carrier_name = 'Horizon Air' WHERE carrier_code = 'QX';
UPDATE analytics.dim_carrier SET carrier_name = 'Piedmont Airlines' WHERE carrier_code = 'PT';
UPDATE analytics.dim_carrier SET carrier_name = 'Breeze Airways' WHERE carrier_code = 'MX';
UPDATE analytics.dim_carrier SET carrier_name = 'Avelo Airlines' WHERE carrier_code = 'XP';

-- ── dim_airport ──
INSERT INTO analytics.dim_airport
SELECT DISTINCT
    origin AS airport_code,
    NULL AS airport_name,
    SPLIT_PART(origin_city, ', ', 1) AS city,
    origin_state AS state
FROM analytics.fact_flights
WHERE origin IS NOT NULL;

-- Add any destination-only airports not already captured
INSERT INTO analytics.dim_airport
SELECT DISTINCT
    dest AS airport_code,
    NULL AS airport_name,
    SPLIT_PART(dest_city, ', ', 1) AS city,
    dest_state AS state
FROM analytics.fact_flights
WHERE dest IS NOT NULL
  AND dest NOT IN (SELECT airport_code FROM analytics.dim_airport);

-- Verify
SELECT 'dim_date' as tbl, COUNT(*) as rows FROM analytics.dim_date
UNION ALL
SELECT 'dim_carrier', COUNT(*) FROM analytics.dim_carrier
UNION ALL
SELECT 'dim_airport', COUNT(*) FROM analytics.dim_airport;
