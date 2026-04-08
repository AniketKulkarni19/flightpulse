-- ============================================================
-- FlightPulse: Data Loading (S3 → Redshift)
-- ============================================================

-- Load fact table from curated Parquet in S3
COPY analytics.fact_flights
FROM 's3://flightpulse-curated-ak19/fact_flights/'
IAM_ROLE 'arn:aws:iam::795644302588:role/service-role/AmazonRedshift-CommandsAccessRole-20260408T075135'
FORMAT AS PARQUET;

-- Derive year and month from flight_date
-- (Parquet partition columns excluded from COPY)
UPDATE analytics.fact_flights
SET year = EXTRACT(YEAR FROM flight_date),
    month = EXTRACT(MONTH FROM flight_date);

-- Verify load
SELECT year, COUNT(*) as flights
FROM analytics.fact_flights
GROUP BY year
ORDER BY year;
