-- ============================================================
-- FlightPulse: Redshift Schema & Table Definitions
-- Star schema design for analytical query performance
-- ============================================================

CREATE SCHEMA IF NOT EXISTS analytics;

-- ============================================================
-- DIMENSION TABLES
-- ============================================================

-- dim_date: Calendar dimension (DISTSTYLE ALL for fast joins)
CREATE TABLE analytics.dim_date (
    date_key        INT PRIMARY KEY,
    full_date       DATE NOT NULL,
    year            SMALLINT NOT NULL,
    quarter         SMALLINT NOT NULL,
    month           SMALLINT NOT NULL,
    month_name      VARCHAR(10),
    day_of_month    SMALLINT,
    day_of_week     SMALLINT,
    day_name        VARCHAR(10),
    is_weekend      BOOLEAN,
    week_of_year    SMALLINT
)
DISTSTYLE ALL
SORTKEY (full_date);

-- dim_carrier: Airline lookup
CREATE TABLE analytics.dim_carrier (
    carrier_code    VARCHAR(5) PRIMARY KEY,
    carrier_name    VARCHAR(100)
)
DISTSTYLE ALL;

-- dim_airport: Airport lookup
CREATE TABLE analytics.dim_airport (
    airport_code    VARCHAR(5) PRIMARY KEY,
    airport_name    VARCHAR(200),
    city            VARCHAR(100),
    state           VARCHAR(50)
)
DISTSTYLE ALL;

-- ============================================================
-- FACT TABLE
-- ============================================================

-- fact_flights: 33M+ flight records
-- DISTKEY on carrier_code optimizes carrier-level aggregations
-- SORTKEY on date_key + origin optimizes date range and airport filters
CREATE TABLE analytics.fact_flights (
    quarter                 INT,
    dayofmonth              INT,
    dayofweek               INT,
    carrier_code            VARCHAR(10),
    origin                  VARCHAR(10),
    origin_city             VARCHAR(200),
    origin_state            VARCHAR(100),
    dest                    VARCHAR(10),
    dest_city               VARCHAR(200),
    dest_state              VARCHAR(100),
    scheduled_dep_time      BIGINT,
    actual_dep_time         BIGINT,
    dep_delay               FLOAT,
    dep_delay_minutes       FLOAT,
    dep_delayed_15          DOUBLE PRECISION,
    scheduled_arr_time      BIGINT,
    actual_arr_time         BIGINT,
    arr_delay               FLOAT,
    arr_delay_minutes       FLOAT,
    arr_delayed_15          DOUBLE PRECISION,
    cancellation_code       VARCHAR(10),
    scheduled_elapsed_time  FLOAT,
    actual_elapsed_time     FLOAT,
    air_time                FLOAT,
    distance                INT,
    carrier_delay           FLOAT,
    weather_delay           FLOAT,
    nas_delay               FLOAT,
    security_delay          FLOAT,
    late_aircraft_delay     FLOAT,
    flight_date             DATE,
    is_on_time              BOOLEAN,
    is_cancelled            BOOLEAN,
    is_diverted             BOOLEAN,
    delay_category          VARCHAR(100),
    primary_delay_cause     VARCHAR(100),
    day_name                VARCHAR(20),
    is_weekend              BOOLEAN,
    dep_hour                INT,
    route                   VARCHAR(100),
    total_delay_minutes     FLOAT,
    date_key                INT,
    month_name              VARCHAR(20)
)
DISTSTYLE KEY DISTKEY (carrier_code)
COMPOUND SORTKEY (date_key, origin);

-- Add year and month columns (derived post-load since
-- Parquet partition columns are not included in COPY)
ALTER TABLE analytics.fact_flights ADD COLUMN year INT;
ALTER TABLE analytics.fact_flights ADD COLUMN month INT;
