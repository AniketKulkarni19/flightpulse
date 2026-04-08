FlightPulse: US Airline Performance Analytics
Platform
An end-to-end cloud data pipeline and analytics platform built on AWS, analyzing 33
million+ domestic flight records (2021–2025) to uncover airline performance trends,
delay root causes, and operational patterns during the post-COVID recovery era.
Architecture
BTS CSV Files ──► Amazon S3 (Raw)
│
▼
AWS Glue Crawler ──► Glue Data Catalog
│
▼
Amazon Athena (Data Validation & Exploration)
│
▼
AWS Glue ETL Job (PySpark)
• Data cleansing & type casting
• Null handling & deduplication
• Derived metrics (7 new columns)
• CSV → Parquet conversion (80% compression)
│
▼
Amazon S3 (Curated / Parquet)
│
▼
Amazon Redshift Serverless
• Star schema (fact + 3 dimensions)
• 4 materialized views for KPIs
• 33M+ rows loaded via COPY
│
▼
Amazon QuickSight
• 3 interactive dashboards
• Executive KPIs, delay analysis, operations
Dashboards
Dashboard 1: Executive Summary
KPI cards (total flights, on-time %, avg delay, cancellation rate), monthly on-time trend with
year-over-year comparison, carrier performance rankings, and day-of-week delay patterns.
See quicksight/dashboard_executive_summary.pdf
Dashboard 2: Delay Root Cause Analysis
Delay cause breakdown by carrier (stacked bar), primary delay cause distribution (donut
chart), delay severity categories, and monthly delay trends by cause type.
See quicksight/dashboard_delay_analysis.pdf
Dashboard 3: Operational Deep Dive
Route performance summary table, day-of-week × departure hour delay heatmap, top 20
busiest routes, and on-time performance by state.
See quicksight/dashboard_operational_deepdive.pdf
Data Model
Star Schema designed for analytical query performance:
Fact Table: analytics.fact_flights (~33M rows)
Flight-level detail: delays, cancellations, diversions, timestamps
7 derived metrics: is_on_time , delay_category , primary_delay_cause , day_name ,
is_weekend , dep_hour , route
Dimension Tables:
analytics.dim_date — calendar attributes (day name, weekend flag, week of year)
analytics.dim_carrier — airline code to full name mapping
analytics.dim_airport — airport code, city, and state
Materialized Views (pre-aggregated KPIs):
mv_daily_carrier_performance — daily metrics by carrier
mv_route_performance — route-level aggregations by year
mv_monthly_delay_causes — delay cause breakdown by month/carrier
mv_hourly_heatmap — day-of-week × hour delay patterns
Tech Stack
Layer Service Purpose
Storage Amazon S3 Raw (CSV) and curated (Parquet) data lake
Cataloging AWS Glue Crawler
Automated schema detection and
partitioning
Exploration Amazon Athena Serverless SQL on S3 for validation and EDA
Transformation AWS Glue ETL (PySpark)
Data cleansing, enrichment, format
conversion
Warehousing
Amazon Redshift
Serverless
Star schema, COPY loading, materialized
views
Visualization Amazon QuickSight Interactive dashboards with SPICE caching
Orchestration AWS IAM Role-based access across services
Repository Structure
flightpulse/
├── README.md
├── glue/
│ └── flightpulse_etl.py # PySpark ETL job
├── athena/
│ ├── 01_validation_queries.sql # Data quality checks
│ └── 02_exploratory_analysis.sql # EDA queries
├── redshift/
│ ├── 01_create_schema_and_tables.sql # Star schema DDL
│ ├── 02_copy_and_load.sql # S3 → Redshift loading
│ ├── 03_populate_dimensions.sql # Dimension table population
│ └── 04_materialized_views.sql # Pre-aggregated KPI views
├── quicksight/
│ ├── dashboard_executive_summary.pdf
│ ├── dashboard_delay_analysis.pdf
│ └── dashboard_operational_deepdive.pdf
└── docs/
└── architecture_diagram.png # (optional) visual diagram
Key Findings
On-time performance improved steadily from 2021 (COVID recovery) through 2024,
with seasonal dips in summer and winter holidays
Late Aircraft delays (cascading failures) are the #1 delay cause, followed by NAS (air
traffic control) and Carrier delays
Fridays and Sundays have the worst on-time performance; Tuesdays and Wednesdays
are the best days to fly
Evening departures (after 5 PM) experience significantly higher delays than morning
flights
The busiest routes (ATL-ORD, LAX-SFO, etc.) don’t necessarily have the worst delays —
smaller regional routes often perform worse
How to Reproduce
1. Download BTS On-Time Performance data (2021-2025) from transtats.bts.gov
2. Upload CSVs to S3 with year=YYYY/ Hive-style partitioning
3. Run Glue Crawler to catalog the data
4. Execute the Glue ETL job ( glue/flightpulse_etl.py )
5. Create Redshift Serverless workspace and run SQL scripts in order
6. Connect QuickSight to Redshift and build dashboards
AWS Cost
Total project cost: ~$15-25 (covered by AWS free tier credits)
Service Cost
S3 Storage ~$2-3
Glue ETL ~$5-8
Athena Queries ~$0.50
Redshift Serverless ~$5-10
QuickSight Free (30-day trial)
Author
Aniket Kulkarni
GitHub: @AniketKulkarni19
LinkedIn: Connect with me
License
This project is for portfolio and educational purposes. Flight data is sourced from the U.S.
Bureau of Transportation Statistics (public domain).
