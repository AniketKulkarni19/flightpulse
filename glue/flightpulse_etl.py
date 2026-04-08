import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
col, when, to_date, dayofweek, date_format, concat,
month as spark_month, year as spark_year, quarter,
hour, floor, lit, trim, upper, round as spark_round
)
from pyspark.sql.types import (
FloatType, IntegerType, BooleanType, StringType
)
# ============================================================
# CONFIGURATION
# ============================================================
RAW_DATABASE = “flightpulse_db”
RAW_TABLE = “raw_flightpulse_raw_ak19”
CURATED_S3_PATH = “s3://flightpulse-curated-ak19/fact_flights/”
# ============================================================
# JOB INITIALIZATION
# ============================================================
args = getResolvedOptions(sys.argv, [‘JOB_NAME’])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args[‘JOB_NAME’], args)
print(”=” * 60)
print(“FlightPulse ETL Job Started”)
print(”=” * 60)
# ============================================================
# STEP 1: READ RAW DATA FROM GLUE CATALOG
# ============================================================
print(“Step 1: Reading raw data from Glue Catalog…”)
raw_dyf = glueContext.create_dynamic_frame.from_catalog(
database=RAW_DATABASE,
table_name=RAW_TABLE
)
raw_df = raw_dyf.toDF()
raw_count = raw_df.count()
print(f”  Raw record count: {raw_count:,}”)
print(f”  Raw column count: {len(raw_df.columns)}”)
# ============================================================
# STEP 2: SELECT AND RENAME COLUMNS
# ============================================================
print(“Step 2: Selecting and renaming columns…”)
for c in raw_df.columns:
raw_df = raw_df.withColumnRenamed(c, c.strip().lower())
selected_df = raw_df.select(
col(“year”),
col(“quarter”),
col(“month”),
col(“dayofmonth”),
col(“dayofweek”),
col(“flightdate”),
col(“reporting_airline”).alias(“carrier_code”),
col(“origin”),
col(“origincityname”).alias(“origin_city”),
col(“originstate”).alias(“origin_state”),
col(“dest”),
col(“destcityname”).alias(“dest_city”),
col(“deststate”).alias(“dest_state”),
col(“crsdeptime”).alias(“scheduled_dep_time”),
col(“deptime”).alias(“actual_dep_time”),
col(“depdelay”).alias(“dep_delay”),
col(“depdelayminutes”).alias(“dep_delay_minutes”),
col(“depdel15”).alias(“dep_delayed_15”),
col(“crsarrtime”).alias(“scheduled_arr_time”),
col(“arrtime”).alias(“actual_arr_time”),
col(“arrdelay”).alias(“arr_delay”),
col(“arrdelayminutes”).alias(“arr_delay_minutes”),
col(“arrdel15”).alias(“arr_delayed_15”),
col(“cancelled”),
col(“cancellationcode”).alias(“cancellation_code”),
col(“diverted”),
col(“crselapsedtime”).alias(“scheduled_elapsed_time”),
col(“actualelapsedtime”).alias(“actual_elapsed_time”),
col(“airtime”).alias(“air_time”),
col(“distance”),
col(“carrierdelay”).alias(“carrier_delay”),
col(“weatherdelay”).alias(“weather_delay”),
col(“nasdelay”).alias(“nas_delay”),
col(“securitydelay”).alias(“security_delay”),
col(“lateaircraftdelay”).alias(“late_aircraft_delay”)
)
print(f”  Selected {len(selected_df.columns)} columns”)
# ============================================================
# STEP 3: CAST DATA TYPES
# ============================================================
print(“Step 3: Casting data types…”)
typed_df = (selected_df
.withColumn(“year”, col(“year”).cast(IntegerType()))
.withColumn(“quarter”, col(“quarter”).cast(IntegerType()))
.withColumn(“month”, col(“month”).cast(IntegerType()))
.withColumn(“dayofmonth”, col(“dayofmonth”).cast(IntegerType()))
.withColumn(“dayofweek”, col(“dayofweek”).cast(IntegerType()))
.withColumn(“flight_date”, to_date(col(“flightdate”), “yyyy-MM-dd”))
.withColumn(“dep_delay”, col(“dep_delay”).cast(FloatType()))
.withColumn(“dep_delay_minutes”, col(“dep_delay_minutes”).cast(FloatType()))
.withColumn(“arr_delay”, col(“arr_delay”).cast(FloatType()))
.withColumn(“arr_delay_minutes”, col(“arr_delay_minutes”).cast(FloatType()))
.withColumn(“carrier_delay”, col(“carrier_delay”).cast(FloatType()))
.withColumn(“weather_delay”, col(“weather_delay”).cast(FloatType()))
.withColumn(“nas_delay”, col(“nas_delay”).cast(FloatType()))
.withColumn(“security_delay”, col(“security_delay”).cast(FloatType()))
.withColumn(“late_aircraft_delay”, col(“late_aircraft_delay”).cast(FloatType()))
.withColumn(“distance”, col(“distance”).cast(IntegerType()))
.withColumn(“air_time”, col(“air_time”).cast(FloatType()))
.withColumn(“scheduled_elapsed_time”, col(“scheduled_elapsed_time”).cast(FloatType()))
.withColumn(“actual_elapsed_time”, col(“actual_elapsed_time”).cast(FloatType()))
.withColumn(“cancelled”, col(“cancelled”).cast(FloatType()))
.withColumn(“diverted”, col(“diverted”).cast(FloatType()))
)
# ============================================================
# STEP 4: HANDLE NULLS
# ============================================================
print(“Step 4: Handling null values…”)
cleaned_df = typed_df.fillna(0, subset=[
“carrier_delay”, “weather_delay”, “nas_delay”,
“security_delay”, “late_aircraft_delay”,
“dep_delay_minutes”, “arr_delay_minutes”
])
cleaned_df = cleaned_df.fillna(0, subset=[“cancelled”, “diverted”])
null_before = typed_df.filter(col(“carrier_delay”).isNull()).count()
null_after = cleaned_df.filter(col(“carrier_delay”).isNull()).count()
print(f”  Carrier delay nulls: {null_before:,} -> {null_after:,}”)
# ============================================================
# STEP 5: CREATE DERIVED METRICS
# ============================================================
print(“Step 5: Creating derived metrics…”)
enriched_df = (cleaned_df
.withColumn(“is_on_time”,
when(col(“arr_delay”) <= 15, True)
.otherwise(False))
.withColumn(“is_cancelled”,
when(col(“cancelled”) == 1.0, True)
.otherwise(False))
.withColumn(“is_diverted”,
when(col(“diverted”) == 1.0, True)
.otherwise(False))
.withColumn(“delay_category”,
when(col(“cancelled”) == 1.0, “Cancelled”)
.when(col(“arr_delay”).isNull(), “Unknown”)
.when(col(“arr_delay”) <= 0, “On-Time”)
.when(col(“arr_delay”) <= 15, “Minor (1-15 min)”)
.when(col(“arr_delay”) <= 60, “Moderate (16-60 min)”)
.when(col(“arr_delay”) <= 180, “Severe (1-3 hrs)”)
.otherwise(“Extreme (3+ hrs)”))
.withColumn(“primary_delay_cause”,
when(col(“cancelled”) == 1.0, “Cancelled”)
.when(col(“arr_delay”) <= 0, “None”)
.when(
(col(“carrier_delay”) >= col(“weather_delay”)) &
(col(“carrier_delay”) >= col(“nas_delay”)) &
(col(“carrier_delay”) >= col(“security_delay”)) &
(col(“carrier_delay”) >= col(“late_aircraft_delay”)) &
(col(“carrier_delay”) > 0),
“Carrier”)
.when(
(col(“weather_delay”) >= col(“carrier_delay”)) &
(col(“weather_delay”) >= col(“nas_delay”)) &
(col(“weather_delay”) >= col(“security_delay”)) &
(col(“weather_delay”) >= col(“late_aircraft_delay”)) &
(col(“weather_delay”) > 0),
“Weather”)
.when(
(col(“nas_delay”) >= col(“carrier_delay”)) &
(col(“nas_delay”) >= col(“weather_delay”)) &
(col(“nas_delay”) >= col(“security_delay”)) &
(col(“nas_delay”) >= col(“late_aircraft_delay”)) &
(col(“nas_delay”) > 0),
“NAS”)
.when(
(col(“security_delay”) >= col(“carrier_delay”)) &
(col(“security_delay”) >= col(“weather_delay”)) &
(col(“security_delay”) >= col(“nas_delay”)) &
(col(“security_delay”) >= col(“late_aircraft_delay”)) &
(col(“security_delay”) > 0),
“Security”)
.when(col(“late_aircraft_delay”) > 0, “Late Aircraft”)
.otherwise(“Other”))
.withColumn(“day_name”,
when(col(“dayofweek”) == 1, “Sunday”)
.when(col(“dayofweek”) == 2, “Monday”)
.when(col(“dayofweek”) == 3, “Tuesday”)
.when(col(“dayofweek”) == 4, “Wednesday”)
.when(col(“dayofweek”) == 5, “Thursday”)
.when(col(“dayofweek”) == 6, “Friday”)
.otherwise(“Saturday”))
.withColumn(“is_weekend”,
when(col(“dayofweek”).isin(1, 7), True)
.otherwise(False))
.withColumn(“dep_hour”,
floor(col(“scheduled_dep_time”).cast(IntegerType()) / 100).cast(IntegerType()))
.withColumn(“route”,
concat(col(“origin”), lit(”-”), col(“dest”)))
.withColumn(“total_delay_minutes”,
col(“carrier_delay”) + col(“weather_delay”) + col(“nas_delay”) +
col(“security_delay”) + col(“late_aircraft_delay”))
.withColumn(“date_key”,
date_format(col(“flight_date”), “yyyyMMdd”).cast(IntegerType()))
.withColumn(“month_name”,
when(col(“month”) == 1, “January”)
.when(col(“month”) == 2, “February”)
.when(col(“month”) == 3, “March”)
.when(col(“month”) == 4, “April”)
.when(col(“month”) == 5, “May”)
.when(col(“month”) == 6, “June”)
.when(col(“month”) == 7, “July”)
.when(col(“month”) == 8, “August”)
.when(col(“month”) == 9, “September”)
.when(col(“month”) == 10, “October”)
.when(col(“month”) == 11, “November”)
.otherwise(“December”))
)
print(f”  Added derived columns. Total columns now: {len(enriched_df.columns)}”)
# ============================================================
# STEP 6: DROP UNNECESSARY COLUMNS & DEDUPLICATE
# ============================================================
print(“Step 6: Final cleanup…”)
final_df = enriched_df.drop(“flightdate”, “cancelled”, “diverted”)
before_dedup = final_df.count()
final_df = final_df.dropDuplicates()
after_dedup = final_df.count()
print(f”  Rows before dedup: {before_dedup:,}”)
print(f”  Rows after dedup:  {after_dedup:,}”)
print(f”  Duplicates removed: {before_dedup - after_dedup:,}”)
# ============================================================
# STEP 7: WRITE TO CURATED S3 AS PARQUET
# ============================================================
print(“Step 7: Writing curated Parquet to S3…”)
final_df.repartition(“year”, “month”).write  
.mode(“overwrite”)  
.partitionBy(“year”, “month”)  
.parquet(CURATED_S3_PATH)
print(”=” * 60)
print(“FlightPulse ETL Job Complete!”)
print(f”  Output: {CURATED_S3_PATH}”)
print(f”  Total records written: {after_dedup:,}”)
print(f”  Partitioned by: year, month”)
print(f”  Format: Parquet (compressed)”)
print(”=” * 60)
job.commit()
