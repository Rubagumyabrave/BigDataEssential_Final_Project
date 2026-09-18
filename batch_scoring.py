"""
=============================================================================
BATCH SCORING — HDFS -> Spark MLlib -> MySQL -> Dashboard
=============================================================================
Rubric item 6: "deployed (batch scoring)... used to generate real
predictions on new/incoming records."

Pipeline for this job:
    1. Read flight records from HDFS (the historical store Kafka Connect
       has been writing into all session).
    2. Apply the trained per-fleet coefficients (the same LinearRegression
       fit earlier with Spark MLlib on model_df_clean.xlsx) to compute a
       predicted fuel burn for every record.
    3. Write the scored results into a MySQL table, which the Django
       dashboard reads from directly.

Runs the SAME way as pyspark_insights_docker.py - inside a container on
kafka-net, so HDFS reads are reliable (no host-to-container networking
issue), and MySQL is reached by its container name.
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, log, exp, lit

spark = SparkSession.builder.appName("FuelBatchScoring").getOrCreate()
spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")

HDFS_PATH = "hdfs://namenode:9000/flights/flights/"
MYSQL_URL = "jdbc:mysql://mysql:3306/flights_db"
MYSQL_USER = "flightuser"
MYSQL_PASSWORD = "flightpass"
OUTPUT_TABLE = "fuel_predictions"

# Trained coefficients from the Spark MLlib LinearRegression fit earlier
# (log-log model: log(fuel_burn) = intercept + b1*log(distance) +
# b2*log(weight) + b3*tailwind). B737-NG and B737F-NG share one model,
# same pooling decision used throughout this project.
COEFFICIENTS = {
    "B737": {
        "intercept": -6.426394064045495,
        "log_distance": 0.7166201686817483,
        "log_weight": 0.9244778178253861,
        "tailwind": -0.003064840113940369,
    },
    "A330": {
        "intercept": -8.93222388124341,
        "log_distance": 0.6802848191599308,
        "log_weight": 1.1475673176091954,
        "tailwind": 0.0015051430171990363,
    },
}


# =============================================================================
# READ AND CLEAN
# =============================================================================

raw = spark.read.json(HDFS_PATH)

df = raw.select(
    col("flight_id"),
    col("fleet"),
    col("great_circle_distance_nm").alias("distance_nm"),
    col("gross_weight_at_liftoff_kg").alias("weight_kg"),
    col("max_tailwind_during_takeoff_kt").alias("tailwind_kt"),
    col("generated_at"),
)
df = df.dropDuplicates(["flight_id"])
df = df.dropna(subset=["fleet", "distance_nm", "weight_kg", "tailwind_kt"])
df = df.filter((col("distance_nm") > 0) & (col("weight_kg") > 0))

print(f"Flights loaded and cleaned for scoring: {df.count()}")

# Pool B737-NG and B737F-NG, same as the training step
df = df.withColumn(
    "fleet_group",
    when(col("fleet") == "A330", "A330").otherwise("B737")
)


# =============================================================================
# SCORE — apply the trained coefficients per fleet group
# =============================================================================

b737 = COEFFICIENTS["B737"]
a330 = COEFFICIENTS["A330"]

log_pred = when(
    col("fleet_group") == "B737",
    b737["intercept"]
    + b737["log_distance"] * log(col("distance_nm"))
    + b737["log_weight"] * log(col("weight_kg"))
    + b737["tailwind"] * col("tailwind_kt")
).otherwise(
    a330["intercept"]
    + a330["log_distance"] * log(col("distance_nm"))
    + a330["log_weight"] * log(col("weight_kg"))
    + a330["tailwind"] * col("tailwind_kt")
)

scored = df.withColumn("predicted_fuel_burn_kg", exp(log_pred))

print("\nSample of scored flights:")
scored.select("flight_id", "fleet", "distance_nm", "weight_kg",
              "tailwind_kt", "predicted_fuel_burn_kg").show(10, truncate=False)

print("\nAverage predicted fuel burn per fleet:")
scored.groupBy("fleet").avg("predicted_fuel_burn_kg").show()


# =============================================================================
# WRITE — results into MySQL for the dashboard to read
# =============================================================================

output = scored.select(
    "flight_id", "fleet", "distance_nm", "weight_kg",
    "tailwind_kt", "generated_at", "predicted_fuel_burn_kg"
)

output.write.format("jdbc") \
    .option("url", MYSQL_URL) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", OUTPUT_TABLE) \
    .option("user", MYSQL_USER) \
    .option("password", MYSQL_PASSWORD) \
    .mode("overwrite") \
    .save()

print(f"\nWrote {output.count()} scored predictions to MySQL table '{OUTPUT_TABLE}'")
print("Done.")
spark.stop()
