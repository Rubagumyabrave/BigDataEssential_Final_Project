"""
Same insights job as before, adjusted to run INSIDE Docker on kafka-net.
Uses hdfs://namenode:9000 (internal Docker address) instead of
hdfs://localhost:9000 - this is the same address Kafka Connect already
uses reliably, since both are on the same Docker network. No host-to-
container networking involved, which is what caused the intermittent
BlockMissingException failures when running from Windows directly.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, max as spark_max, min as spark_min, round as spark_round, when
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

spark = SparkSession.builder.appName("FlightInsights").getOrCreate()
spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")

HDFS_PATH = "hdfs://namenode:9000/flights/flights/"

raw = spark.read.json(HDFS_PATH)
print("Raw files read. Schema:")
raw.printSchema()

df = raw.select(
    col("flight_id"),
    col("fleet"),
    col("great_circle_distance_nm").alias("distance_nm"),
    col("gross_weight_at_liftoff_kg").alias("weight_kg"),
    col("max_tailwind_during_takeoff_kt").alias("tailwind_kt"),
    col("generated_at"),
)
df = df.dropDuplicates(["flight_id"])
# Older records (before the model was simplified) have no tailwind field -
# keep only rows with the core fields actually present, so averages aren't
# skewed by nulls.
df = df.dropna(subset=["fleet", "distance_nm", "weight_kg"])

print(f"\nTotal unique flights loaded from HDFS: {df.count()}")
df.show(5, truncate=False)

print("\n" + "=" * 70)
print("INSIGHT 1: Average distance and weight per fleet")
print("=" * 70)
insight1 = df.groupBy("fleet").agg(
    count("*").alias("n_flights"),
    spark_round(avg("distance_nm"), 1).alias("avg_distance_nm"),
    spark_round(avg("weight_kg"), 0).alias("avg_weight_kg"),
)
insight1.orderBy("fleet").show()

print("=" * 70)
print("INSIGHT 2: Distance range per fleet (min - max)")
print("=" * 70)
insight2 = df.groupBy("fleet").agg(
    spark_round(spark_min("distance_nm"), 0).alias("min_distance_nm"),
    spark_round(spark_max("distance_nm"), 0).alias("max_distance_nm"),
    spark_round(spark_max("distance_nm") - spark_min("distance_nm"), 0).alias("range_nm"),
)
insight2.orderBy("fleet").show()

print("=" * 70)
print("INSIGHT 3: Headwind vs tailwind flight counts per fleet")
print("=" * 70)
df_wind = df.withColumn(
    "wind_condition",
    when(col("tailwind_kt") > 0, "tailwind")
    .when(col("tailwind_kt") < 0, "headwind")
    .otherwise("calm")
)
insight3 = df_wind.groupBy("fleet", "wind_condition").count()
insight3.orderBy("fleet", "wind_condition").show()

print("=" * 70)
print("INSIGHT 4: Heaviest recorded flight per fleet")
print("=" * 70)
w = Window.partitionBy("fleet").orderBy(col("weight_kg").desc())
heaviest = (
    df.withColumn("rank", row_number().over(w))
      .filter(col("rank") == 1)
      .select("fleet", "flight_id", "distance_nm", "weight_kg")
)
heaviest.show(truncate=False)

print("\nDone.")
spark.stop()
