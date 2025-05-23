from pyspark.sql import SparkSession
from pyspark.sql.functions import count,avg,sum,first
import os
import shutil

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("ReadFromNeon") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()


path_data = "/opt/airflow/data/traffic_accidents.csv"

Crash_Data = spark.read.csv("/opt/airflow/data/crash.csv", header=True, inferSchema=True)

Date_Data = spark.read.csv("/opt/airflow/data/date.csv", header=True, inferSchema=True)

roadway_Data = spark.read.csv("/opt/airflow/data/roadway.csv", header=True, inferSchema=True)

dashboard1 = (
    Crash_Data.join(Date_Data, Crash_Data["crash_date"] == Date_Data["date"], "inner")
    .drop(Date_Data["date"])  # Drop the redundant column
    .groupBy("crash_date", "day_of_week", "crash_hour")
    .agg(
        count("id").alias("total_crashes"),
        count("injuries_fatal").alias("total_fatal_injuries"),
        sum("injuries_fatal").alias("sum_fatal_injuries")
    )
    .orderBy("crash_date", "crash_hour")
)

c = Crash_Data.alias("c")
r = roadway_Data.alias("r")

dashboard2 = c.join(r, c["road_id"] == r["id"], "inner") \
    .groupBy(
        c["prim_contributory_cause"], 
        c["weather_condition"], 
        r["road_defect"]
    ) \
    .agg(
        count(c["id"]).alias("total_crashes"),
        avg("injuries_fatal").alias("avg_fatal_injuries"),
        sum("injuries_fatal").alias("sum_fatal_injuries"),
        sum("injuries_incapacitating").alias("total_injuries_incapacitating"),
        sum("injuries_non_incapacitating").alias("total_injuries_non_incapacitating"),
        sum("injuries_reported_not_evident").alias("total_injuries_reported_not_evident"),
        sum("injuries_no_indication").alias("total_injuries_no_indication")
    )

table_name = "dashboard1"

output_dir = f"/opt/airflow/data/tmp_{table_name}_csv"
final_csv_path = f"/opt/airflow/data/{table_name}.csv"

if os.path.exists(output_dir):
    shutil.rmtree(output_dir)

dashboard1.coalesce(1).write.csv(output_dir, header=True, mode="overwrite")

for file in os.listdir(output_dir):
    if file.startswith("part-") and file.endswith(".csv"):
        os.rename(os.path.join(output_dir, file), final_csv_path)
        break

# Hapus folder temp
shutil.rmtree(output_dir)

print(f"Saved: {final_csv_path}")

table_name = "dashboard2"

output_dir = f"/opt/airflow/data/tmp_{table_name}_csv"
final_csv_path = f"/opt/airflow/data/{table_name}.csv"

if os.path.exists(output_dir):
    shutil.rmtree(output_dir)

dashboard2.coalesce(1).write.csv(output_dir, header=True, mode="overwrite")

for file in os.listdir(output_dir):
    if file.startswith("part-") and file.endswith(".csv"):
        os.rename(os.path.join(output_dir, file), final_csv_path)
        break

# Hapus folder temp
shutil.rmtree(output_dir)

print(f"Saved: {final_csv_path}")

