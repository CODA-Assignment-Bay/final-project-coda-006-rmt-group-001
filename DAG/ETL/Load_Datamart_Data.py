from pyspark.sql import SparkSession
import os
import shutil

spark = SparkSession.builder \
    .appName("WriteToPostgres") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

# Initialize Spark Session
postgres_url = "jdbc:postgresql://ep-tight-feather-a10zdamw-pooler.ap-southeast-1.aws.neon.tech:5432/neondb?sslmode=require"

postgres_properties = {
    "user": "neondb_owner",
    "password": "npg_u7OicJ5MYTaP",
    "driver": "org.postgresql.Driver"
}

dashboard1_Data = spark.read.csv("/opt/airflow/data/dashboard1.csv", header=True, inferSchema=True)

dashboard2_Data = spark.read.csv("/opt/airflow/data/dashboard2.csv", header=True, inferSchema=True)

table_name = "crash_time"

output_dir = f"/opt/airflow/data/tmp_{table_name}_csv"
final_csv_path = f"/opt/airflow/data/{table_name}.csv"

if os.path.exists(output_dir):
    shutil.rmtree(output_dir)

dashboard1_Data.coalesce(1).write.csv(output_dir, header=True, mode="overwrite")

for file in os.listdir(output_dir):
    if file.startswith("part-") and file.endswith(".csv"):
        os.rename(os.path.join(output_dir, file), final_csv_path)
        break

# Hapus folder temp
shutil.rmtree(output_dir)

print(f"Saved: {final_csv_path}")

table_name = "crash_factors"

output_dir = f"/opt/airflow/data/tmp_{table_name}_csv"
final_csv_path = f"/opt/airflow/data/{table_name}.csv"

if os.path.exists(output_dir):
    shutil.rmtree(output_dir)

dashboard2_Data.coalesce(1).write.csv(output_dir, header=True, mode="overwrite")

for file in os.listdir(output_dir):
    if file.startswith("part-") and file.endswith(".csv"):
        os.rename(os.path.join(output_dir, file), final_csv_path)
        break

# Hapus folder temp
shutil.rmtree(output_dir)

print(f"Saved: {final_csv_path}")