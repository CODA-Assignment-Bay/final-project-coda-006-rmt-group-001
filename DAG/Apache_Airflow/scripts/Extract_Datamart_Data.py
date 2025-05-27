from pyspark.sql import SparkSession
import os
import shutil

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("ReadFromNeon") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

postgres_url = "jdbc:postgresql://ep-tight-feather-a10zdamw-pooler.ap-southeast-1.aws.neon.tech:5432/neondb?sslmode=require"

postgres_properties = {
    "user": "neondb_owner",
    "password": "npg_u7OicJ5MYTaP",
    "driver": "org.postgresql.Driver"
}

data_Date = spark.read.jdbc(url=postgres_url, table="date", properties=postgres_properties)
data_Road = spark.read.jdbc(url=postgres_url, table="roadway", properties=postgres_properties)
data_Crash = spark.read.jdbc(url=postgres_url, table="crash", properties=postgres_properties)

table_name = "date"

output_dir = f"/opt/airflow/data/tmp_{table_name}_csv"
final_csv_path = f"/opt/airflow/data/{table_name}.csv"

if os.path.exists(output_dir):
    shutil.rmtree(output_dir)

data_Date.coalesce(1).write.csv(output_dir, header=True, mode="overwrite")

for file in os.listdir(output_dir):
    if file.startswith("part-") and file.endswith(".csv"):
        os.rename(os.path.join(output_dir, file), final_csv_path)
        break

# Hapus folder temp
shutil.rmtree(output_dir)

table_name = "roadway"

output_dir = f"/opt/airflow/data/tmp_{table_name}_csv"
final_csv_path = f"/opt/airflow/data/{table_name}.csv"

if os.path.exists(output_dir):
    shutil.rmtree(output_dir)

data_Road.coalesce(1).write.csv(output_dir, header=True, mode="overwrite")

for file in os.listdir(output_dir):
    if file.startswith("part-") and file.endswith(".csv"):
        os.rename(os.path.join(output_dir, file), final_csv_path)
        break

# Hapus folder temp
shutil.rmtree(output_dir)

print(f"Saved: {final_csv_path}")

table_name = "crash"

output_dir = f"/opt/airflow/data/tmp_{table_name}_csv"
final_csv_path = f"/opt/airflow/data/{table_name}.csv"

if os.path.exists(output_dir):
    shutil.rmtree(output_dir)

data_Crash.coalesce(1).write.csv(output_dir, header=True, mode="overwrite")

for file in os.listdir(output_dir):
    if file.startswith("part-") and file.endswith(".csv"):
        os.rename(os.path.join(output_dir, file), final_csv_path)
        break

# Hapus folder temp
shutil.rmtree(output_dir)

print(f"Saved: {final_csv_path}")