from pyspark.sql import SparkSession
import kagglehub
import os
import shutil

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("WriteToPostgres") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

DATASET_ROOT_DIR = "/opt/airflow/data"
path = kagglehub.dataset_download("oktayrdeki/traffic-accidents")
print("Path to dataset files:", path)
if not os.path.exists(DATASET_ROOT_DIR):
    os.makedirs(DATASET_ROOT_DIR)
for filename in os.listdir(path):
    src = os.path.join(path, filename)
    dst = os.path.join(DATASET_ROOT_DIR, filename)
    shutil.copy(src, dst)
print("Path to dataset files:", DATASET_ROOT_DIR)