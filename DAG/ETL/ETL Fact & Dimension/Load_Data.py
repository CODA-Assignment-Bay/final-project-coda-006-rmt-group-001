from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
import psycopg2

conn = psycopg2.connect(
    dbname="neondb",
    user="neondb_owner",
    password="npg_u7OicJ5MYTaP",
    host="ep-tight-feather-a10zdamw-pooler.ap-southeast-1.aws.neon.tech",
    port="5432",
    sslmode="require"
)

# Execute the TRUNCATE command
with conn:
    with conn.cursor() as cursor:
        cursor.execute("TRUNCATE crash, roadway, date RESTART IDENTITY CASCADE;")

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("WriteToPostgres") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

postgres_url = "jdbc:postgresql://ep-tight-feather-a10zdamw-pooler.ap-southeast-1.aws.neon.tech:5432/neondb?sslmode=require"

postgres_properties = {
    "user": "neondb_owner",
    "password": "npg_u7OicJ5MYTaP",
    "driver": "org.postgresql.Driver"
}

data_date = spark.read.csv('/opt/airflow/data/Clean_Crash_Date_Data.csv', header=True , inferSchema=True)
data_date = data_date.withColumn("date", to_date("date", "yyyy-MM-dd"))
data_road = spark.read.csv('/opt/airflow/data/Clean_Road_Data.csv', header=True , inferSchema=True)
data_crash = spark.read.csv('/opt/airflow/data/Clean_Crash_Data.csv', header=True , inferSchema=True)
data_crash = data_crash.withColumn("crash_date", to_date("crash_date", "yyyy-MM-dd"))


data_date.write.jdbc(url=postgres_url, table="date", mode="append", properties=postgres_properties)
data_road.write.jdbc(url=postgres_url, table="roadway", mode="append", properties=postgres_properties)
data_crash.write.jdbc(url=postgres_url, table="crash", mode="append", properties=postgres_properties)