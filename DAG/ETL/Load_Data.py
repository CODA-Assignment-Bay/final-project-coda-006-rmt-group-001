from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("WriteToPostgres") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

postgres_url = "jdbc:postgresql://ep-black-sun-a1uqdrq7-pooler.ap-southeast-1.aws.neon.tech:5432/neondb?sslmode=require"

postgres_properties = {
    "user": "neondb_owner",
    "password": "npg_MP9sx2oAVjyf",
    "driver": "org.postgresql.Driver"
}

data_date = spark.read.csv('/opt/airflow/data/Clean_Crash_Date_Data.csv', header=True , inferSchema=True)
data_road = spark.read.csv('/opt/airflow/data/Clean_Road_Data.csv', header=True , inferSchema=True)
data_crash = spark.read.csv('/opt/airflow/data/Clean_Crash_Data.csv', header=True , inferSchema=True)

data_date.write.jdbc(url=postgres_url, table="date", mode="append", properties=postgres_properties)
data_road.write.jdbc(url=postgres_url, table="roadway", mode="append", properties=postgres_properties)
data_crash.write.jdbc(url=postgres_url, table="crash", mode="append", properties=postgres_properties)