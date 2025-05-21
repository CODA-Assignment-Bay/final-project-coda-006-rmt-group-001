from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, row_number, ceil, dayofmonth, sequence, explode, to_date, min, max, trunc, last_day, dayofweek
from pyspark.sql.window import Window
import os
import shutil

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("WriteToPostgres") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()


path_data = "/opt/airflow/data/traffic_accidents.csv"

raw_data = spark.read.csv(path_data, header=True, inferSchema=True)

window = Window.orderBy("trafficway_type", "alignment", "roadway_surface_condition", "road_defect")

Roadway_Data = raw_data.select(col("trafficway_type"), col("alignment"), col("roadway_surface_cond").alias("roadway_surface_condition"), 
                               col("road_defect")).withColumn("id", row_number().over(window))

Clean_Roadway_Data = Roadway_Data.dropDuplicates(["trafficway_type", "alignment", "roadway_surface_condition",  "road_defect"])

window = Window.orderBy("trafficway_type", "alignment", "roadway_surface_condition", "road_defect")

Final_Roadway_Data = Clean_Roadway_Data.select(col("trafficway_type"), ("alignment"), ("roadway_surface_condition"),
                                        ("road_defect")).withColumn("id", row_number().over(window))

raw_data = raw_data.withColumn("crash_date", to_date("crash_date", "MM/dd/yyyy hh:mm:ss a"))

datas_date = raw_data.select(
    min("crash_date").alias("min_date"),
    max("crash_date").alias("max_date")
)

date_range = datas_date.select(
    trunc("min_date", "MM").alias("start_of_min_month"),
    last_day("max_date").alias("end_of_max_month")
)

generated_dates= date_range.select(
    explode(sequence(to_date(col("start_of_min_month")), to_date(col("end_of_max_month")))).alias("crash_date")
)

window = Window.orderBy("date")

Final_Date_Data = generated_dates.select(
    col("crash_date").alias("date"),
    year("crash_date").alias("crash_year"),
    month("crash_date").alias("crash_month"),
    dayofweek("crash_date").alias("crash_day_of_week")
).withColumn("crash_week", ceil(dayofmonth("crash_date") / 7.0))



Join_Final_Roadway_Data = Final_Roadway_Data.withColumnRenamed("id", "road_id")

raw_data_alias = raw_data.alias("a")
final_date_alias = Final_Date_Data.alias("b")

Date_Temp = raw_data_alias.join(final_date_alias, raw_data_alias["crash_date"] == final_date_alias["date"])

Crash_Temp = Date_Temp.join(Join_Final_Roadway_Data, (Date_Temp["trafficway_type"] == Join_Final_Roadway_Data["trafficway_type"])
                             & (Date_Temp["alignment"] == Join_Final_Roadway_Data["alignment"])
                             & (Date_Temp["roadway_surface_cond"] == Join_Final_Roadway_Data["roadway_surface_condition"])
                             & (Date_Temp["road_defect"] == Join_Final_Roadway_Data["road_defect"])
                             , "inner")

Crash_Data = Crash_Temp.select(raw_data_alias["crash_date"], "road_id", "crash_hour", "traffic_control_device", "weather_condition", "lighting_condition",
                                   "first_crash_type", "crash_type", "intersection_related_i", "damage", "prim_contributory_cause",
                                   "num_units", "most_severe_injury", "injuries_total", "injuries_fatal", "injuries_incapacitating",
                                   "injuries_non_incapacitating", "injuries_reported_not_evident", "injuries_no_indication")


tmp_path = os.path.join("/opt/airflow/data", "_tmp_output")

Crash_Data.coalesce(1).write.mode("overwrite").option("header", "true").csv(tmp_path)

for file in os.listdir(tmp_path):
    if file.startswith("part-") and file.endswith(".csv"):
        full_temp_file_path = os.path.join(tmp_path, file)
        break
else:
    raise FileNotFoundError("CSV part file not found in temp folder.")

final_output_path = os.path.join("/opt/airflow/data", "Clean_Crash_Data.csv")
shutil.move(full_temp_file_path, final_output_path)

shutil.rmtree(tmp_path)

print(f"Saved: {final_output_path}")

Final_Date_Data = Final_Date_Data.orderBy("date")

tmp_path = os.path.join("/opt/airflow/data", "_tmp_output")

Final_Date_Data.coalesce(1).write.mode("overwrite").option("header", "true").csv(tmp_path)

for file in os.listdir(tmp_path):
    if file.startswith("part-") and file.endswith(".csv"):
        full_temp_file_path = os.path.join(tmp_path, file)
        break
else:
    raise FileNotFoundError("CSV part file not found in temp folder.")

final_output_path = os.path.join("/opt/airflow/data", "Clean_Crash_Date_Data.csv")
shutil.move(full_temp_file_path, final_output_path)

shutil.rmtree(tmp_path)

print(f"Saved: {final_output_path}")

Final_Roadway_Data = Final_Roadway_Data.drop("id")
Final_Roadway_Data = Final_Roadway_Data.orderBy("trafficway_type", "alignment", "roadway_surface_condition", "road_defect")

tmp_path = os.path.join("/opt/airflow/data", "_tmp_output")

Final_Roadway_Data.coalesce(1).write.mode("overwrite").option("header", "true").csv(tmp_path)

for file in os.listdir(tmp_path):
    if file.startswith("part-") and file.endswith(".csv"):
        full_temp_file_path = os.path.join(tmp_path, file)
        break
else:
    raise FileNotFoundError("CSV part file not found in temp folder.")

final_output_path = os.path.join("/opt/airflow/data", "Clean_Road_Data.csv")
shutil.move(full_temp_file_path, final_output_path)

shutil.rmtree(tmp_path)

print(f"Saved: {final_output_path}")

