CREATE TABLE date (
  date date primary key,
  year integer,
  month integer,
  week integer,
  day_of_week integer
)

CREATE TABLE roadway(
  id SERIAL primary key,
  trafficway_type varchar(40),
  alignment varchar(40),
  roadway_surface_condition varchar(40),
  road_defect varchar(40)
)

CREATE TABLE crash(
  id SERIAL primary key,
  crash_date date references date(crash_date),
  road_id int references roadway(id),
  traffic_control_device varchar(100),
  weather_condition varchar(100),
  lighting_condition varchar(100),
  first_crash_type varchar(100),
  crash_type varchar(100),
  intersection_related_i varchar(100),
  damage varchar(100),
  prim_contributory_cause varchar(100),
  num_units integer,
  most_severe_injury varchar(100),
  injuries_total integer,
  injuries_fatal integer,
  injuries_incapacitating integer,
  injuries_non_incapacitating integer,
  injuries_reported_not_evident integer,
  injuries_no_indication integer,
  crash_hour INTEGER
)

-- Untuk DDL Data Mart --

CREATE TABLE crash_time(
  crash_date DATE,
  day_of_week INTEGER,
  crash_hour INTEGER,
  total_crashes INTEGER,
  total_fatal_injuries INTEGER,
  sum_fatal_injuries INTEGER
)

CREATE TABLE crash_factors(
  prim_contributory_cause VARCHAR(100),
  weather_condition VARCHAR(100),
  road_defect VARCHAR(100),
  sample_crash_id INTEGER,
  avg_fatal_injuries DOUBLE PRECISION,
  sum_fatal_injuries INTEGER,
  total_injuries_incapacitating INTEGER,
  total_injuries_non_incapacitating INTEGER,
  total_injuries_reported_not_evident INTEGER,
  total_injuries_no_indication INTEGER
)