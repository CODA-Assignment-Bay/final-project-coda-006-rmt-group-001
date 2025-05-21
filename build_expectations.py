# -- Import Libraries and Building Context --

# Import Libraries
import pandas as pd
from great_expectations.data_context import FileDataContext


# Building Context
context = FileDataContext.create(project_root_dir='./')

# ================================================================================================================

# -- Crash Table --

# Give a name to a Datasource. This name must be unique between Datasources.
datasource_crash = 'dataset-table-Crash'
ds_crash = context.sources.add_pandas(datasource_crash)

# Give a name to a data asset
crash_asset = 'crash-data'
path_to_data_crash = 'datasets/Clean_Crash_Data.csv'
asset_crash = ds_crash.add_csv_asset(crash_asset, filepath_or_buffer=path_to_data_crash)

# Build batch request
batch_request_crash = asset_crash.build_batch_request()

# Creat an expectation suite
expectation_suite_crash_dataset = 'expectation-crash-dataset'
context.add_or_update_expectation_suite(expectation_suite_crash_dataset)

# Create a validator using above expectation suite
validator_crash = context.get_validator(
    batch_request = batch_request_crash,
    expectation_suite_name = expectation_suite_crash_dataset
)

# Check the validator
validator_crash.head()

# Expectation
validator_crash.expect_column_values_to_not_be_null('crash_date')
validator_crash.expect_column_values_to_not_be_null('road_id')
columns_injury = ["injuries_total", "injuries_fatal", "injuries_incapacitating", "injuries_non_incapacitating","injuries_reported_not_evident","injuries_no_indication"]

for cols in columns_injury:
    results = validator_crash.expect_column_values_to_be_in_type_list(column = cols, type_list=['integer', 'float', 'int64', 'float64'])
    print(f"{cols}: {'Success' if results['success'] else 'Failed'}")

# Saving expectation suite
validator_crash.save_expectation_suite(discard_failed_expectations=False)

# ================================================================================================================

# -- Date Table --

# Give a name to a Datasource. This name must be unique between Datasources.
datasource_date= 'dataset-table-Date'
ds_date = context.sources.add_pandas(datasource_date)

# Give a name to a data asset
date_asset = 'date-data'
path_to_data_date = 'datasets/Clean_Crash_Date_Data.csv'
asset_date = ds_date.add_csv_asset(date_asset, filepath_or_buffer=path_to_data_date)

# Build batch request
batch_request_date = asset_date.build_batch_request()

# Creat an expectation suite
expectation_suite_date_dataset = 'expectation-date-dataset'
context.add_or_update_expectation_suite(expectation_suite_date_dataset)

# Create a validator using above expectation suite
validator_date = context.get_validator(
    batch_request = batch_request_date,
    expectation_suite_name = expectation_suite_date_dataset
)

# Check the validator
validator_date.head()

# Finding min max on a column
df_date = pd.read_csv('datasets/Clean_Crash_Date_Data.csv') # Make sure path files are correct
min_year = df_date['year'].min()
max_year = df_date['year'].max()
print("Min year:", min_year)
print("Max year:", max_year)

# Expectations
validator_date.expect_column_values_to_be_unique('date')
validator_date.expect_column_values_to_be_between(
    column='year', 
    min_value= min_year, 
    max_value = max_year
)
validator_date.expect_column_values_to_be_between(
    column='month', 
    min_value= 1, 
    max_value = 12
)
validator_date.expect_column_values_to_be_between(
    column='day_of_week', 
    min_value= 1, 
    max_value = 7
)
validator_date.expect_column_values_to_be_between(
    column='week', 
    min_value= 1, 
    max_value = 5
)

# Saving expectation suite
validator_date.save_expectation_suite(discard_failed_expectations=False)

# ================================================================================================================

# -- Road Table --

# Give a name to a Datasource. This name must be unique between Datasources.
datasource_road= 'dataset-table-Road-1'
ds_road = context.sources.add_pandas(datasource_road)

# Give a name to a data asset
road_asset = 'date-data'
path_to_data_road = 'datasets/Clean_Road_Data.csv'
asset_road = ds_road.add_csv_asset(road_asset, filepath_or_buffer=path_to_data_road)

# Build batch request
batch_request_road = asset_road.build_batch_request()

# Creat an expectation suite
expectation_suite_road_dataset = 'expectation-road-dataset'
context.add_or_update_expectation_suite(expectation_suite_road_dataset)

# Create a validator using above expectation suite
validator_road = context.get_validator(
    batch_request = batch_request_road,
    expectation_suite_name = expectation_suite_road_dataset
)

# Check the validator
validator_road.head()

# Expectations
columns = ["trafficway_type", "alignment", "roadway_surface_condition", "road_defect"]

for col in columns:
    result = validator_road.expect_column_values_to_not_be_null(column=col)
    print(f"{col}: {'Success' if result['success'] else 'Failed'}")

# Saving expectation suite
validator_road.save_expectation_suite(discard_failed_expectations=False)

# ================================================================================================================
# ================================================================================================================

# -- crash_factor Table --

# Give a name to a Datasource. This name must be unique between Datasources.
datasource_crash_factor = 'dataset-crash_factor'
ds_crash_factor = context.sources.add_pandas(datasource_crash_factor)

# Give a name to a data asset
crash_factor_asset = 'crash_factor-data'
path_to_data_crash_factor = 'datasets/crash_factors.csv'
asset_crash_factor = ds_crash_factor.add_csv_asset(crash_factor_asset, filepath_or_buffer=path_to_data_crash_factor)

# Build batch request
batch_request_crash_factor = asset_crash_factor.build_batch_request()

# Creat an expectation suite
expectation_suite_crash_factor_dataset = 'expectation-crash_factor-dataset'
context.add_or_update_expectation_suite(expectation_suite_crash_factor_dataset)

# Create a validator using above expectation suite
validator_crash_factor = context.get_validator(
    batch_request = batch_request_crash_factor,
    expectation_suite_name = expectation_suite_crash_factor_dataset
)

# Check the validator
validator_crash_factor.head()

# Expectation
columns_cf_1 = ["total_crashes", "avg_fatal_injuries", "sum_fatal_injuries"]
columns_cf_2 = ["total_injuries_incapacitating", "total_injuries_non_incapacitating", "total_injuries_reported_not_evident", "total_injuries_no_indication"]
for colo in columns_cf_1:
    results_1 = validator_crash_factor.expect_column_to_exist(column = colo)
    print(f"{colo}: {'Success' if results_1['success'] else 'Failed'}")
for cola in columns_cf_2:
    results_2 = validator_crash_factor.expect_column_values_to_be_in_type_list(column = cola, type_list=['int64', 'float', 'integer', 'float64'])
    print(f"{cola}: {'Success' if results_2['success'] else 'Failed'}")

# Saving expectation suite
validator_crash_factor.save_expectation_suite(discard_failed_expectations=False)

# ================================================================================================================

# -- crash_time Table --

# Give a name to a Datasource. This name must be unique between Datasources.
datasource_crash_time= 'dataset-crash_time'
ds_crash_time = context.sources.add_pandas(datasource_crash_time)

# Give a name to a data asset
crash_time_asset = 'crash_time-data'
path_to_data_crash_time = 'datasets/crash_time.csv'
asset_crash_time = ds_crash_time.add_csv_asset(crash_time_asset, filepath_or_buffer= path_to_data_crash_time)

# Build batch request
batch_request_crash_time = asset_crash_time.build_batch_request()

# Creat an expectation suite
expectation_suite_crash_time_dataset = 'expectation-crash_time-dataset'
context.add_or_update_expectation_suite(expectation_suite_crash_time_dataset)

# Create a validator using above expectation suite
validator_crash_time = context.get_validator(
    batch_request = batch_request_crash_time,
    expectation_suite_name = expectation_suite_crash_time_dataset
)

# Check the validator
validator_crash_time.head()

# Expectations
validator_crash_time.expect_column_values_to_not_be_null('crash_date')
validator_crash_time.expect_column_to_exist('sum_fatal_injuries')
columns_ct = ["day_of_week", "crash_hour", "total_crashes", "total_fatal_injuries"]

for colu in columns_ct:
    results_3 = validator_crash_time.expect_column_values_to_be_in_type_list(column = colu, type_list=['int64', 'float', 'integer', 'float64'])
    print(f"{colu}: {'Success' if results_3['success'] else 'Failed'}")

# Saving expectation suite
validator_crash_time.save_expectation_suite(discard_failed_expectations=False)

print("All expectation suites generated and saved successfully.")