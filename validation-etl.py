# -- Import Libraries and Building Context --

# Import Libraries
from great_expectations.data_context import FileDataContext


# Building Context
context = FileDataContext.create(project_root_dir='./')

# ================================================================================================================

# -- Crash Table --

datasource_crash = 'dataset-table-Crash'
crash_asset = 'crash-data'
expectation_suite_crash_dataset = 'expectation-crash-dataset'

# ================================================================================================================

# -- Date Table --
datasource_date= 'dataset-table-Date'
date_asset = 'date-data'
expectation_suite_date_dataset = 'expectation-date-dataset'

# ================================================================================================================

# -- Road Table --
datasource_road= 'dataset-table-Road-1'
road_asset = 'date-data'
expectation_suite_road_dataset = 'expectation-road-dataset'

# ================================================================================================================
# File datasets 
transformed_path_crash = '/opt/airflow/data/Clean_Crash_Data.csv'
transformed_path_date = '/opt/airflow/data/Clean_Crash_Date_Data.csv'
transformed_path_road = '/opt/airflow/data/Clean_Road_Data.csv'

# ================================================================================================================

# -- Crash Table --

# Configure datasources and assets for the validation run
ds_crash = context.sources.add_pandas(datasource_crash)
asset_crash = ds_crash.add_csv_asset(crash_asset, filepath_or_buffer=transformed_path_crash)
batch_request_crash = asset_crash.build_batch_request()

# -- Date Table --
ds_date = context.sources.add_pandas(datasource_date)
asset_date = ds_date.add_csv_asset(date_asset, filepath_or_buffer=transformed_path_date)
batch_request_date = asset_date.build_batch_request()

# -- Road Table --
ds_road = context.sources.add_pandas(datasource_road)
asset_road = ds_road.add_csv_asset(road_asset, filepath_or_buffer=transformed_path_road)
batch_request_road = asset_road.build_batch_request()


# ================================================================================================================


# Create Checkpoint
checkpoint = context.add_or_update_checkpoint(
    name="multidata_validation_checkpoint",
    validations=[
        {
            "batch_request" : batch_request_crash,
            "expectation_suite_name" : expectation_suite_crash_dataset
        },
        {
            "batch_request" : batch_request_date,
            "expectation_suite_name" : expectation_suite_date_dataset
        },
        {
            "batch_request" : batch_request_road,
            "expectation_suite_name" : expectation_suite_road_dataset
        }
    ],
    # Configure Data Docs to be built after the checkpoint run
    action_list=[
        {
            "name": "run_callbacks",
            "action": {
                "class_name": "RunCallbacksAction",
                "args": {"callbacks": []},
            },
        },
        {
            "name": "store_validation_result",
            "action": {"class_name": "StoreValidationResultAction"},
        },
        {
            "name": "store_evaluation_parameter_metrics",
            "action": {"class_name": "StoreEvaluationParameterMetricsAction"},
        },
        {
            "name": "update_data_docs",
            "action": {"class_name": "UpdateDataDocsAction"},
        },
    ]
)

# Running Checkpoint
checkpoint_result = checkpoint.run()

# Check validation results
if not checkpoint_result["success"]:
    print("Data validation failed!")
    # Optional: Build Data Docs explicitly here if not done by action_list
    # context.build_data_docs() # This is now handled by UpdateDataDocsAction
    raise ValueError("Great Expectations data validation failed.")
else:
    print("Data validation succeeded.")
    # Optional: Build Data Docs explicitly here if not done by action_list
   