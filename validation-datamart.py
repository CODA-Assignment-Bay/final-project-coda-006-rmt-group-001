# -- Import Libraries and Building Context --

# Import Libraries
from great_expectations.data_context import FileDataContext


# Building Context
context = FileDataContext.create(project_root_dir='./')

# ================================================================================================================

# -- crash_factor Table --

datasource_crash_factor = 'dataset-crash_factor'
crash_factor_asset = 'crash_factor-data'
expectation_suite_crash_factor_dataset = 'expectation-crash_factor-dataset'

# ================================================================================================================

# -- Date Table --
datasource_crash_time= 'ddataset-crash_time'
crash_time_asset = 'crash_time-data'
expectation_suite_crash_time_dataset = 'expectation-crash_time-dataset'


# ================================================================================================================
# File datasets 
transformed_path_crash_factor = '/opt/airflow/data/dashboard2.csv'
transformed_path_crash_time = '/opt/airflow/data/dashboard1.csv'

# ================================================================================================================

# -- Crash Table --

# Configure datasources and assets for the validation run
ds_crash_factor = context.sources.add_pandas(datasource_crash_factor)
asset_crash_factor = ds_crash_factor.add_csv_asset(crash_factor_asset, filepath_or_buffer=transformed_path_crash_factor)
batch_request_crash_factor = asset_crash_factor.build_batch_request()

# -- Date Table --
ds_crash_time = context.sources.add_pandas(datasource_crash_time)
asset_crash_time = ds_crash_time.add_csv_asset(crash_time_asset, filepath_or_buffer= transformed_path_crash_time)
batch_request_crash_time = asset_crash_time.build_batch_request()




# ================================================================================================================


# Create Checkpoint
checkpoint = context.add_or_update_checkpoint(
    name="multidata_datamart_validation_checkpoint",
    validations=[
        {
            "batch_request" : batch_request_crash_factor,
            "expectation_suite_name" : expectation_suite_crash_factor_dataset
        },
        {
            "batch_request" : batch_request_crash_time,
            "expectation_suite_name" : expectation_suite_crash_time_dataset
        },
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
   