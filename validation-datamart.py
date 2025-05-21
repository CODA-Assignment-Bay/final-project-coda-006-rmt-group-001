# -- Import Libraries and Building Context --

# Import Libraries
from great_expectations.data_context import get_context
from great_expectations.core.batch import RuntimeBatchRequest


# Building Context
context = get_context(context_root_dir="./gx")

# ================================================================================================================
# File datasets (one for local testing or Airflow deployment)
# If testing locally, make sure these paths are correct for your local files:
transformed_path_crash_time_local = 'datasets/crash_time.csv'
transformed_path_crash_factor_local = 'datasets/crash_factor.csv'

# If deploying to Airflow, use these paths (assuming files are at /opt/airflow/data/):
transformed_path_crash_time_airflow = '/opt/airflow/data/dashboard1.csv'
transformed_path_crash_factor_airflow = '/opt/airflow/data/dashboard2.csv'

# Choose which paths to use:
# For local testing:
# current_crash_time_path = transformed_path_crash_time_local
# current_crash_factor_path = transformed_path_crash_factor_local

# For Airflow deployment:
current_crash_time_path = transformed_path_crash_time_airflow
current_crash_factor_path = transformed_path_crash_factor_airflow

# ================================================================================================================

batch_request_crash_factor = RuntimeBatchRequest( 
    datasource_name="dataset-crash_factor",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="crash_factor-data",
    runtime_parameters={"path": current_crash_factor_path}, # Use the chosen path variable
    batch_identifiers={"default_identifier_name": "crash_factor_run"},
    batch_spec_passthrough={"reader_method": "csv"}, # This is how to read the file
)

batch_request_crash_time = RuntimeBatchRequest( 
    datasource_name="dataset-crash_time",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="crash_time-data",
    runtime_parameters={"path": current_crash_time_path}, # Use the chosen path variable
    batch_identifiers={"default_identifier_name": "crash_time_run"},
    batch_spec_passthrough={"reader_method": "csv"}, # This is how to read the file
)




# ================================================================================================================


# Create Checkpoint
checkpoint = context.add_or_update_checkpoint(
    name="multidata_datamart_validation_checkpoint", # Changed name as per your code
    validations=[
        {
            "batch_request" : batch_request_crash_factor,
            "expectation_suite_name" : 'expectation-crash_factor-dataset' # Ensure this matches your suite name exactly
        },
        {
            "batch_request" : batch_request_crash_time,
            "expectation_suite_name" : 'expectation-crash_time-dataset' # Ensure this matches your suite name exactly
        },
    ],
    # action_list=[
    #     {
    #         "name": "run_callbacks",
    #         "action": {
    #             "class_name": "RunCallbacksAction",
    #             "args": {"callbacks": []},
    #         },
    #     },
    #     {
    #         "name": "store_validation_result",
    #         "action": {"class_name": "StoreValidationResultAction"},
    #     },
    #     {
    #         "name": "store_evaluation_parameter_metrics",
    #         "action": {"class_name": "StoreEvaluationParameterMetricsAction"},
    #     },
    #     {
    #         "name": "update_data_docs",
    #         "action": {"class_name": "UpdateDataDocsAction"},
    #     },
    # ]
)

# Running Checkpoint
checkpoint_result = checkpoint.run()

# Check validation results
if not checkpoint_result["success"]:
    print("Data validation failed!")
    context.build_data_docs()
    raise ValueError("Great Expectations data validation failed.")
else:
    print("Data validation succeeded.")
    context.build_data_docs()