# -- Import Libraries and Building Context --

# Import Libraries
from great_expectations.data_context import get_context
from great_expectations.core.batch import RuntimeBatchRequest


# Building Context
context = get_context(context_root_dir="./gx")

# ================================================================================================================
# File datasets (one for local testing or Airflow deployment)
# If testing locally, make sure these paths are correct for your local files:
transformed_path_crash_local = 'datasets/Clean_Crash_Data.csv'
transformed_path_date_local = 'datasets/Clean_Crash_Date_Data.csv'
transformed_path_road_local = 'datasets/Clean_Date_Data.csv'

# If deploying to Airflow, use these paths (assuming files are at /opt/airflow/data/):
transformed_path_crash_airflow  = '/opt/airflow/data/Clean_Crash_Data.csv'
transformed_path_date_airflow  = '/opt/airflow/data/Clean_Crash_Date_Data.csv'
transformed_path_road_airflow  = '/opt/airflow/data/Clean_Road_Data.csv'

# Choose which paths to use:
# For local testing:
# current_crash_path = transformed_path_crash_local 
# current_date_path = transformed_path_date_local 
# current_road_path = transformed_path_road_local

# For Airflow deployment:
current_crash_path = transformed_path_crash_airflow
current_date_path = transformed_path_date_airflow
current_road_path = transformed_path_road_airflow

# ================================================================================================================

batch_request_crash = RuntimeBatchRequest( 
    datasource_name="dataset-table-Crash",
    data_connector_name="default_runtime_data_connector_name", 
    data_asset_name="crash-data",
    runtime_parameters={"path": current_crash_path}, # Use the chosen path variable
    batch_identifiers={"default_identifier_name": "crash_data_run"},
    batch_spec_passthrough={"reader_method": "csv"}, # This is how to read the file
)

batch_request_date = RuntimeBatchRequest( 
    datasource_name="dataset-table-Date",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="date-data",
    runtime_parameters={"path": current_date_path}, # Use the chosen path variable
    batch_identifiers={"default_identifier_name": "date_data_run"}, 
    batch_spec_passthrough={"reader_method": "csv"}, # This is how to read the file
)

batch_request_road = RuntimeBatchRequest( 
    datasource_name="dataset-table-Road-1",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="road-data",
    runtime_parameters={"path": current_road_path}, # Specify path here
    batch_identifiers={"default_identifier_name": "road_data_run"}, 
    batch_spec_passthrough={"reader_method": "csv"}, # This is how to read the file
)

# ================================================================================================================


# Create Checkpoint
checkpoint = context.add_or_update_checkpoint(
    name="multidata_validation_checkpoint",
    validations=[
        {
            "batch_request" : batch_request_crash,
            "expectation_suite_name" : "expectation-crash-dataset"
        },
        {
            "batch_request" : batch_request_date,
            "expectation_suite_name" : "expectation-date-dataset"
        },
        {
            "batch_request" : batch_request_road,
            "expectation_suite_name" : "expectation-road-dataset"
        }
    ],
)

# Running Checkpoint
checkpoint_result = checkpoint.run()

# Check validation results
if not checkpoint_result["success"]:
    print("Data validation failed!")
    # Optional: Build Data Docs explicitly here if not done by action_list
    context.build_data_docs()
    # context.build_data_docs() # This is now handled by UpdateDataDocsAction
    raise ValueError("Great Expectations data validation failed.")
else:
    print("Data validation succeeded.")
    context.build_data_docs()
    # Optional: Build Data Docs explicitly here if not done by action_list
   