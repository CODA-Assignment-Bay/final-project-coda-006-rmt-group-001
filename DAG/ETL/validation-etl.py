import great_expectations as gx

context = gx.get_context(context_root_dir='/opt/airflow/data/gx')

# -- Crash Table --

ds_crash = context.get_datasource('dataset-table-Crash')
asset_crash = ds_crash.get_asset('crash-data')
batch_request_crash = asset_crash.build_batch_request()


# -- Date Table --
ds_date = context.get_datasource('dataset-table-Date')
asset_date = ds_date.get_asset('date-data')
batch_request_date = asset_date.build_batch_request()


# -- Road Table --
ds_road = context.get_datasource('dataset-table-Road-1')
asset_road = ds_road.get_asset('road-data')
batch_request_road = asset_road.build_batch_request()

checkpoint_etl = context.add_or_update_checkpoint(
    name="multidata_validation_checkpoint",
    run_name_template="multidata_validation_checkpoint-%Y%m%d-%H%M%S",
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
checkpoint_result = checkpoint_etl.run()

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
