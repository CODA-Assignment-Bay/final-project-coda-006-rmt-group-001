import great_expectations as gx

context = gx.get_context(context_root_dir='./gx')

# -- Crash Factor Table --
ds_crash_factor = context.get_datasource('dataset-crash_factor')
asset_crash_factor = ds_crash_factor.get_asset('crash_factor-data')
batch_request_crash_factor = asset_crash_factor.build_batch_request()


# -- Crash Time Table --
ds_crash_time = context.get_datasource('dataset-crash_time')
asset_crash_time = ds_crash_time.get_asset('crash_time-data')
batch_request_crash_time = asset_crash_time.build_batch_request()


checkpoint_datamart = context.add_or_update_checkpoint(
    name="multidata_datamart_validation_checkpoint",
    run_name_template="multidata_datamart_validation_checkpoint-%Y%m%d-%H%M%S",
    validations=[
        {
            "batch_request" : batch_request_crash_factor,
            "expectation_suite_name" : "expectation-crash_factor-dataset"
        },
        {
            "batch_request" : batch_request_crash_time,
            "expectation_suite_name" : "expectation-crash_time-dataset"
        },
    ],
)

# Running Checkpoint
checkpoint_datamart_result = checkpoint_datamart.run()

# Check validation results
if not checkpoint_datamart_result["success"]:
    print("Data validation failed!")
    # Optional: Build Data Docs explicitly here if not done by action_list
    context.build_data_docs()
    # context.build_data_docs() # This is now handled by UpdateDataDocsAction
    raise ValueError("Great Expectations data validation failed.")
else:
    print("Data validation succeeded.")
    context.build_data_docs()
    # Optional: Build Data Docs explicitly here if not done by action_list
