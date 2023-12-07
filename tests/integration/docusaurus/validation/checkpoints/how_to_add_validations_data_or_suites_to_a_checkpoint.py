import great_expectations as gx

context = gx.get_context(mode="ephemeral")

one_validation = [
    {
        "batch_request": {
            "datasource_name": "my_datasource",
            "data_asset_name": "users",
        },
        "expectation_suite_name": "users.warning",
    },
]

checkpoint = context.add_checkpoint(
    name="my_test_checkpoint",
    validations=one_validation,
)

# <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_add_validations_data_or_suites_to_a_checkpoint.py add_expectation_suite">
validations = [
    {
        "batch_request": {
            "datasource_name": "my_datasource",
            "data_asset_name": "users",
        },
        "expectation_suite_name": "users.warning",
    },
    {
        "batch_request": {
            "datasource_name": "my_datasource",
            "data_asset_name": "users.error",
        },
        "expectation_suite_name": "users",
    },
]

checkpoint = context.add_or_update_checkpoint(
    name="my_test_checkpoint",
    validations=validations,
)
# </snippet>
assert len(checkpoint.validations) == 2

# <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_add_validations_data_or_suites_to_a_checkpoint.py add_validation">
validations = [
    {
        "batch_request": {
            "datasource_name": "my_datasource",
            "data_asset_name": "users",
        },
        "expectation_suite_name": "users.warning",
    },
    {
        "batch_request": {
            "datasource_name": "my_datasource",
            "data_asset_name": "users",
        },
        "expectation_suite_name": "users.error",
    },
    {
        "batch_request": {
            "datasource_name": "my_datasource",
            "data_asset_name": "users",
        },
        "expectation_suite_name": "users.delivery",
        "action_list": [
            {
                "name": "quarantine_failed_data",
                "action": {
                    "class_name": "CreateQuarantineData",
                },
            },
            {
                "name": "advance_passed_data",
                "action": {
                    "class_name": "CreateQuarantineData",
                },
            },
        ],
    },
]

checkpoint = context.add_or_update_checkpoint(
    name="my_test_checkpoint", validations=validations
)
# </snippet>
assert len(checkpoint.validations) == 3
