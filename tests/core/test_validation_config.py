import json

import pytest

import great_expectations as gx
from great_expectations.core.batch_config import BatchConfig
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.validation_config import ValidationConfig


@pytest.mark.unit
def test_validation_config_serializes_params_into_ids():
    _ = gx.get_context(mode="ephemeral")

    data_name = "my_batch_config"
    data_id = "72da422e-0d92-4e2c-89de-9a634ed18c0d"

    suite_name = "my_suite"
    suite_id = "f665433d-a862-41d9-9a9c-8b165bbca1d0"

    validation_name = "my_validation_config"
    validation_id = "9ad3386f-ea2f-4495-bf72-5673eda4d1a9"

    data = BatchConfig(
        name=data_name,
        id=data_id,
    )
    suite = ExpectationSuite(name=suite_name, id=suite_id)

    validation_config = ValidationConfig(
        name=validation_name,
        data=data,
        suite=suite,
        id=validation_id,
    )

    actual = validation_config.json(models_as_dict=False)
    expected = {
        "name": "my_validation_config",
        "data": {
            "name": data_name,
            "id": data_id,
        },
        "suite": {
            "name": suite_name,
            "id": suite_id,
        },
        "id": validation_id,
    }

    assert json.loads(actual) == expected


def test_validation_config_deserializes_from_ids():
    pass
