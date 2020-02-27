#!/usr/bin/env python3

import time

from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig


print("Beginning to construct a DataContext.")
config = DataContextConfig(
            config_version=1,
            datasources={},
            expectations_store_name="expectations",
            validations_store_name="validations",
            evaluation_parameter_store_name="evaluation_parameters",
            plugins_directory=None,
            validation_operators={},
            stores={
                "expectations": {"class_name": "ExpectationsStore"},
                "validations": {"class_name": "ValidationsStore"},
                "evaluation_parameters": {"class_name": "EvaluationParameterStore"}
            },
            data_docs_sites={},
            config_variables_file_path=None,
            telemetry_config={
                "enabled": True,
                "data_context_id": None,  # Leaving this as none causes a new id to be generated
                "telemetry_bucket": "priv.greatexpectations.telemetry"
            },
            commented_map=None,
        )
_ = BaseDataContext(config)

print("Done constructing a DataContext.")
time.sleep(30)
print("Ending a long nap.")
