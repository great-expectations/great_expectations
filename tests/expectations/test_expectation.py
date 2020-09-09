import pandas as pd

from great_expectations.core.expectation_configuration import \
    ExpectationConfiguration
from great_expectations.core.expectation_validation_result import \
    ExpectationValidationResult
from great_expectations.datasource import PandasDatasource
from great_expectations.validator.validator import DatasetValidator


def test_configuration_validation():
    df = pd.DataFrame({"PClass": [1, 2, 3, 4]})
    # maybe THIS is an "Expectation" and other is an "ExpectationImplementation"
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "PClass", "value_set": [1, 2, 3]},
    )
    res = expectation_configuration.validate(df)
    assert res == ExpectationValidationResult(success=False)


def test_dataset_validation():
    df = pd.DataFrame({"PClass": [1, 2, 3, 4]})
    datasource = PandasDatasource()
    batch = datasource.get_batch(batch_kwargs={"dataset": df})
    # Validator
    #    - Does not have a suite property that it maintains
    # InteractiveValidator
    #    - Does have a suite property that it maintains
    validator = DatasetValidator(batch=batch)
    res = validator.expect_column_values_to_be_in_set("PClass", [1, 2])
    assert res.success is False
