import pandas as pd
import pytest

import great_expectations.expectations as gxe
from great_expectations.data_context.data_context.abstract_data_context import AbstractDataContext


class ExpectColumnValuesAsStringToBePositiveInteger(gxe.ExpectColumnValuesToMatchRegex):
    regex: str = "^\\d+$"


@pytest.mark.big
def test_expect_column_values_as_string_to_be_positive_integers_pass(
    empty_data_context: AbstractDataContext,
):
    df = pd.DataFrame({"a": ["1", "2", "3", "4", "5"]})
    data_asset = empty_data_context.data_sources.pandas_default.add_dataframe_asset("my_dataframe")
    batch = data_asset.add_batch_definition_whole_dataframe("my_batch_definition").get_batch(
        batch_parameters={"dataframe": df}
    )

    result = batch.validate(ExpectColumnValuesAsStringToBePositiveInteger(column="a"))

    assert result.success


@pytest.mark.big
def test_expect_column_values_as_string_to_be_positive_integers_fail(
    empty_data_context: AbstractDataContext,
):
    df = pd.DataFrame({"a": ["1", "2", "3", "4", "a"]})

    data_asset = empty_data_context.data_sources.pandas_default.add_dataframe_asset("my_dataframe")
    batch = data_asset.add_batch_definition_whole_dataframe("my_batch_definition").get_batch(
        batch_parameters={"dataframe": df}
    )

    result = batch.validate(ExpectColumnValuesAsStringToBePositiveInteger(column="a"))

    assert not result.success
