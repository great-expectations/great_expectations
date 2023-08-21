import datetime
from tempfile import TemporaryDirectory

import pandas as pd
import pytest

from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.dataset import PandasDataset


@pytest.mark.filesystem
def test_save_expectation_suite_with_datetime_objects(
    data_context_parameterized_expectation_suite,
):
    # create datetime evaluation parameters
    evaluation_parameters = {
        "now": datetime.datetime.now(),
        "now_minus_48h": datetime.datetime.now() - datetime.timedelta(days=2),
    }
    test_data = {
        "data_refresh": [
            datetime.datetime.now(),
            datetime.datetime.now() - datetime.timedelta(days=1),
        ]
    }
    test_df = pd.DataFrame(test_data)
    dataset_name = "test_pandas_source"

    with TemporaryDirectory():
        context = data_context_parameterized_expectation_suite
        ge_path = context.root_directory

        context.add_datasource(dataset_name, class_name="PandasDatasource")

        batch_kwargs = {
            "dataset": test_df,
            "datasource": dataset_name,
            "PandasInMemoryDF": True,
            "ge_batch_id": "test_id",
        }

        empty_suite = context.add_expectation_suite("test_suite")

        batch = context._get_batch_v2(
            batch_kwargs=batch_kwargs, expectation_suite_name=empty_suite
        )
        for param in evaluation_parameters:
            batch.set_evaluation_parameter(param, evaluation_parameters[param])

        # Add expectation that will succeed using the datetime in a $PARAMETER
        batch.expect_column_max_to_be_between(
            column="data_refresh", min_value={"$PARAMETER": "now_minus_48h"}
        )
        result = batch.validate()
        assert result.success
        batch.save_expectation_suite()
        assert isinstance(batch, PandasDataset)

        # Check that we can load the saved expectation suite
        reloaded_expectation_suite = context.get_expectation_suite("test_suite")
        assert isinstance(reloaded_expectation_suite, ExpectationSuite)

        # Check that we can build Data Docs
        index_page_locator_infos = context.build_data_docs()
        assert (
            index_page_locator_infos["local_site"]
            == f"file://{ge_path}/uncommitted/data_docs/local_site/index.html"
        )
