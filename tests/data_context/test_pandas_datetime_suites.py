import pandas as pd
from tempfile import TemporaryDirectory
import great_expectations as ge
from great_expectations.dataset import PandasDataset
from great_expectations.data_context.data_context import ExpectationSuite
import os
import datetime


def test_save_expectation_suite_with_datetime_objects(data_context_parameterized_expectation_suite):
    # create datetime evaluation parameters
    evaluation_params = {"now": datetime.datetime.now(), "now_minus_48h": datetime.datetime.now() - datetime.timedelta(days=2)}
    test_data = {"data_refresh": [datetime.datetime.now(), datetime.datetime.now() - datetime.timedelta(days=1)]}
    test_df = pd.DataFrame(test_data)
    dataset_name = "test_pandas_source"

    with TemporaryDirectory() as tempdir:
        ge_path = os.path.join(tempdir, "great_expectations")
        ge.DataContext.create(tempdir, usage_statistics_enabled=False)
        context = ge.DataContext(ge_path)

        context.add_datasource(dataset_name, class_name="PandasDatasource")

        batch_kwargs = {"dataset": test_df, "datasource": dataset_name, "PandasInMemoryDF": True, "ge_batch_id": "test_id",}

        empty_suite = context.create_expectation_suite("test_suite")

        batch = context.get_batch(batch_kwargs=batch_kwargs, expectation_suite_name=empty_suite)
        for param in evaluation_params:
            batch.set_evaluation_parameter(param, evaluation_params[param])

        # Add expectation that will succeed using the datetime in a $PARAMETER
        batch.expect_column_max_to_be_between(column="data_refresh", min_value={"$PARAMETER": "now_minus_48h"})
        result = batch.validate()
        assert result.success
        batch.save_expectation_suite()
        assert isinstance(batch, PandasDataset)

        # Check that we can load the saved expectation suite
        reloaded_expectation_suite = context.get_expectation_suite("test_suite")
        assert isinstance(reloaded_expectation_suite, ExpectationSuite)

        # Run validation via the action_list_operator
        run_id = {
            "run_name": f"{dataset_name}_{datetime.datetime.now()}",
            "run_time": datetime.datetime.now(),
        }
        results = context.run_validation_operator(
            "action_list_operator", assets_to_validate=[batch], run_id=run_id,
            evaluation_parameters=evaluation_params
        )
        assert results.success

        # Check that we can build Data Docs
        index_page_locator_infos = context.build_data_docs()
        assert index_page_locator_infos["local_site"] == f"file://{ge_path}/uncommitted/data_docs/local_site/index.html"

        # Check that we can reload the expectation suite and validate
        reloaded_batch = context.get_batch(batch_kwargs=batch_kwargs, expectation_suite_name=reloaded_expectation_suite)

        run_id = {
            "run_name": f"reloaded_{dataset_name}_{datetime.datetime.now()}",
            "run_time": datetime.datetime.now(),
        }
        reloaded_results = context.run_validation_operator(
            "action_list_operator", assets_to_validate=[reloaded_batch], run_id=run_id,
        )

        assert reloaded_results.success
