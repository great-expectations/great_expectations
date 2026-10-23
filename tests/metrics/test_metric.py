import pandas as pd
import pytest

import great_expectations as gx
from great_expectations.compatibility.pydantic import ValidationError
from great_expectations.core.types import Comparable
from great_expectations.metrics.column import ColumnMetric
from great_expectations.metrics.column.mean import ColumnMean
from great_expectations.metrics.metric import AbstractClassInstantiationError, Metric
from great_expectations.metrics.metric_results import MetricResult
from great_expectations.validator.metric_configuration import (
    MetricConfiguration,
    MetricConfigurationID,
)

BATCH_ID = "my_data_source-my_data_asset-year_2025"
COLUMN = "my_column"

FULLY_QUALIFIED_METRIC_NAME = "column_values.above"


class ColumnValuesAboveResult(MetricResult[bool]): ...


class ColumnValuesAbove(ColumnMetric[ColumnValuesAboveResult]):
    name = FULLY_QUALIFIED_METRIC_NAME

    min_value: Comparable
    strict_min: bool = False


class TestMetric:
    @pytest.mark.unit
    def test_metric_instantiation_raises(self):
        with pytest.raises(AbstractClassInstantiationError):
            Metric(column=COLUMN)


class TestMetricDefinition:
    @pytest.mark.unit
    def test_success(self):
        class MyColumnValuesAbove(ColumnMetric[ColumnValuesAboveResult]):
            name = FULLY_QUALIFIED_METRIC_NAME

            min_value: Comparable
            strict_min: bool = False

    @pytest.mark.unit
    def test_success_without_generic_return_types(self):
        class MyColumnValuesAbove(ColumnMetric):
            name = FULLY_QUALIFIED_METRIC_NAME

            min_value: Comparable
            strict_min: bool = False


class TestMetricInstantiation:
    @pytest.mark.unit
    def test_instantiation_success(self):
        ColumnValuesAbove(
            column=COLUMN,
            min_value=42,
        )

    @pytest.mark.unit
    def test_instantiation_missing_domain_parameters_raises(self):
        with pytest.raises(ValidationError):
            ColumnValuesAbove(min_value=42)


class TestMetricConfig:
    @pytest.mark.unit
    def test_empty_batch_id_raises_with_param_name(self):
        metric = ColumnValuesAbove(column=COLUMN, min_value=42)
        with pytest.raises(ValueError, match=r"^batch_id must be a non-empty string\.$"):
            metric.config(batch_id="")

    @pytest.mark.unit
    def test_success(self):
        expected_config = MetricConfiguration(
            metric_name=FULLY_QUALIFIED_METRIC_NAME,
            metric_domain_kwargs={
                "batch_id": BATCH_ID,
                "row_condition": None,
                "condition_parser": None,
                "column": COLUMN,
            },
            metric_value_kwargs={
                "min_value": 42,
                "strict_min": False,
            },
        )

        actual_config = ColumnValuesAbove(
            column=COLUMN,
            min_value=42,
        ).config(batch_id=BATCH_ID)

        assert actual_config.metric_name == expected_config.metric_name
        assert actual_config.metric_domain_kwargs == expected_config.metric_domain_kwargs
        assert actual_config.metric_value_kwargs == expected_config.metric_value_kwargs
        assert isinstance(actual_config.id, MetricConfigurationID)


class TestMetricImmutability:
    @pytest.mark.unit
    def test_domain_kwarg_immutability_success(self):
        column_values_above = ColumnValuesAbove(
            column=COLUMN,
            min_value=42,
        )

        with pytest.raises(TypeError):
            column_values_above.column = "updated_column"

    @pytest.mark.unit
    def test_value_kwarg_immutability_success(self):
        column_values_above = ColumnValuesAbove(
            column=COLUMN,
            min_value=42,
        )

        with pytest.raises(TypeError):
            column_values_above.min_value = 42


class TestMetricIdFromBatch:
    @pytest.mark.unit
    def test_two_identical_metrics_with_same_batch_id_returns_same_metric_id(self):
        metric_1 = ColumnValuesAbove(
            column=COLUMN,
            min_value=42,
        )
        metric_2 = ColumnValuesAbove(
            column=COLUMN,
            min_value=42,
        )
        assert metric_1.metric_id_for_batch(BATCH_ID) == metric_2.metric_id_for_batch(BATCH_ID)

    @pytest.mark.unit
    def test_metric_with_different_value_kwargs_return_different_ids(self):
        metric_1 = ColumnValuesAbove(
            column=COLUMN,
            min_value=42,
        )
        metric_2 = ColumnValuesAbove(
            column=COLUMN,
            min_value=43,
        )
        assert metric_1.metric_id_for_batch(BATCH_ID) != metric_2.metric_id_for_batch(BATCH_ID)

    @pytest.mark.unit
    def test_metric_with_different_batch_ids_return_different_ids(self):
        metric_1 = ColumnValuesAbove(
            column=COLUMN,
            min_value=42,
        )
        metric_2 = ColumnValuesAbove(
            column=COLUMN,
            min_value=42,
        )
        assert metric_1.metric_id_for_batch(BATCH_ID) != metric_2.metric_id_for_batch(
            BATCH_ID + "_another"
        )


class TestComputeMetric:
    @pytest.mark.unit
    def test_same_metric_different_args_have_different_results(self):
        context = gx.get_context(mode="ephemeral")
        data_source = context.data_sources.add_pandas("Pandas Data Source")
        data_asset = data_source.add_dataframe_asset("DataFrame Asset")
        batch_definition = data_asset.add_batch_definition_whole_dataframe(
            "Whole DataFrame Batch Definition"
        )
        df = pd.DataFrame(
            {
                "a": [1, 2, 3],
                "b": [4, 5, 6],
            }
        )
        batch = batch_definition.get_batch({"dataframe": df})
        metrics = [
            ColumnMean(column="a"),
            ColumnMean(column="b"),
        ]
        metric_result = batch.compute_metrics(metrics)
        assert isinstance(metric_result, list)
        assert len(metric_result) == 2
        assert metric_result[0].value == 2.0
        assert metric_result[0].id == metrics[0].metric_id_for_batch(batch.id)
        assert metric_result[1].value == 5.0
        assert metric_result[1].id == metrics[1].metric_id_for_batch(batch.id)
        assert metric_result[0].id != metric_result[1].id

    @pytest.mark.unit
    def test_single_metric_as_list_result_is_list(self):
        context = gx.get_context(mode="ephemeral")
        data_source = context.data_sources.add_pandas("Pandas Data Source")
        data_asset = data_source.add_dataframe_asset("DataFrame Asset")
        batch_definition = data_asset.add_batch_definition_whole_dataframe(
            "Whole DataFrame Batch Definition"
        )
        df = pd.DataFrame(
            {
                "a": [1, 2, 3],
            }
        )
        batch = batch_definition.get_batch({"dataframe": df})
        metrics = [
            ColumnMean(column="a"),
        ]
        metric_result = batch.compute_metrics(metrics)
        assert isinstance(metric_result, list)
        assert len(metric_result) == 1
        assert metric_result[0].value == 2.0

    @pytest.mark.unit
    def test_single_metric_result_is_metric_result(self):
        context = gx.get_context(mode="ephemeral")
        data_source = context.data_sources.add_pandas("Pandas Data Source")
        data_asset = data_source.add_dataframe_asset("DataFrame Asset")
        batch_definition = data_asset.add_batch_definition_whole_dataframe(
            "Whole DataFrame Batch Definition"
        )
        df = pd.DataFrame(
            {
                "a": [1, 2, 3],
            }
        )
        batch = batch_definition.get_batch({"dataframe": df})
        metric_result = batch.compute_metrics(ColumnMean(column="a"))
        assert isinstance(metric_result, MetricResult)
        assert metric_result.value == 2.0
