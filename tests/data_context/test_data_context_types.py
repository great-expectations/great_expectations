import pytest

from six import string_types

from great_expectations.data_context.types import NormalizedDataAssetName, NamespaceAwareValidationMetric


# noinspection PyPep8Naming
def test_NamespaceAwareValidationMetric():
    metric_no_value = NamespaceAwareValidationMetric(
        data_asset_name=NormalizedDataAssetName("my_dataset", "my_generator", "my_asset"),
        batch_fingerprint="20190101__74d1a208bcf41091d60c9d333a85b82f",
        metric_name="column_value_count",
        metric_kwargs={
            "column": "Age"
        }
    )

    assert metric_no_value.urn == \
           "urn:great_expectations:my_dataset/my_generator/my_asset:20190101__74d1a208bcf41091d60c9d333a85b82f:column_value_count:column=Age"

    metric_with_value = NamespaceAwareValidationMetric(
        data_asset_name=NormalizedDataAssetName("my_dataset", "my_generator", "my_asset"),
        batch_fingerprint="20190101__74d1a208bcf41091d60c9d333a85b82f",
        metric_name="column_value_count",
        metric_kwargs={
            "column": "Age"
        },
        metric_value=30
    )

    assert metric_with_value.urn == \
           "urn:great_expectations:my_dataset/my_generator/my_asset:20190101__74d1a208bcf41091d60c9d333a85b82f:column_value_count:column=Age:30"

    with pytest.raises(KeyError):
        extra_key_metric = NamespaceAwareValidationMetric(
            data_asset_name=NormalizedDataAssetName("my_dataset", "my_generator", "my_asset"),
            batch_fingerprint="20190101__74d1a208bcf41091d60c9d333a85b82f",
            metric_name="column_value_count",
            metric_kwargs={
                "column": "Age"
            },
            metric_value=30,
            not_a_key="lemmein"
        )

    with pytest.raises(KeyError):
        missing_key_metric = NamespaceAwareValidationMetric(
            data_asset_name=NormalizedDataAssetName("my_dataset", "my_generator", "my_asset"),
            batch_fingerprint="20190101__74d1a208bcf41091d60c9d333a85b82f",
            metric_kwargs={
                "column": "Age"
            },
            metric_value=30,
        )

    urn = "urn:great_expectations:my_dataset/my_generator/my_asset:20190101__74d1a208bcf41091d60c9d333a85b82f:column_value_count:column=Age&result_format=SUMMARY:30"
    metric = NamespaceAwareValidationMetric.from_urn(urn)
    assert metric.data_asset_name == NormalizedDataAssetName("my_dataset", "my_generator", "my_asset")
    assert metric.batch_fingerprint == "20190101__74d1a208bcf41091d60c9d333a85b82f"
    assert metric.metric_name == "column_value_count"
    assert metric.metric_kwargs == {
        "column": "Age",
        "result_format": "SUMMARY"
    }

    # NOTE: The metric_value lost its type in this round trip!
    assert metric.metric_value == "30"

    # To remedy that, we need to specify a metric type, and a default value of coerce types
    class IntegerNamespaceAwareValidationMetric(NamespaceAwareValidationMetric):
        _coerce_types = True
        _key_types = {
            "data_asset_name": NormalizedDataAssetName,
            "batch_fingerprint": string_types,
            "metric_name": string_types,
            "metric_kwargs": dict,
            "metric_value": int
        }

    urn = "urn:great_expectations:my_dataset/my_generator/my_asset:20190101__74d1a208bcf41091d60c9d333a85b82f:column_value_count:column=Age&result_format=SUMMARY:30"
    metric = IntegerNamespaceAwareValidationMetric.from_urn(urn)
    assert metric.data_asset_name == NormalizedDataAssetName("my_dataset", "my_generator", "my_asset")
    assert metric.batch_fingerprint == "20190101__74d1a208bcf41091d60c9d333a85b82f"
    assert metric.metric_name == "column_value_count"
    assert metric.metric_kwargs == {
        "column": "Age",
        "result_format": "SUMMARY"
    }
    # Note that now we have the correct type of metric_value
    assert metric.metric_value == 30

    with pytest.raises(ValueError):
        urn = "my_dataset/my_generator/my_asset:20190101__74d1a208bcf41091d60c9d333a85b82f:column_value_count:column=Age&result_format=SUMMARY:30"
        metric = NamespaceAwareValidationMetric.from_urn(urn)
