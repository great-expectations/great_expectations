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

    assert metric_no_value.key == \
    ('NamespaceAwareValidationMetric',
        NormalizedDataAssetName(datasource='my_dataset', generator='my_generator', generator_asset='my_asset'),
        '20190101__74d1a208bcf41091d60c9d333a85b82f',
      'column_value_count', (('column', 'Age'),))

    metric_with_value = NamespaceAwareValidationMetric(
        data_asset_name=NormalizedDataAssetName("my_dataset", "my_generator", "my_asset"),
        batch_fingerprint="20190101__74d1a208bcf41091d60c9d333a85b82f",
        metric_name="column_value_count",
        metric_kwargs={
            "column": "Age"
        },
        metric_value=30
    )


    assert metric_with_value.key == \
    ('NamespaceAwareValidationMetric',
                     NormalizedDataAssetName(datasource='my_dataset', generator='my_generator',
                                             generator_asset='my_asset'), '20190101__74d1a208bcf41091d60c9d333a85b82f',
                     'column_value_count', (('column', 'Age'),))

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

