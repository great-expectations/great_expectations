"""Methods, classes and files to be excluded from consideration as part of the public API.

Include here methods that share a name with another method for example (since we use string matching
to determine what is used in our documentation code snippets).
"""

from __future__ import annotations

import pathlib

from docs.sphinx_api_docs_source.include_exclude_definition import (
    IncludeExcludeDefinition,
)

DEFAULT_EXCLUDES: list[IncludeExcludeDefinition] = [
    IncludeExcludeDefinition(
        reason="Fluent is not part of the public API",
        filepath=pathlib.Path("great_expectations/datasource/fluent/interfaces.py"),
    ),
    IncludeExcludeDefinition(
        reason="Fluent is not part of the public API",
        name="read_csv",
        filepath=pathlib.Path("great_expectations/datasource/fluent/config.py"),
    ),
    IncludeExcludeDefinition(
        reason="Fluent-style read_csv is not referenced in the docs yet, but due to string matching it is being flagged.",
        name="read_csv",
        filepath=pathlib.Path(
            "great_expectations/datasource/fluent/pandas_datasource.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Marshmallow dump methods are not part of the public API",
        name="dump",
        filepath=pathlib.Path("great_expectations/data_context/types/base.py"),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from __init__.py",
        filepath=pathlib.Path("great_expectations/types/__init__.py"),
    ),
    IncludeExcludeDefinition(
        reason="False match for from datasource_configuration_test_utilities import is_subset",
        name="is_subset",
        filepath=pathlib.Path("great_expectations/core/domain.py"),
    ),
    IncludeExcludeDefinition(
        reason="False match for validator.get_metric()",
        name="get_metric",
        filepath=pathlib.Path(
            "great_expectations/core/expectation_validation_result.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Captured in AbstractDataContext",
        name="add_checkpoint",
        filepath=pathlib.Path(
            "great_expectations/data_context/data_context/cloud_data_context.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Captured in AbstractDataContext",
        name="create_expectation_suite",
        filepath=pathlib.Path(
            "great_expectations/data_context/data_context/cloud_data_context.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Captured in AbstractDataContext",
        name="get_expectation_suite",
        filepath=pathlib.Path(
            "great_expectations/data_context/data_context/cloud_data_context.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Captured in AbstractDataContext",
        name="list_checkpoints",
        filepath=pathlib.Path(
            "great_expectations/data_context/data_context/cloud_data_context.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Captured in AbstractDataContext",
        name="list_expectation_suite_names",
        filepath=pathlib.Path(
            "great_expectations/data_context/data_context/cloud_data_context.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Captured in AbstractDataContext",
        name="save_expectation_suite",
        filepath=pathlib.Path(
            "great_expectations/data_context/data_context/cloud_data_context.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Captured in AbstractDataContext",
        name="add_store",
        filepath=pathlib.Path(
            "great_expectations/data_context/data_context/file_data_context.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for python dict `.get()`",
        name="get",
        filepath=pathlib.Path(
            "great_expectations/data_context/store/_store_backend.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for python `set()`",
        name="set",
        filepath=pathlib.Path(
            "great_expectations/data_context/store/_store_backend.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for python `set()`",
        name="set",
        filepath=pathlib.Path(
            "great_expectations/data_context/store/datasource_store.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for python dict `.get()`",
        name="get",
        filepath=pathlib.Path(
            "great_expectations/data_context/store/expectations_store.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for python dict `.get()`",
        name="get",
        filepath=pathlib.Path(
            "great_expectations/data_context/store/html_site_store.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for python `set()`",
        name="set",
        filepath=pathlib.Path(
            "great_expectations/data_context/store/html_site_store.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for python dict `.get()`",
        name="get",
        filepath=pathlib.Path("great_expectations/data_context/store/store.py"),
    ),
    IncludeExcludeDefinition(
        reason="False match for python `set()`",
        name="set",
        filepath=pathlib.Path("great_expectations/data_context/store/store.py"),
    ),
    IncludeExcludeDefinition(
        reason="False match for context.add_checkpoint()",
        name="add_checkpoint",
        filepath=pathlib.Path(
            "great_expectations/data_context/store/checkpoint_store.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for context.list_checkpoints()",
        name="list_checkpoints",
        filepath=pathlib.Path(
            "great_expectations/data_context/store/checkpoint_store.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for datasource self_check",
        name="self_check",
        filepath=pathlib.Path(
            "great_expectations/data_context/store/configuration_store.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for datasource self_check",
        name="self_check",
        filepath=pathlib.Path(
            "great_expectations/data_context/store/expectations_store.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for datasource self_check",
        name="self_check",
        filepath=pathlib.Path(
            "great_expectations/data_context/store/html_site_store.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for datasource self_check",
        name="self_check",
        filepath=pathlib.Path(
            "great_expectations/data_context/store/json_site_store.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for datasource self_check",
        name="self_check",
        filepath=pathlib.Path("great_expectations/data_context/store/store.py"),
    ),
    IncludeExcludeDefinition(
        reason="False match for datasource self_check",
        name="self_check",
        filepath=pathlib.Path(
            "great_expectations/data_context/store/validation_results_store.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for yaml.dump()",
        name="dump",
        filepath=pathlib.Path("great_expectations/data_context/templates.py"),
    ),
    IncludeExcludeDefinition(
        reason="Helper method used in tests, not part of public API",
        name="file_relative_path",
        filepath=pathlib.Path("great_expectations/data_context/util.py"),
    ),
    IncludeExcludeDefinition(
        reason="False match for dict `.update()` method.",
        name="update",
        filepath=pathlib.Path(
            "great_expectations/execution_engine/execution_engine.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Included in Validator public api",
        name="head",
        filepath=pathlib.Path(
            "great_expectations/execution_engine/sparkdf_execution_engine.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Currently only used in testing code, not referenced in docs.",
        name="close",
        filepath=pathlib.Path(
            "great_expectations/execution_engine/sqlalchemy_execution_engine.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Internal test-findings writer; false match on the generic name `close`.",
        name="close",
        filepath=pathlib.Path(
            "great_expectations/core/validation_result_schemas/findings_emitter.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for Python `dict`",
        name="dict",
        filepath=pathlib.Path("great_expectations/render/renderer_configuration.py"),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        name="read_csv",
        filepath=pathlib.Path("great_expectations/util.py"),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        name="validate",
        filepath=pathlib.Path("great_expectations/util.py"),
    ),
    IncludeExcludeDefinition(
        reason="Included in Validator public api",
        name="get_metric",
        filepath=pathlib.Path("great_expectations/validator/metrics_calculator.py"),
    ),
    IncludeExcludeDefinition(
        reason="Included in Validator public api",
        name="head",
        filepath=pathlib.Path("great_expectations/validator/metrics_calculator.py"),
    ),
    IncludeExcludeDefinition(
        reason="False match for Python `Set.add()`",
        name="add",
        filepath=pathlib.Path("great_expectations/validator/validation_graph.py"),
    ),
    IncludeExcludeDefinition(
        reason="False match for Python dict `.update()`",
        name="update",
        filepath=pathlib.Path("great_expectations/validator/validation_graph.py"),
    ),
    IncludeExcludeDefinition(
        reason="to_json_dict is an internal helper method",
        name="to_json_dict",
        filepath=pathlib.Path("great_expectations/core/domain.py"),
    ),
    IncludeExcludeDefinition(
        reason="to_json_dict is an internal helper method",
        name="to_json_dict",
        filepath=pathlib.Path(
            "great_expectations/core/expectation_diagnostics/expectation_diagnostics.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Internal use",
        name="IDDict",
        filepath=pathlib.Path("great_expectations/core/id_dict.py"),
    ),
    IncludeExcludeDefinition(
        reason="to_json_dict is an internal helper method",
        name="to_json_dict",
        filepath=pathlib.Path("great_expectations/expectations/row_conditions.py"),
    ),
    IncludeExcludeDefinition(
        reason="to_json_dict is an internal helper method",
        name="to_json_dict",
        filepath=pathlib.Path("great_expectations/types/base.py"),
    ),
    IncludeExcludeDefinition(
        reason="Internal helper method",
        name="filter_properties_dict",
        filepath=pathlib.Path("great_expectations/util.py"),
    ),
    IncludeExcludeDefinition(
        reason="to_json_dict is an internal helper method",
        name="to_json_dict",
        filepath=pathlib.Path("great_expectations/validator/exception_info.py"),
    ),
    IncludeExcludeDefinition(
        reason="Validate method on custom type not included in the public API",
        name="validate",
        filepath=pathlib.Path(
            "great_expectations/datasource/fluent/serializable_types/pyspark.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason='The "columns()" property in this module is not included in the public API',
        name="columns",
        filepath=pathlib.Path("great_expectations/datasource/fluent/sql_datasource.py"),
    ),
    IncludeExcludeDefinition(
        reason="The add method shares a name with a public API method",
        name="add",
        filepath=pathlib.Path(
            "great_expectations/experimental/metric_repository/metric_repository.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="The add method shares a name with a public API method",
        name="add",
        filepath=pathlib.Path(
            "great_expectations/experimental/metric_repository/data_store.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="The add method shares a name with a public API method",
        name="add",
        filepath=pathlib.Path(
            "great_expectations/experimental/metric_repository/cloud_data_store.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Metric values are not included in the public API.",
        name="dict",
        filepath=pathlib.Path(
            "great_expectations/experimental/metric_repository/metrics.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Internal protocols are not included in the public API.",
        name="add_dataframe_asset",
        filepath=pathlib.Path("great_expectations/datasource/datasource_dict.py"),
    ),
    IncludeExcludeDefinition(
        reason="Not yet part of the public API",
        name="ResultFormat",
        filepath=pathlib.Path("great_expectations/validator/v1_validator.py"),
    ),
    IncludeExcludeDefinition(
        reason="Not yet part of the public API",
        name="ResultFormat",
        filepath=pathlib.Path("great_expectations/core/result_format.py"),
    ),
    IncludeExcludeDefinition(
        reason="This method shares a name with a public API method.",
        name="add_expectation",
        filepath=pathlib.Path(
            "great_expectations/data_context/store/expectations_store.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="This method shares a name with a public API method.",
        name="delete_expectation",
        filepath=pathlib.Path(
            "great_expectations/data_context/store/expectations_store.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="This method shares a name with a public API method.",
        name="delete",
        filepath=pathlib.Path(
            "great_expectations/data_context/store/datasource_store.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="",
        name="ExpectColumnValuesToBeInSet",
        filepath=pathlib.Path(
            "great_expectations/expectations/core/expect_column_values_to_be_in_set.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="We do not want Expectations in our API docs. Expectation docs live in the gallery.",
        name="ExpectColumnValuesToBeBetween",
        filepath=pathlib.Path(
            "great_expectations/expectations/core/expect_column_values_to_be_between.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="We do not want Expectations in our API docs. Expectation docs live in the gallery.",
        name="ExpectColumnValuesToNotBeNull",
        filepath=pathlib.Path(
            "great_expectations/expectations/core/expect_column_values_to_not_be_null.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="This method does not need to be accessed by users.",
        name="get_or_create_spark_session",
        filepath=pathlib.Path(
            "great_expectations/execution_engine/sparkdf_execution_engine.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="This method can be removed in 1.0",
        name="get_or_create_spark_application",
        filepath=pathlib.Path("great_expectations/core/util.py"),
    ),
    IncludeExcludeDefinition(
        reason="This method can be removed in 1.0",
        name="get_or_create_spark_session",
        filepath=pathlib.Path("great_expectations/core/util.py"),
    ),
    IncludeExcludeDefinition(
        reason="This method does not need to be accessed by users, and will eventually be removed from docs.",
        name="get_batch_parameters_keys",
        filepath=pathlib.Path(
            "great_expectations/datasource/fluent/pandas_datasource.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="This method does not need to be accessed by users, and will eventually be removed from docs.",
        name="get_batch_parameters_keys",
        filepath=pathlib.Path(
            "great_expectations/datasource/fluent/spark_datasource.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="This method does not need to be accessed by users, and will eventually be removed from docs.",
        name="get_batch_parameters_keys",
        filepath=pathlib.Path("great_expectations/datasource/fluent/sql_datasource.py"),
    ),
    IncludeExcludeDefinition(
        reason="This action is not currently supported",
        name="OpsgenieAlertAction",
        filepath=pathlib.Path("great_expectations/checkpoint/actions.py"),
    ),
    IncludeExcludeDefinition(
        reason="This action is not currently supported",
        name="PagerdutyAlertAction",
        filepath=pathlib.Path("great_expectations/checkpoint/actions.py"),
    ),
    IncludeExcludeDefinition(
        reason="This action is not currently supported",
        name="SNSNotificationAction",
        filepath=pathlib.Path("great_expectations/checkpoint/actions.py"),
    ),
]
