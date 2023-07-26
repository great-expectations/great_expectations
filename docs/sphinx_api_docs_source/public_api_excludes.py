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
        reason="We now use get_context(), this method only exists for backward compatibility.",
        name="DataContext",
        filepath=pathlib.Path(
            "great_expectations/data_context/data_context/data_context.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="We now use get_context(), this method only exists for backward compatibility.",
        name="BaseDataContext",
        filepath=pathlib.Path(
            "great_expectations/data_context/data_context/base_data_context.py"
        ),
    ),
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
        reason="Exclude code from v2 API",
        filepath=pathlib.Path(
            "great_expectations/datasource/batch_kwargs_generator/batch_kwargs_generator.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        filepath=pathlib.Path(
            "great_expectations/datasource/batch_kwargs_generator/databricks_batch_kwargs_generator.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        filepath=pathlib.Path(
            "great_expectations/datasource/batch_kwargs_generator/glob_reader_batch_kwargs_generator.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        filepath=pathlib.Path(
            "great_expectations/datasource/batch_kwargs_generator/manual_batch_kwargs_generator.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        filepath=pathlib.Path(
            "great_expectations/datasource/batch_kwargs_generator/query_batch_kwargs_generator.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        filepath=pathlib.Path(
            "great_expectations/datasource/batch_kwargs_generator/s3_batch_kwargs_generator.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        filepath=pathlib.Path(
            "great_expectations/datasource/batch_kwargs_generator/s3_subdir_reader_batch_kwargs_generator.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        filepath=pathlib.Path(
            "great_expectations/datasource/batch_kwargs_generator/subdir_reader_batch_kwargs_generator.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        filepath=pathlib.Path(
            "great_expectations/datasource/batch_kwargs_generator/table_batch_kwargs_generator.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="ValidationOperators are now run from Checkpoints: https://docs.greatexpectations.io/docs/guides/miscellaneous/migration_guide#manually-migrate-v2-checkpoints-to-v3-checkpoints",
        filepath=pathlib.Path(
            "great_expectations/validation_operators/types/validation_operator_result.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="ValidationOperators are now run from Checkpoints: https://docs.greatexpectations.io/docs/guides/miscellaneous/migration_guide#manually-migrate-v2-checkpoints-to-v3-checkpoints",
        filepath=pathlib.Path(
            "great_expectations/validation_operators/validation_operators.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="ValidationActions are now run from Checkpoints: https://docs.greatexpectations.io/docs/guides/miscellaneous/migration_guide#manually-migrate-v2-checkpoints-to-v3-checkpoints",
        name="run",
        filepath=pathlib.Path("great_expectations/checkpoint/actions.py"),
    ),
    IncludeExcludeDefinition(
        reason="CLI internal methods should not be part of the public API",
        filepath=pathlib.Path("great_expectations/cli/datasource.py"),
    ),
    IncludeExcludeDefinition(
        reason="CLI internal methods should not be part of the public API",
        filepath=pathlib.Path("great_expectations/cli/toolkit.py"),
    ),
    IncludeExcludeDefinition(
        reason="False match for from datasource_configuration_test_utilities import is_subset",
        name="is_subset",
        filepath=pathlib.Path("great_expectations/core/domain.py"),
    ),
    IncludeExcludeDefinition(
        reason="Already captured in the Data Context",
        name="test_yaml_config",
        filepath=pathlib.Path(
            "great_expectations/data_context/config_validator/yaml_config_validator.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for validator.get_metric()",
        name="get_metric",
        filepath=pathlib.Path(
            "great_expectations/core/expectation_validation_result.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for context.get_expectation_suite()",
        name="get_expectation_suite",
        filepath=pathlib.Path("great_expectations/data_asset/data_asset.py"),
    ),
    IncludeExcludeDefinition(
        reason="False match for context.save_expectation_suite() and validator.save_expectation_suite()",
        name="save_expectation_suite",
        filepath=pathlib.Path("great_expectations/data_asset/data_asset.py"),
    ),
    IncludeExcludeDefinition(
        reason="False match for validator.validate()",
        name="validate",
        filepath=pathlib.Path("great_expectations/data_asset/data_asset.py"),
    ),
    IncludeExcludeDefinition(
        reason="False match for validator.validate()",
        name="validate",
        filepath=pathlib.Path(
            "great_expectations/datasource/data_connector/batch_filter.py"
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
        filepath=pathlib.Path("great_expectations/data_context/store/query_store.py"),
    ),
    IncludeExcludeDefinition(
        reason="False match for python `set()`",
        name="set",
        filepath=pathlib.Path("great_expectations/data_context/store/query_store.py"),
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
            "great_expectations/data_context/store/validations_store.py"
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
        reason="Exclude code from v2 API",
        name="expect_column_values_to_be_between",
        filepath=pathlib.Path("great_expectations/dataset/dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        name="expect_column_values_to_not_be_null",
        filepath=pathlib.Path("great_expectations/dataset/dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        name="expect_table_row_count_to_be_between",
        filepath=pathlib.Path("great_expectations/dataset/dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        name="expect_column_values_to_be_between",
        filepath=pathlib.Path("great_expectations/dataset/pandas_dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        name="expect_column_values_to_not_be_null",
        filepath=pathlib.Path("great_expectations/dataset/pandas_dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        name="expect_column_values_to_be_between",
        filepath=pathlib.Path("great_expectations/dataset/sparkdf_dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        name="expect_column_values_to_not_be_null",
        filepath=pathlib.Path("great_expectations/dataset/sparkdf_dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        name="head",
        filepath=pathlib.Path("great_expectations/dataset/sparkdf_dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        name="expect_column_values_to_be_between",
        filepath=pathlib.Path("great_expectations/dataset/sqlalchemy_dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        name="expect_column_values_to_not_be_null",
        filepath=pathlib.Path("great_expectations/dataset/sqlalchemy_dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        name="head",
        filepath=pathlib.Path("great_expectations/dataset/sqlalchemy_dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="self_check is mentioned but in the docs we currently recommend using test_yaml_config which uses self_check under the hood. E.g. https://docs.greatexpectations.io/docs/guides/setup/configuring_data_contexts/how_to_configure_datacontext_components_using_test_yaml_config/#steps",
        name="self_check",
        filepath=pathlib.Path("great_expectations/checkpoint/checkpoint.py"),
    ),
    IncludeExcludeDefinition(
        reason="self_check is mentioned but in the docs we currently recommend using test_yaml_config which uses self_check under the hood. E.g. https://docs.greatexpectations.io/docs/guides/setup/configuring_data_contexts/how_to_configure_datacontext_components_using_test_yaml_config/#steps",
        name="self_check",
        filepath=pathlib.Path(
            "great_expectations/datasource/data_connector/data_connector.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="self_check is mentioned but in the docs we currently recommend using test_yaml_config which uses self_check under the hood. E.g. https://docs.greatexpectations.io/docs/guides/setup/configuring_data_contexts/how_to_configure_datacontext_components_using_test_yaml_config/#steps",
        name="self_check",
        filepath=pathlib.Path(
            "great_expectations/datasource/data_connector/runtime_data_connector.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="self_check is mentioned but in the docs we currently recommend using test_yaml_config which uses self_check under the hood. E.g. https://docs.greatexpectations.io/docs/guides/setup/configuring_data_contexts/how_to_configure_datacontext_components_using_test_yaml_config/#steps",
        name="self_check",
        filepath=pathlib.Path("great_expectations/datasource/new_datasource.py"),
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
        reason="False match for Python `dict`",
        name="dict",
        filepath=pathlib.Path("great_expectations/render/renderer_configuration.py"),
    ),
    IncludeExcludeDefinition(
        reason="False match for context.get_validator()",
        name="get_validator",
        filepath=pathlib.Path(
            "great_expectations/rule_based_profiler/domain_builder/domain_builder.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for context.get_validator()",
        name="get_validator",
        filepath=pathlib.Path("great_expectations/rule_based_profiler/helpers/util.py"),
    ),
    IncludeExcludeDefinition(
        reason="False match for context.get_validator()",
        name="get_validator",
        filepath=pathlib.Path(
            "great_expectations/rule_based_profiler/parameter_builder/parameter_builder.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Not used directly but from Data Assistant or RuleBasedProfiler",
        name="run",
        filepath=pathlib.Path("great_expectations/rule_based_profiler/rule/rule.py"),
    ),
    IncludeExcludeDefinition(
        reason="self_check is mentioned but in the docs we currently recommend using test_yaml_config which uses self_check under the hood. E.g. https://docs.greatexpectations.io/docs/guides/setup/configuring_data_contexts/how_to_configure_datacontext_components_using_test_yaml_config/#steps",
        name="self_check",
        filepath=pathlib.Path(
            "great_expectations/rule_based_profiler/rule_based_profiler.py"
        ),
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
        reason="v2 API",
        name="expect_column_mean_to_be_between",
        filepath=pathlib.Path("great_expectations/dataset/dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="v2 API",
        name="expect_column_values_to_be_in_set",
        filepath=pathlib.Path("great_expectations/dataset/dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="v2 API",
        name="expect_column_values_to_be_in_set",
        filepath=pathlib.Path("great_expectations/dataset/pandas_dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="v2 API",
        name="expect_column_values_to_be_in_set",
        filepath=pathlib.Path("great_expectations/dataset/sparkdf_dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="v2 API",
        name="expect_column_values_to_be_in_set",
        filepath=pathlib.Path("great_expectations/dataset/sqlalchemy_dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="to_json_dict is an internal helper method",
        name="to_json_dict",
        filepath=pathlib.Path("great_expectations/expectations/row_conditions.py"),
    ),
    IncludeExcludeDefinition(
        reason="to_json_dict is an internal helper method",
        name="to_json_dict",
        filepath=pathlib.Path(
            "great_expectations/rule_based_profiler/attributed_resolved_metrics.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="to_json_dict is an internal helper method",
        name="to_json_dict",
        filepath=pathlib.Path("great_expectations/rule_based_profiler/builder.py"),
    ),
    IncludeExcludeDefinition(
        reason="to_json_dict is an internal helper method",
        name="to_json_dict",
        filepath=pathlib.Path("great_expectations/rule_based_profiler/config/base.py"),
    ),
    IncludeExcludeDefinition(
        reason="to_json_dict is an internal helper method",
        name="to_json_dict",
        filepath=pathlib.Path(
            "great_expectations/rule_based_profiler/estimators/numeric_range_estimation_result.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="to_json_dict is an internal helper method",
        name="to_json_dict",
        filepath=pathlib.Path(
            "great_expectations/rule_based_profiler/estimators/numeric_range_estimator.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="to_json_dict is an internal helper method",
        name="to_json_dict",
        filepath=pathlib.Path(
            "great_expectations/rule_based_profiler/helpers/cardinality_checker.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="to_json_dict is an internal helper method",
        name="to_json_dict",
        filepath=pathlib.Path(
            "great_expectations/rule_based_profiler/helpers/configuration_reconciliation.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="to_json_dict is an internal helper method",
        name="to_json_dict",
        filepath=pathlib.Path(
            "great_expectations/rule_based_profiler/helpers/runtime_environment.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="to_json_dict is an internal helper method",
        name="to_json_dict",
        filepath=pathlib.Path(
            "great_expectations/rule_based_profiler/parameter_container.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="to_json_dict is an internal helper method",
        name="to_json_dict",
        filepath=pathlib.Path("great_expectations/rule_based_profiler/rule/rule.py"),
    ),
    IncludeExcludeDefinition(
        reason="to_json_dict is an internal helper method",
        name="to_json_dict",
        filepath=pathlib.Path("great_expectations/types/attributes.py"),
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
        reason="False match for DataAssistant.run()",
        name="run",
        filepath=pathlib.Path(
            "great_expectations/rule_based_profiler/data_assistant/data_assistant_runner.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="LegacyDatasource is not included in the public API",
        name="get_available_data_asset_names",
        filepath=pathlib.Path("great_expectations/datasource/datasource.py"),
    ),
    IncludeExcludeDefinition(
        reason="LegacyDatasource is not included in the public API",
        name="get_batch",
        filepath=pathlib.Path("great_expectations/datasource/sqlalchemy_datasource.py"),
    ),
    IncludeExcludeDefinition(
        reason="LegacyDatasource is not included in the public API",
        name="get_batch",
        filepath=pathlib.Path("great_expectations/datasource/sparkdf_datasource.py"),
    ),
    IncludeExcludeDefinition(
        reason="LegacyDatasource is not included in the public API",
        name="get_batch",
        filepath=pathlib.Path("great_expectations/datasource/pandas_datasource.py"),
    ),
    IncludeExcludeDefinition(
        reason="LegacyDatasource is not included in the public API",
        name="get_batch",
        filepath=pathlib.Path("great_expectations/datasource/datasource.py"),
    ),
    IncludeExcludeDefinition(
        reason="Deprecated v2 api Dataset is not included in the public API",
        filepath=pathlib.Path("great_expectations/dataset/dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="Validate method on custom type not included in the public API",
        name="validate",
        filepath=pathlib.Path(
            "great_expectations/datasource/fluent/serializable_types/pyspark.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Close is not included in the public API.",
        name="close",
        filepath=pathlib.Path("great_expectations/agent/message_service/subscriber.py"),
    ),
    IncludeExcludeDefinition(
        reason="Run is not included in the public API.",
        name="run",
        filepath=pathlib.Path("great_expectations/agent/agent.py"),
    ),
    IncludeExcludeDefinition(
        reason="Close is not included in the public API.",
        name="close",
        filepath=pathlib.Path(
            "great_expectations/agent/message_service/asyncio_rabbit_mq_client.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Close is not included in the public API.",
        name="run",
        filepath=pathlib.Path(
            "great_expectations/agent/message_service/asyncio_rabbit_mq_client.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Run is not included in the public API.",
        name="run",
        filepath=pathlib.Path(
            "great_expectations/agent/actions/data_assistants/run_onboarding_data_assistant.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Run is not included in the public API.",
        name="run",
        filepath=pathlib.Path(
            "great_expectations/agent/actions/data_assistants/run_missingness_data_assistant.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Run is not included in the public API.",
        name="build_batch_request",
        filepath=pathlib.Path(
            "great_expectations/agent/actions/data_assistants/utils.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Run is not included in the public API.",
        name="run",
        filepath=pathlib.Path("great_expectations/agent/actions/agent_action.py"),
    ),
    IncludeExcludeDefinition(
        reason='The "columns()" property in this module is not included in the public API',
        name="columns",
        filepath=pathlib.Path("great_expectations/datasource/fluent/sql_datasource.py"),
    ),
    IncludeExcludeDefinition(
        reason='The "columns()" property in this module is not included in the public API',
        name="columns",
        filepath=pathlib.Path(
            "great_expectations/datasource/fluent/spark_generic_splitters.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="The run method shares a name with a public API method",
        name="run",
        filepath=pathlib.Path(
            "great_expectations/agent/actions/run_column_descriptive_metrics_action.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="The run method shares a name with a public API method",
        name="run",
        filepath=pathlib.Path("great_expectations/agent/actions/list_table_names.py"),
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
]
