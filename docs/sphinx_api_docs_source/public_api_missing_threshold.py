"""Methods and classes that are not marked with the @public_api decorator but may appear in our public docs are listed here.

Over time this list should be driven to 0 by either adding the @public_api decorator and an appropriate docstring or
adding an exclude directive to docs/sphinx_api_docs_source/public_api_excludes.py
"""

ITEMS_IGNORED_FROM_PUBLIC_API = [
    "File: great_expectations/_docs_decorators.py Name: add",
    "File: great_expectations/checkpoint/actions.py Name: _run",
    "File: great_expectations/checkpoint/actions.py Name: run",
    "File: great_expectations/checkpoint/actions.py Name: update",
    "File: great_expectations/checkpoint/checkpoint.py Name: describe_dict",
    "File: great_expectations/compatibility/not_imported.py Name: is_version_greater_or_equal",
    "File: great_expectations/compatibility/typing_extensions.py Name: override",
    "File: great_expectations/core/batch.py Name: head",
    "File: great_expectations/core/batch_definition.py Name: build_batch_request",
    "File: great_expectations/core/expectation_diagnostics/expectation_doctor.py Name: print_diagnostic_checklist",
    "File: great_expectations/core/expectation_diagnostics/expectation_doctor.py Name: run_diagnostics",
    "File: great_expectations/core/expectation_suite.py Name: add_expectation_configuration",
    "File: great_expectations/core/expectation_suite.py Name: remove_expectation",
    "File: great_expectations/core/expectation_validation_result.py Name: describe_dict",
    "File: great_expectations/core/factory/factory.py Name: add",
    "File: great_expectations/core/factory/factory.py Name: all",
    "File: great_expectations/core/factory/factory.py Name: get",
    "File: great_expectations/core/metric_domain_types.py Name: MetricDomainTypes",
    "File: great_expectations/core/metric_function_types.py Name: MetricPartialFunctionTypes",
    "File: great_expectations/core/partitioners.py Name: ColumnPartitionerMonthly",
    "File: great_expectations/core/partitioners.py Name: PartitionerColumnValue",
    "File: great_expectations/core/yaml_handler.py Name: YAMLHandler",
    "File: great_expectations/core/yaml_handler.py Name: dump",
    "File: great_expectations/core/yaml_handler.py Name: load",
    "File: great_expectations/data_context/data_context/abstract_data_context.py Name: AbstractDataContext",
    "File: great_expectations/data_context/data_context/abstract_data_context.py Name: add_store",
    "File: great_expectations/data_context/data_context/abstract_data_context.py Name: delete_datasource",
    "File: great_expectations/data_context/data_context/abstract_data_context.py Name: get_docs_sites_urls",
    "File: great_expectations/data_context/data_context/abstract_data_context.py Name: get_validator",
    "File: great_expectations/data_context/data_context/abstract_data_context.py Name: list_datasources",
    "File: great_expectations/data_context/data_context/abstract_data_context.py Name: open_data_docs",
    "File: great_expectations/data_context/data_context/context_factory.py Name: build_data_docs",
    "File: great_expectations/data_context/data_context/context_factory.py Name: get_context",
    "File: great_expectations/data_context/data_context/context_factory.py Name: get_docs_sites_urls",
    "File: great_expectations/data_context/data_context/context_factory.py Name: get_validator",
    "File: great_expectations/data_context/data_context_variables.py Name: save",
    "File: great_expectations/data_context/store/_store_backend.py Name: add",
    "File: great_expectations/data_context/store/_store_backend.py Name: update",
    "File: great_expectations/data_context/store/checkpoint_store.py Name: CheckpointStore",
    "File: great_expectations/data_context/store/database_store_backend.py Name: DatabaseStoreBackend",
    "File: great_expectations/data_context/store/expectations_store.py Name: ExpectationsStore",
    "File: great_expectations/data_context/store/metric_store.py Name: MetricStore",
    "File: great_expectations/data_context/store/query_store.py Name: SqlAlchemyQueryStore",
    "File: great_expectations/data_context/store/store.py Name: add",
    "File: great_expectations/data_context/store/store.py Name: update",
    "File: great_expectations/data_context/store/tuple_store_backend.py Name: TupleAzureBlobStoreBackend",
    "File: great_expectations/data_context/store/tuple_store_backend.py Name: TupleFilesystemStoreBackend",
    "File: great_expectations/data_context/store/tuple_store_backend.py Name: TupleGCSStoreBackend",
    "File: great_expectations/data_context/store/tuple_store_backend.py Name: TupleS3StoreBackend",
    "File: great_expectations/data_context/store/validation_definition_store.py Name: ValidationDefinitionStore",
    "File: great_expectations/data_context/store/validation_results_store.py Name: ValidationResultsStore",
    "File: great_expectations/data_context/types/base.py Name: update",
    "File: great_expectations/data_context/types/resource_identifiers.py Name: GXCloudIdentifier",
    "File: great_expectations/datasource/datasource_dict.py Name: add_dataframe_asset",
    "File: great_expectations/datasource/fluent/config.py Name: yaml",
    "File: great_expectations/datasource/fluent/config_str.py Name: ConfigStr",
    "File: great_expectations/datasource/fluent/data_asset/path/dataframe_partitioners.py Name: columns",
    "File: great_expectations/datasource/fluent/data_asset/path/directory_asset.py Name: build_batch_request",
    "File: great_expectations/datasource/fluent/data_asset/path/directory_asset.py Name: get_batch_parameters_keys",
    "File: great_expectations/datasource/fluent/data_asset/path/file_asset.py Name: build_batch_request",
    "File: great_expectations/datasource/fluent/data_asset/path/file_asset.py Name: get_batch_parameters_keys",
    "File: great_expectations/datasource/fluent/data_asset/path/path_data_asset.py Name: get_batch",
    "File: great_expectations/datasource/fluent/data_asset/path/path_data_asset.py Name: get_batch_parameters_keys",
    "File: great_expectations/datasource/fluent/data_connector/batch_filter.py Name: validate",
    "File: great_expectations/datasource/fluent/fabric.py Name: build_batch_request",
    "File: great_expectations/datasource/fluent/fabric.py Name: get_batch",
    "File: great_expectations/datasource/fluent/fluent_base_model.py Name: yaml",
    "File: great_expectations/datasource/fluent/invalid_datasource.py Name: build_batch_request",
    "File: great_expectations/datasource/fluent/invalid_datasource.py Name: get_asset",
    "File: great_expectations/datasource/fluent/invalid_datasource.py Name: get_batch",
    "File: great_expectations/datasource/fluent/invalid_datasource.py Name: get_batch_parameters_keys",
    "File: great_expectations/datasource/fluent/pandas_datasource.py Name: build_batch_request",
    "File: great_expectations/datasource/fluent/pandas_datasource.py Name: get_batch",
    "File: great_expectations/datasource/fluent/sources.py Name: delete_datasource",
    "File: great_expectations/datasource/fluent/spark_datasource.py Name: build_batch_request",
    "File: great_expectations/datasource/fluent/spark_datasource.py Name: get_batch",
    "File: great_expectations/datasource/fluent/sql_datasource.py Name: build_batch_request",
    "File: great_expectations/datasource/fluent/sql_datasource.py Name: get_batch",
    "File: great_expectations/exceptions/exceptions.py Name: DataContextError",
    "File: great_expectations/exceptions/exceptions.py Name: InvalidExpectationConfigurationError",
    "File: great_expectations/execution_engine/execution_engine.py Name: ExecutionEngine",
    "File: great_expectations/execution_engine/execution_engine.py Name: get_compute_domain",
    "File: great_expectations/execution_engine/pandas_execution_engine.py Name: PandasExecutionEngine",
    "File: great_expectations/execution_engine/pandas_execution_engine.py Name: get_compute_domain",
    "File: great_expectations/execution_engine/sparkdf_execution_engine.py Name: SparkDFExecutionEngine",
    "File: great_expectations/execution_engine/sparkdf_execution_engine.py Name: get_compute_domain",
    "File: great_expectations/execution_engine/sqlalchemy_execution_engine.py Name: SqlAlchemyExecutionEngine",
    "File: great_expectations/execution_engine/sqlalchemy_execution_engine.py Name: execute_query",
    "File: great_expectations/execution_engine/sqlalchemy_execution_engine.py Name: get_compute_domain",
    "File: great_expectations/expectations/core/expect_column_max_to_be_between.py Name: ExpectColumnMaxToBeBetween",
    "File: great_expectations/expectations/core/expect_column_to_exist.py Name: ExpectColumnToExist",
    "File: great_expectations/expectations/core/expect_column_values_to_be_in_type_list.py Name: ExpectColumnValuesToBeInTypeList",
    "File: great_expectations/expectations/core/expect_column_values_to_be_null.py Name: ExpectColumnValuesToBeNull",
    "File: great_expectations/expectations/core/expect_column_values_to_be_of_type.py Name: ExpectColumnValuesToBeOfType",
    "File: great_expectations/expectations/core/expect_table_column_count_to_be_between.py Name: ExpectTableColumnCountToBeBetween",
    "File: great_expectations/expectations/core/expect_table_column_count_to_equal.py Name: ExpectTableColumnCountToEqual",
    "File: great_expectations/expectations/core/expect_table_columns_to_match_ordered_list.py Name: ExpectTableColumnsToMatchOrderedList",
    "File: great_expectations/expectations/core/expect_table_columns_to_match_set.py Name: ExpectTableColumnsToMatchSet",
    "File: great_expectations/expectations/core/expect_table_row_count_to_be_between.py Name: ExpectTableRowCountToBeBetween",
    "File: great_expectations/expectations/core/expect_table_row_count_to_equal.py Name: ExpectTableRowCountToEqual",
    "File: great_expectations/expectations/core/expect_table_row_count_to_equal_other_table.py Name: ExpectTableRowCountToEqualOtherTable",
    "File: great_expectations/expectations/core/unexpected_rows_expectation.py Name: UnexpectedRowsExpectation",
    "File: great_expectations/expectations/expectation.py Name: ColumnAggregateExpectation",
    "File: great_expectations/expectations/expectation.py Name: ColumnMapExpectation",
    "File: great_expectations/expectations/expectation.py Name: UnexpectedRowsExpectation",
    "File: great_expectations/expectations/expectation.py Name: render_suite_parameter_string",
    "File: great_expectations/expectations/expectation.py Name: validate_configuration",
    "File: great_expectations/expectations/expectation_configuration.py Name: ExpectationConfiguration",
    "File: great_expectations/expectations/expectation_configuration.py Name: to_domain_obj",
    "File: great_expectations/expectations/expectation_configuration.py Name: type",
    "File: great_expectations/expectations/metrics/column_aggregate_metric_provider.py Name: ColumnAggregateMetricProvider",
    "File: great_expectations/expectations/metrics/column_aggregate_metric_provider.py Name: column_aggregate_partial",
    "File: great_expectations/expectations/metrics/column_aggregate_metric_provider.py Name: column_aggregate_value",
    "File: great_expectations/expectations/metrics/map_metric_provider/column_condition_partial.py Name: column_condition_partial",
    "File: great_expectations/expectations/metrics/map_metric_provider/column_map_metric_provider.py Name: ColumnMapMetricProvider",
    "File: great_expectations/expectations/metrics/metric_provider.py Name: MetricProvider",
    "File: great_expectations/expectations/metrics/metric_provider.py Name: metric_partial",
    "File: great_expectations/expectations/metrics/metric_provider.py Name: metric_value",
    "File: great_expectations/expectations/model_field_types.py Name: validate",
    "File: great_expectations/expectations/regex_based_column_map_expectation.py Name: validate_configuration",
    "File: great_expectations/expectations/set_based_column_map_expectation.py Name: validate_configuration",
    "File: great_expectations/experimental/metric_repository/metric_retriever.py Name: get_validator",
    "File: great_expectations/experimental/rule_based_profiler/helpers/util.py Name: build_batch_request",
    "File: great_expectations/experimental/rule_based_profiler/rule_based_profiler.py Name: run",
    "File: great_expectations/render/components.py Name: validate",
    "File: great_expectations/render/renderer/email_renderer.py Name: EmailRenderer",
    "File: great_expectations/render/renderer/microsoft_teams_renderer.py Name: MicrosoftTeamsRenderer",
    "File: great_expectations/render/renderer/opsgenie_renderer.py Name: OpsgenieRenderer",
    "File: great_expectations/render/renderer/renderer.py Name: renderer",
    "File: great_expectations/render/renderer/site_builder.py Name: DefaultSiteIndexBuilder",
    "File: great_expectations/render/renderer/site_builder.py Name: SiteBuilder",
    "File: great_expectations/render/renderer/slack_renderer.py Name: SlackRenderer",
    "File: great_expectations/render/util.py Name: handle_strict_min_max",
    "File: great_expectations/render/util.py Name: num_to_str",
    "File: great_expectations/render/util.py Name: parse_row_condition_string_pandas_engine",
    "File: great_expectations/render/util.py Name: substitute_none_for_missing",
    "File: great_expectations/validator/metric_configuration.py Name: MetricConfiguration",
    "File: great_expectations/validator/metrics_calculator.py Name: columns",
    "File: great_expectations/validator/validation_graph.py Name: resolve",
    "File: great_expectations/validator/validator.py Name: columns",
    "File: great_expectations/validator/validator.py Name: head",
    "File: great_expectations/validator/validator.py Name: remove_expectation",
    "File: great_expectations/validator/validator.py Name: save_expectation_suite",
    "File: great_expectations/validator/validator.py Name: validate",
]
