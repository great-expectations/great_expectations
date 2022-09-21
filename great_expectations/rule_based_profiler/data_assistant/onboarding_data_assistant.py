from typing import Any, Dict, List, Optional

from great_expectations.rule_based_profiler.config import ParameterBuilderConfig
from great_expectations.rule_based_profiler.data_assistant import DataAssistant
from great_expectations.rule_based_profiler.data_assistant.data_assistant import (
    build_map_metric_rule,
)
from great_expectations.rule_based_profiler.data_assistant_result import (
    DataAssistantResult,
    OnboardingDataAssistantResult,
)
from great_expectations.rule_based_profiler.domain import SemanticDomainTypes
from great_expectations.rule_based_profiler.domain_builder import (
    CategoricalColumnDomainBuilder,
    ColumnDomainBuilder,
    TableDomainBuilder,
)
from great_expectations.rule_based_profiler.expectation_configuration_builder import (
    DefaultExpectationConfigurationBuilder,
    ExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.helpers.cardinality_checker import (
    CardinalityLimitMode,
)
from great_expectations.rule_based_profiler.helpers.util import sanitize_parameter_name
from great_expectations.rule_based_profiler.parameter_builder import (
    MeanTableColumnsSetMatchMultiBatchParameterBuilder,
    MetricMultiBatchParameterBuilder,
    ParameterBuilder,
    ValueSetMultiBatchParameterBuilder,
)
from great_expectations.rule_based_profiler.parameter_container import (
    DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER,
    FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY,
    VARIABLES_KEY,
)
from great_expectations.rule_based_profiler.rule import Rule
from great_expectations.validator.validator import Validator


class OnboardingDataAssistant(DataAssistant):
    """
    OnboardingDataAssistant provides dataset exploration and validation to help with Great Expectations "Onboarding".

    OnboardingDataAssistant.run() Args:
        - batch_request (BatchRequestBase or dict):
        - estimation (NumericRangeEstimator, str, or None):
        - include_column_names:
        - exclude_column_names:
        - include_column_name_suffixes:
        - exclude_column_name_suffixes: ??????
        - semantic_type_filter_module_name:
        - semantic_type_filter_class_name:
        - max_unexpected_values:
        - max_unexpected_ratio:
        - min_max_unexpected_values_proportion:
        - allowed_semantic_types_passthrough:
        - cardinality_limit_mode:
        - max_unique_values:
        - max_proportion_unique:
        - table_rule (dict): This Rule will calculate metrics and if applicable, estimate Expectation parameters and produce Expectations for the table Domain.
            * false_positive_rate:
            * estimator:
            * n_resamples:
            * random_seed:
            * quantile_statistic_interpolation_method:
            * quantile_bias_correction:
            * quantile_bias_std_error_ratio_threshold:
            * include_estimator_samples_histogram_in_details:
            * truncate_values:
                - lower_bound:
                - upper_bound:
            * round_decimals:
            * exact_match:
            * success_ratio:
        - column_value_uniqueness_rule (dict):
            * success_ratio:
        - column_value_nullity_rule (dict):
            * success_ratio:
        - column_value_nonnullity_rule (dict):
            * success_ratio:
        - numeric_columns_rule (dict):
            * mostly:
            * strict_min:
            * strict_max:
            * quantiles:
            * allow_relative_error:
            * false_postiive_rate:
            * estimator:
            * n_resamples:
            * random_seed:
            * quantile_statistic_interpolation_method:
            * quantile_bias_correction:
            * quantile_bias_std_error_ratio_threshold:
            * include_estimator_samples_in_histogram_details:
            * truncate_values:
                - lower_bound:
                - upper_bound:
            * round_decimals:
        - datetime_columns_rule (dict):
            * mostly:
            * strict_min:
            * strict_max:
            * false_positive_rate:
            * estimator:
            * n_resamples:
            * random_seed:
            * quantile_statistic_interpolation_method:
            * quantile_bias_correction:
            * quantile_bias_std_error_ratio_threshold:
            * include_estimator_samples_histogram_in_details:
            * truncate_values:
                - lower_bound:
                - upper_bound:
            * round_decimals:
        - text_columns_rule (dict):
            * mostly:
            * strict_min:
            * strict_max:
            * false_positive_rate:
            * estimator:
            * bootstrap:
            * n_resamples:
            * random_seed:
            * quantile_statistic_interpolation_method:
            * quantile_bias_correction:
            * quantile_bias_std_error_ratio_thresohld:
            * include_estimator_samples_histogram_in_details:
            * truncate_values:
                - lower_bound:
                - upper_bound:
            * round_decimals:
            * success_ratio:
        - categorical_columns_rule (dict):
            * cardinality_limit_mode:
            * mostly:
            * strict_min:
            * strict_max:
            * false_positive_rate:
            * estimator:
            * n_resamples:
            * random_seed:
            * quantile_statistic_interpolation_method:
            * quantile_bias_correction:
            * quantile_bias_std_error_ratio_threshold:
            * include_estimator_samples_histogram_in_details:
            * truncate_values:
                - lower_bound:
                - upper_bound:
            * round_decimals:

    Returns:
        OnboardingDataAssistantResult


    #### Table Rule
    This Rule will calculate metrics and if applicable, estimate Expectation parameters and produce Expectations for the table Domain.

    Expectations Produced:
        * expect_table_row_count_to_be_between:
        * expect_table_columns_to_match_set:

    #### Column Uniqueness Rule
    This Rule will calculate metrics for uniqueness and nullity and if applicable, produce Expectations for each column Domain.

    Expectations Produced:
        * expect_column_values_to_be_unique
        * expect_column_values_to_be_null
        * expect_column_values_to_not_be_null

    #### Numeric Column Rule
    The NumericColumnRule will calculate the min_value and max_value for the following expectations.

    * expect_column_min_to_be_between
    * expect_column_max_to_be_between
    * expect_column_values_to_be_between
    * expect_column_median_to_be_between
    * expect_column_mean_to_be_between
    * expect_column_stdev_to_be_between

    By default the estimation will be done by the exact estimator by default, which takes in the values across all Batches in the batch list (in our example 12 months of data from 2019).

    If you do data_assistant.run() with the estimator parameter set to flag_outliers then bootstrapping will be done done behind-the-scenes to estimate outliers.

    The parameters for bootstrapping are:

    * n_resamples : It is set by default to 9999 which is the default in https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.bootstrap.htm

    * false_positive_rate: A user-configured fraction between 0 and 1 expressing desired false positive rate for
        identifying unexpected values as judged by the upper- and lower- quantiles of the observed metric data. Set by default to be 0.05.
    * random_seed: Seed for randomization. If omitted (which is the default), then we use np.random.choice. otherwise, we use np.random.Generator(np.random.PCG64(random_seed)).
    * round_decimals: A user-configured non-negative integer indicating the number of decimals of
        rounding precision of the computed parameter values (i.e., min_value, max_value) prior to packaging them
        on output. For NumericColumnRule the default is 15 which means calculations are done 15 digits after the decimal point.
    * mostly: is 1.0 by default
    * strict_min and strict_max are False, and these are parameters that determine whether the minimum proportion of unique values must be strictly smaller than max or min value.
    * truncate_values:  User-configured directive for whether or not to allow the computed parameter values (i.e.,lower_bound, upper_bound) to take on values outside the specified bounds when packaged on output
    * allow_relative_error:  Whether to allow relative error in quantile communications on backends that support or require it.

    In addition, the expect_column_quantile_values_to_be_between Expectation takes in the following parameters:

    * quantiles : Quantiles and associated value ranges for the column, with the default being[0.25, 0.5, 0.75]
    * quantile_statistic_interpolation_method: which is used when estimating quantile values. Recognized values include auto, nearest, and linear. (default is nearest).
    * quantile_bias_correction: Used when determining whether to correct for quantile bias. Recognized values are True and  False with default being False.
    * quantile_bias_std_error_ratio_threshold: If omitted
        (default), then 0.25 is used (as minimum ratio of bias to standard error for applying bias correction).
    * include_estimator_samples_histogram_in_details: Determines whether the estimator samples are included in the results. (default is False).

    #### DateColumnRules
    The DateColumnRule will take a datetime column and calculate the min_value and max_value for the following Expectations.

    * expect_column_min_to_be_between
    * expect_column_max_to_be_between
    * expect_column_values_to_be_between

    Estimation will be done using the exact estimator by default, but if you select flag_outliers then bootstrapping will use the following parameters:

    * n_resamples : For bootstrapping. It is set by default to 9999 which is the default in https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.bootstrap.htm
    * false_positive_rate: user-configured fraction between 0 and 1 expressing desired false positive rate for
        identifying unexpected values as judged by the upper- and lower- quantiles of the observed metric data. Default is 0.05.
    * random_seed: Seed for randomization. If omitted (which is the default), then we use np.random.choice. otherwise, we use np.random.Generator(np.random.PCG64(random_seed)).
    * round_decimals A user-configured non-negative integer indicating the number of decimals of the
        rounding precision of the computed parameter values (i.e., min_value, max_value) prior to packaging them
        on output. Default for DateColumnRules is  1 which means calculations are done 1 digit after the decimal point

    #### TextColumnsRule

    The TextColumnRule will generate parameters for the following 2 Expectations.

    * expect_column_value_lengths_to_be_between
    * expect_column_values_to_match_regex

    For expect_column_value_lengths_to_be_between will have the parameters min_value and max_value estimated.

    Estimation will be done using the exact estimator by default, but if you select flag_outliers then the following parameters are set by default for bootstrap estimation

    * n_resamples : For bootstrapping. It is set by default to 9999 which is the default in https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.bootstrap.htm
    * false_positive_rate: user-configured fraction between 0 and 1 expressing desired false positive rate for
        identifying unexpected values as judged by the upper- and lower- quantiles of the observed metric data. Default is 0.05.
    * random_seed: Seed for randomization. If omitted (which is the default), then we use np.random.choice. otherwise, we use np.random.Generator(np.random.PCG64(random_seed)).
    * round_decimals A user-configured non-negative integer indicating the number of decimals of the
        rounding precision of the computed parameter values (i.e., min_value, max_value) prior to packaging them
        on output. Default for TextColumnRule is  0 which means calculations are done to the nearest integer.

    For expect_column_values_to_match_regex, the values in the column be matched against a candidate list of common regex values which were built from the following sources:

       - [20 Most Common Regular Expressions](https://regexland.com/most-common-regular-expressions/)
       - [Stackoverflow on how to test for valid uuid](https://stackoverflow.com/questions/7905929/how-to-test-valid-uuid-guid/13653180#13653180)

    * This is the regex list used by the TextColumnsRule.

    CANDIDATE_REGEX: Set[str] = {
        r"\\d+",  # whole number with 1 or more digits
        r"-?\\d+",  # negative whole numbers
        r"-?\\d+(?:\\.\\d*)?",  # decimal numbers with . (period) separator
        r"[A-Za-z0-9\\.,;:!?()\"'%\\-]+",  # general text
        r"^\\s+",  # leading space
        r"\\s+$",  # trailing space
        r"https?:\\/\\/(?:www\\.)?[-a-zA-Z0-9@:%._\\+~#=]{2,256}\\.[a-z]{2,6}\b(?:[-a-zA-Z0-9@:%_\\+.~#()?&//=]*)",  # Matching URL (including http(s) protocol)
        r"<\\/?(?:p|a|b|img)(?: \\/)?>",  # HTML tags
        r"(?:25[0-5]|2[0-4]\\d|[01]\\d{2}|\\d{1,2})(?:.(?:25[0-5]|2[0-4]\\d|[01]\\d{2}|\\d{1,2})){3}",  # IPv4 IP address
        r"(?:[A-Fa-f0-9]){0,4}(?: ?:? ?(?:[A-Fa-f0-9]){0,4}){0,7}",  # IPv6 IP address,
        r"\b[0-9a-fA-F]{8}\b-[0-9a-fA-F]{4}-[0-5][0-9a-fA-F]{3}-[089ab][0-9a-fA-F]{3}-\b[0-9a-fA-F]{12}\b ",  # UUID
    }

    **Note**: The above list can be found in the [great_expectations repo here](https://github.com/great-expectations/great_expectations/blob/da2376b613843844afc041538269bb7683444b1f/great_expectations/rule_based_profiler/parameter_builder/regex_pattern_string_parameter_builder.py#L38)

    #### CategoricalColumnsRule

    The CategoricalColumnsRule will generate parameters for the following 3 Expectations:

    * expect_column_values_to_be_in_set
    * expect_column_unique_value_count_to_be_between
    * expect_column_proportion_of_unique_values_to_be_between


    Categorical columns are determined to be ones that meet a certain cardinality threshold. This prevents us from calculating the number of unique value in a column with millions of rows, with each value being slightly different from another (which is theoretically possible).

    Great Expectations gets around this by only building the unique value set for columns that have less than a certain number of unique values, which is determined by the cardinality threshold. The default threshold is FEW which means Great Expectations will generate parameters for expect_column_values_to_be_in_set() for columns where the number of unique values are less than or equal to 100.

    Other values for cardinality values include:
        ZERO = AbsoluteCardinalityLimit("ZERO", 0)
        ONE = AbsoluteCardinalityLimit("ONE", 1)
        TWO = AbsoluteCardinalityLimit("TWO", 2)
        VERY_FEW = AbsoluteCardinalityLimit("VERY_FEW", 10)
        FEW = AbsoluteCardinalityLimit("FEW", 100)
        SOME = AbsoluteCardinalityLimit("SOME", 1000)
        MANY = AbsoluteCardinalityLimit("MANY", 10000)
        VERY_MANY = AbsoluteCardinalityLimit("VERY_MANY", 100000)
        UNIQUE = RelativeCardinalityLimit("UNIQUE", 1.0)

    The full list of cardinality values used by Great Expectations can be found in the [great_expectations repo here](https://github.com/great-expectations/great_expectations/blob/da2376b613843844afc041538269bb7683444b1f/great_expectations/rule_based_profiler/helpers/cardinality_checker.py#L55).

    The three Expectations each have their own parameters

    expect_column_values_to_be_in_set requires column (column name) and value_set, which is calculated for all columns that meet the cardinality threshold.

    expect_column_unique_value_count_to_be_between has column (column name) and min_value and max_value.

    Estimation will be done using the exact estimator by default, but if you select flag_outliers then the following parameters are set by default for bootstrap estimation

    * n_resamples : For bootstrapping. It is set by default to 9999 which is the default in https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.bootstrap.htm
    * false_positive_rate: user-configured fraction between 0 and 1 expressing desired false positive rate for
        identifying unexpected values as judged by the upper- and lower- quantiles of the observed metric data. Default is 0.05.
    * random_seed: Seed for randomization. If omitted (which is the default), then we use np.random.choice. otherwise, we use np.random.Generator(np.random.PCG64(random_seed)).

    expect_column_proportion_of_unique_values_to_be_between has the following parameters

    * column: column name
    * min_value:  minimum proportion of unique values, ranging from 0 to 1
    * max_value:  minimum proportion of unique values, ranging from 0 to 1
    * strict_min: determine whether the minimum proportion of unique values must be strictly greater than min value, with the default=False
    * strict_max: determine whether the minimum proportion of unique values must be strictly smaller than max value, with the default=False

    Estimation will be done using the exact estimator by default, but if you select flag_outliers then the following parameters are set by default for bootstrap estimation

    * n_resamples : For bootstrapping. It is set by default to 9999 which is the default in https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.bootstrap.htm
    * false_positive_rate: user-configured fraction between 0 and 1 expressing desired false positive rate for
        identifying unexpected values as judged by the upper- and lower- quantiles of the observed metric data. Default is 0.05.
    * random_seed: Seed for randomization. If omitted (which is the default), then then we use np.random.choice. otherwise, we use np.random.Generator(np.random.PCG64(random_seed)).
    * round_decimals: A user-configured non-negative integer indicating the number of decimals of the
        rounding precision of the computed parameter values (i.e., min_value, max_value) prior to packaging them
        on output. For CategoricalColumnsRule the default is 15 which means calculations are done 15 digits after the decimal point.
    * mostly: is 1.0 by default
    * strict_min and strict_max are False, and these are parameters that determine whether the minimum proportion of unique values must be strictly smaller than max or min value.
    * truncate_values:  User-configured directive for whether or not to allow the computed parameter values (i.e.,lower_bound, upper_bound) to take on values outside the specified bounds when packaged on output
    * allow_relative_error:  Whether to allow relative error in quantile communications on backends that support or require it.
    """

    __alias__: str = "onboarding"

    def __init__(
        self,
        name: str,
        validator: Validator,
    ) -> None:
        super().__init__(
            name=name,
            validator=validator,
        )

    def get_variables(self) -> Optional[Dict[str, Any]]:
        """
        Returns:
            Optional "variables" configuration attribute name/value pairs (overrides), commonly-used in Builder objects.
        """
        return None

    def get_rules(self) -> Optional[List[Rule]]:
        """
        Returns:
            Optional custom list of "Rule" objects implementing particular "DataAssistant" functionality.
        """
        table_rule: Rule = self._build_table_rule()

        total_count_metric_multi_batch_parameter_builder_for_evaluations: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_table_row_count_metric_multi_batch_parameter_builder()
        )

        column_value_uniqueness_rule: Rule = build_map_metric_rule(
            data_assistant_class_name=self.__class__.__name__,
            rule_name="column_value_uniqueness_rule",
            expectation_type="expect_column_values_to_be_unique",
            map_metric_name="column_values.unique",
            total_count_metric_multi_batch_parameter_builder_for_evaluations=total_count_metric_multi_batch_parameter_builder_for_evaluations,
            include_column_names=None,
            exclude_column_names=None,
            include_column_name_suffixes=None,
            exclude_column_name_suffixes=None,
            semantic_type_filter_module_name=None,
            semantic_type_filter_class_name=None,
            include_semantic_types=None,
            exclude_semantic_types=None,
            max_unexpected_values=0,
            max_unexpected_ratio=None,
            min_max_unexpected_values_proportion=9.75e-1,
        )
        column_value_nullity_rule: Rule = build_map_metric_rule(
            data_assistant_class_name=self.__class__.__name__,
            rule_name="column_value_nullity_rule",
            expectation_type="expect_column_values_to_be_null",
            map_metric_name="column_values.null",
            total_count_metric_multi_batch_parameter_builder_for_evaluations=total_count_metric_multi_batch_parameter_builder_for_evaluations,
            include_column_names=None,
            exclude_column_names=None,
            include_column_name_suffixes=None,
            exclude_column_name_suffixes=None,
            semantic_type_filter_module_name=None,
            semantic_type_filter_class_name=None,
            include_semantic_types=None,
            exclude_semantic_types=None,
            max_unexpected_values=0,
            max_unexpected_ratio=None,
            min_max_unexpected_values_proportion=9.75e-1,
        )
        column_value_nonnullity_rule: Rule = build_map_metric_rule(
            data_assistant_class_name=self.__class__.__name__,
            rule_name="column_value_nonnullity_rule",
            expectation_type="expect_column_values_to_not_be_null",
            map_metric_name="column_values.nonnull",
            total_count_metric_multi_batch_parameter_builder_for_evaluations=total_count_metric_multi_batch_parameter_builder_for_evaluations,
            include_column_names=None,
            exclude_column_names=None,
            include_column_name_suffixes=None,
            exclude_column_name_suffixes=None,
            semantic_type_filter_module_name=None,
            semantic_type_filter_class_name=None,
            include_semantic_types=None,
            exclude_semantic_types=None,
            max_unexpected_values=0,
            max_unexpected_ratio=None,
            min_max_unexpected_values_proportion=9.75e-1,
        )
        numeric_columns_rule: Rule = self._build_numeric_columns_rule()
        datetime_columns_rule: Rule = self._build_datetime_columns_rule()
        text_columns_rule: Rule = self._build_text_columns_rule()
        categorical_columns_rule: Rule = self._build_categorical_columns_rule()

        return [
            table_rule,
            column_value_uniqueness_rule,
            column_value_nullity_rule,
            column_value_nonnullity_rule,
            numeric_columns_rule,
            datetime_columns_rule,
            text_columns_rule,
            categorical_columns_rule,
        ]

    def _build_data_assistant_result(
        self, data_assistant_result: DataAssistantResult
    ) -> DataAssistantResult:
        return OnboardingDataAssistantResult(
            _batch_id_to_batch_identifier_display_name_map=data_assistant_result._batch_id_to_batch_identifier_display_name_map,
            profiler_config=data_assistant_result.profiler_config,
            profiler_execution_time=data_assistant_result.profiler_execution_time,
            rule_domain_builder_execution_time=data_assistant_result.rule_domain_builder_execution_time,
            rule_execution_time=data_assistant_result.rule_execution_time,
            metrics_by_domain=data_assistant_result.metrics_by_domain,
            expectation_configurations=data_assistant_result.expectation_configurations,
            citation=data_assistant_result.citation,
            _usage_statistics_handler=data_assistant_result._usage_statistics_handler,
        )

    @staticmethod
    def _build_table_rule() -> Rule:
        """
        This method builds "Rule" object focused on emitting "ExpectationConfiguration" objects for table "Domain" type.
        """
        # Step-1: Instantiate "TableDomainBuilder" object.

        table_domain_builder = TableDomainBuilder(
            data_context=None,
        )

        # Step-2: Declare "ParameterBuilder" for every metric of interest.

        table_row_count_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_table_row_count_metric_multi_batch_parameter_builder()
        )
        table_columns_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_table_columns_metric_multi_batch_parameter_builder()
        )

        # Step-3: Declare "ParameterBuilder" for every "validation" need in "ExpectationConfigurationBuilder" objects.

        evaluation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]] = [
            ParameterBuilderConfig(
                **table_row_count_metric_multi_batch_parameter_builder_for_metrics.to_json_dict()
            ),
        ]
        table_row_count_range_parameter_builder_for_validations: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.build_numeric_metric_range_multi_batch_parameter_builder(
            metric_name=None,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
        )

        mean_table_columns_set_match_multi_batch_parameter_builder_for_validations = (
            MeanTableColumnsSetMatchMultiBatchParameterBuilder(
                name="column_names_set_estimator",
                metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
                metric_value_kwargs=None,
                evaluation_parameter_builder_configs=None,
            )
        )

        # Step-4: Pass "validation" "ParameterBuilderConfig" objects to every "DefaultExpectationConfigurationBuilder", responsible for emitting "ExpectationConfiguration" (with specified "expectation_type").

        validation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]]

        validation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **table_row_count_range_parameter_builder_for_validations.to_json_dict(),
            ),
        ]
        expect_table_row_count_to_be_between_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_table_row_count_to_be_between",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            min_value=f"{table_row_count_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[0]",
            max_value=f"{table_row_count_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[1]",
            meta={
                "profiler_details": f"{table_row_count_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
            },
        )

        validation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **mean_table_columns_set_match_multi_batch_parameter_builder_for_validations.to_json_dict(),
            ),
        ]
        expect_table_columns_to_match_set_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_table_columns_to_match_set",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            condition=f"{mean_table_columns_set_match_multi_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}success_ratio >= {VARIABLES_KEY}success_ratio",
            column_set=f"{mean_table_columns_set_match_multi_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}",
            exact_match=f"{VARIABLES_KEY}exact_match",
            meta={
                "profiler_details": f"{mean_table_columns_set_match_multi_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
            },
        )

        # Step-5: Instantiate and return "Rule" object, comprised of "variables", "domain_builder", "parameter_builders", and "expectation_configuration_builders" components.

        variables: dict = {
            "false_positive_rate": 0.05,
            "estimator": "bootstrap",
            "n_resamples": 9999,
            "random_seed": None,
            "quantile_statistic_interpolation_method": "nearest",
            "quantile_bias_correction": False,
            "quantile_bias_std_error_ratio_threshold": None,
            "include_estimator_samples_histogram_in_details": False,
            "truncate_values": {
                "lower_bound": 0,
                "upper_bound": None,
            },
            "round_decimals": 0,
            "exact_match": None,
            "success_ratio": 1.0,
        }
        parameter_builders: List[ParameterBuilder] = [
            table_row_count_metric_multi_batch_parameter_builder_for_metrics,
            table_columns_metric_multi_batch_parameter_builder_for_metrics,
        ]
        expectation_configuration_builders: List[ExpectationConfigurationBuilder] = [
            expect_table_row_count_to_be_between_expectation_configuration_builder,
            expect_table_columns_to_match_set_expectation_configuration_builder,
        ]
        rule = Rule(
            name="table_rule",
            variables=variables,
            domain_builder=table_domain_builder,
            parameter_builders=parameter_builders,
            expectation_configuration_builders=expectation_configuration_builders,
        )

        return rule

    @staticmethod
    def _build_numeric_columns_rule() -> Rule:
        """
        This method builds "Rule" object focused on emitting "ExpectationConfiguration" objects for numeric columns.
        """

        # Step-1: Instantiate "ColumnDomainBuilder" for selecting numeric columns (but not "ID-type" columns).

        numeric_column_type_domain_builder = ColumnDomainBuilder(
            include_column_names=None,
            exclude_column_names=None,
            include_column_name_suffixes=None,
            exclude_column_name_suffixes=[
                "_id",
                "_ID",
            ],
            semantic_type_filter_module_name=None,
            semantic_type_filter_class_name=None,
            include_semantic_types=[
                SemanticDomainTypes.NUMERIC,
            ],
            exclude_semantic_types=[
                SemanticDomainTypes.IDENTIFIER,
            ],
            data_context=None,
        )

        # Step-2: Declare "ParameterBuilder" for every metric of interest.

        column_histogram_single_batch_parameter_builder_for_metrics: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.build_histogram_single_batch_parameter_builder(
            name="column_values.partition",
        )
        column_min_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_column_min_metric_multi_batch_parameter_builder()
        )
        column_max_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_column_max_metric_multi_batch_parameter_builder()
        )
        column_quantile_values_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_column_quantile_values_metric_multi_batch_parameter_builder()
        )
        column_median_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_column_median_metric_multi_batch_parameter_builder()
        )
        column_mean_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_column_mean_metric_multi_batch_parameter_builder()
        )
        column_standard_deviation_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_column_standard_deviation_metric_multi_batch_parameter_builder()
        )

        # Step-3: Declare "ParameterBuilder" for every "validation" need in "ExpectationConfigurationBuilder" objects.

        evaluation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]]

        evaluation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_min_metric_multi_batch_parameter_builder_for_metrics.to_json_dict()
            ),
        ]
        column_min_values_range_parameter_builder_for_validations: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.build_numeric_metric_range_multi_batch_parameter_builder(
            metric_name=None,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
        )

        evaluation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_max_metric_multi_batch_parameter_builder_for_metrics.to_json_dict()
            ),
        ]
        column_max_values_range_parameter_builder_for_validations: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.build_numeric_metric_range_multi_batch_parameter_builder(
            metric_name=None,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
        )

        evaluation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_quantile_values_metric_multi_batch_parameter_builder_for_metrics.to_json_dict()
            ),
        ]
        column_quantile_values_range_parameter_builder_for_validations: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.build_numeric_metric_range_multi_batch_parameter_builder(
            metric_name=None,
            metric_value_kwargs={
                "quantiles": f"{VARIABLES_KEY}quantiles",
                "allow_relative_error": f"{VARIABLES_KEY}allow_relative_error",
            },
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
        )

        evaluation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_median_metric_multi_batch_parameter_builder_for_metrics.to_json_dict()
            ),
        ]
        column_median_values_range_parameter_builder_for_validations: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.build_numeric_metric_range_multi_batch_parameter_builder(
            metric_name=None,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
        )

        evaluation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_mean_metric_multi_batch_parameter_builder_for_metrics.to_json_dict()
            ),
        ]
        column_mean_values_range_parameter_builder_for_validations: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.build_numeric_metric_range_multi_batch_parameter_builder(
            metric_name=None,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
        )

        evaluation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_standard_deviation_metric_multi_batch_parameter_builder_for_metrics.to_json_dict()
            ),
        ]
        column_standard_deviation_values_range_parameter_builder_for_validations: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.build_numeric_metric_range_multi_batch_parameter_builder(
            metric_name=None,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
        )

        # Step-4: Pass "validation" "ParameterBuilderConfig" objects to every "DefaultExpectationConfigurationBuilder", responsible for emitting "ExpectationConfiguration" (with specified "expectation_type").

        validation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]]

        validation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_min_values_range_parameter_builder_for_validations.to_json_dict(),
            ),
        ]
        expect_column_min_to_be_between_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_min_to_be_between",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            min_value=f"{column_min_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[0]",
            max_value=f"{column_min_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[1]",
            strict_min=f"{VARIABLES_KEY}strict_min",
            strict_max=f"{VARIABLES_KEY}strict_max",
            meta={
                "profiler_details": f"{column_min_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
            },
        )

        validation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_max_values_range_parameter_builder_for_validations.to_json_dict(),
            ),
        ]
        expect_column_max_to_be_between_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_max_to_be_between",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            min_value=f"{column_max_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[0]",
            max_value=f"{column_max_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[1]",
            strict_min=f"{VARIABLES_KEY}strict_min",
            strict_max=f"{VARIABLES_KEY}strict_max",
            meta={
                "profiler_details": f"{column_max_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
            },
        )

        validation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_min_values_range_parameter_builder_for_validations.to_json_dict(),
            ),
            ParameterBuilderConfig(
                **column_max_values_range_parameter_builder_for_validations.to_json_dict(),
            ),
        ]
        expect_column_values_to_be_between_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_values_to_be_between",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            min_value=f"{column_min_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[0]",
            max_value=f"{column_max_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[1]",
            mostly=f"{VARIABLES_KEY}mostly",
            strict_min=f"{VARIABLES_KEY}strict_min",
            strict_max=f"{VARIABLES_KEY}strict_max",
            meta={
                "profiler_details": {
                    "column_min_values_range_estimator": f"{column_min_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
                    "column_max_values_range_estimator": f"{column_max_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
                },
            },
        )

        validation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_quantile_values_range_parameter_builder_for_validations.to_json_dict(),
            ),
        ]
        expect_column_quantile_values_to_be_between_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_quantile_values_to_be_between",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            quantile_ranges={
                "quantiles": f"{VARIABLES_KEY}quantiles",
                "value_ranges": f"{column_quantile_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}",
            },
            allow_relative_error=f"{VARIABLES_KEY}allow_relative_error",
            meta={
                "profiler_details": f"{column_quantile_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
            },
        )

        validation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_median_values_range_parameter_builder_for_validations.to_json_dict(),
            ),
        ]
        expect_column_median_to_be_between_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_median_to_be_between",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            min_value=f"{column_median_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[0]",
            max_value=f"{column_median_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[1]",
            strict_min=f"{VARIABLES_KEY}strict_min",
            strict_max=f"{VARIABLES_KEY}strict_max",
            meta={
                "profiler_details": f"{column_median_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
            },
        )

        validation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_mean_values_range_parameter_builder_for_validations.to_json_dict(),
            ),
        ]
        expect_column_mean_to_be_between_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_mean_to_be_between",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            min_value=f"{column_mean_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[0]",
            max_value=f"{column_mean_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[1]",
            strict_min=f"{VARIABLES_KEY}strict_min",
            strict_max=f"{VARIABLES_KEY}strict_max",
            meta={
                "profiler_details": f"{column_mean_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
            },
        )

        validation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_standard_deviation_values_range_parameter_builder_for_validations.to_json_dict(),
            ),
        ]
        expect_column_stdev_to_be_between_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_stdev_to_be_between",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            min_value=f"{column_standard_deviation_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[0]",
            max_value=f"{column_standard_deviation_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[1]",
            strict_min=f"{VARIABLES_KEY}strict_min",
            strict_max=f"{VARIABLES_KEY}strict_max",
            meta={
                "profiler_details": f"{column_standard_deviation_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
            },
        )

        # Step-5: Instantiate and return "Rule" object, comprised of "variables", "domain_builder", "parameter_builders", and "expectation_configuration_builders" components.

        variables: dict = {
            "mostly": 1.0,
            "strict_min": False,
            "strict_max": False,
            "quantiles": [
                0.25,
                0.5,
                0.75,
            ],
            "allow_relative_error": False,
            "false_positive_rate": 0.05,
            "estimator": "bootstrap",
            "n_resamples": 9999,
            "random_seed": None,
            "quantile_statistic_interpolation_method": "nearest",
            "quantile_bias_correction": False,
            "quantile_bias_std_error_ratio_threshold": None,
            "include_estimator_samples_histogram_in_details": False,
            "truncate_values": {
                "lower_bound": None,
                "upper_bound": None,
            },
            "round_decimals": None,
        }
        parameter_builders: List[ParameterBuilder] = [
            column_histogram_single_batch_parameter_builder_for_metrics,
            column_min_metric_multi_batch_parameter_builder_for_metrics,
            column_max_metric_multi_batch_parameter_builder_for_metrics,
            column_quantile_values_metric_multi_batch_parameter_builder_for_metrics,
            column_median_metric_multi_batch_parameter_builder_for_metrics,
            column_mean_metric_multi_batch_parameter_builder_for_metrics,
            column_standard_deviation_metric_multi_batch_parameter_builder_for_metrics,
        ]
        expectation_configuration_builders: List[ExpectationConfigurationBuilder] = [
            expect_column_min_to_be_between_expectation_configuration_builder,
            expect_column_max_to_be_between_expectation_configuration_builder,
            expect_column_values_to_be_between_expectation_configuration_builder,
            expect_column_quantile_values_to_be_between_expectation_configuration_builder,
            expect_column_median_to_be_between_expectation_configuration_builder,
            expect_column_mean_to_be_between_expectation_configuration_builder,
            expect_column_stdev_to_be_between_expectation_configuration_builder,
        ]
        rule = Rule(
            name="numeric_columns_rule",
            variables=variables,
            domain_builder=numeric_column_type_domain_builder,
            parameter_builders=parameter_builders,
            expectation_configuration_builders=expectation_configuration_builders,
        )

        return rule

    @staticmethod
    def _build_datetime_columns_rule() -> Rule:
        """
        This method builds "Rule" object focused on emitting "ExpectationConfiguration" objects for datetime columns.
        """

        # Step-1: Instantiate "ColumnDomainBuilder" for selecting proper datetime columns (not "datetime-looking" text).

        datetime_column_type_domain_builder = ColumnDomainBuilder(
            include_column_names=None,
            exclude_column_names=None,
            include_column_name_suffixes=None,
            exclude_column_name_suffixes=None,
            semantic_type_filter_module_name=None,
            semantic_type_filter_class_name=None,
            include_semantic_types=[
                SemanticDomainTypes.DATETIME,
            ],
            exclude_semantic_types=[
                SemanticDomainTypes.TEXT,
            ],
            data_context=None,
        )

        # Step-2: Declare "ParameterBuilder" for every metric of interest.

        column_min_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_column_min_metric_multi_batch_parameter_builder()
        )
        column_max_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_column_max_metric_multi_batch_parameter_builder()
        )

        # Step-3: Declare "ParameterBuilder" for every "validation" need in "ExpectationConfigurationBuilder" objects.

        evaluation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]]

        evaluation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_min_metric_multi_batch_parameter_builder_for_metrics.to_json_dict()
            ),
        ]
        column_min_values_range_parameter_builder_for_validations: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.build_numeric_metric_range_multi_batch_parameter_builder(
            metric_name=None,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
        )

        evaluation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_max_metric_multi_batch_parameter_builder_for_metrics.to_json_dict()
            ),
        ]
        column_max_values_range_parameter_builder_for_validations: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.build_numeric_metric_range_multi_batch_parameter_builder(
            metric_name=None,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
        )

        # Step-4: Pass "validation" "ParameterBuilderConfig" objects to every "DefaultExpectationConfigurationBuilder", responsible for emitting "ExpectationConfiguration" (with specified "expectation_type").

        validation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]]

        validation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_min_values_range_parameter_builder_for_validations.to_json_dict(),
            ),
        ]
        expect_column_min_to_be_between_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_min_to_be_between",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            min_value=f"{column_min_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[0]",
            max_value=f"{column_min_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[1]",
            strict_min=f"{VARIABLES_KEY}strict_min",
            strict_max=f"{VARIABLES_KEY}strict_max",
            meta={
                "profiler_details": f"{column_min_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
            },
        )

        validation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_max_values_range_parameter_builder_for_validations.to_json_dict(),
            ),
        ]
        expect_column_max_to_be_between_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_max_to_be_between",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            min_value=f"{column_max_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[0]",
            max_value=f"{column_max_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[1]",
            strict_min=f"{VARIABLES_KEY}strict_min",
            strict_max=f"{VARIABLES_KEY}strict_max",
            meta={
                "profiler_details": f"{column_max_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
            },
        )

        validation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_min_values_range_parameter_builder_for_validations.to_json_dict(),
            ),
            ParameterBuilderConfig(
                **column_max_values_range_parameter_builder_for_validations.to_json_dict(),
            ),
        ]
        expect_column_values_to_be_between_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_values_to_be_between",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            min_value=f"{column_min_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[0]",
            max_value=f"{column_max_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[1]",
            strict_min=f"{VARIABLES_KEY}strict_min",
            strict_max=f"{VARIABLES_KEY}strict_max",
            mostly=f"{VARIABLES_KEY}mostly",
            meta={
                "profiler_details": {
                    "column_min_values_range_estimator": f"{column_min_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
                    "column_max_values_range_estimator": f"{column_max_values_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
                },
            },
        )

        # Step-5: Instantiate and return "Rule" object, comprised of "variables", "domain_builder", "parameter_builders", and "expectation_configuration_builders" components.

        variables: dict = {
            "mostly": 1.0,
            "strict_min": False,
            "strict_max": False,
            "false_positive_rate": 0.05,
            "estimator": "bootstrap",
            "n_resamples": 9999,
            "random_seed": None,
            "quantile_statistic_interpolation_method": "nearest",
            "quantile_bias_correction": False,
            "quantile_bias_std_error_ratio_threshold": None,
            "include_estimator_samples_histogram_in_details": False,
            "truncate_values": {
                "lower_bound": None,
                "upper_bound": None,
            },
            "round_decimals": None,
        }
        parameter_builders: List[ParameterBuilder] = [
            column_min_metric_multi_batch_parameter_builder_for_metrics,
            column_max_metric_multi_batch_parameter_builder_for_metrics,
        ]
        expectation_configuration_builders: List[ExpectationConfigurationBuilder] = [
            expect_column_min_to_be_between_expectation_configuration_builder,
            expect_column_max_to_be_between_expectation_configuration_builder,
            expect_column_values_to_be_between_expectation_configuration_builder,
        ]
        rule = Rule(
            name="datetime_columns_rule",
            variables=variables,
            domain_builder=datetime_column_type_domain_builder,
            parameter_builders=parameter_builders,
            expectation_configuration_builders=expectation_configuration_builders,
        )

        return rule

    @staticmethod
    def _build_text_columns_rule() -> Rule:

        # Step-1: Instantiate "ColumnDomainBuilder" for selecting proper text columns.

        text_column_type_domain_builder = ColumnDomainBuilder(
            include_column_names=None,
            exclude_column_names=None,
            include_column_name_suffixes=None,
            exclude_column_name_suffixes=None,
            semantic_type_filter_module_name=None,
            semantic_type_filter_class_name=None,
            include_semantic_types=[
                SemanticDomainTypes.TEXT,
            ],
            exclude_semantic_types=[
                SemanticDomainTypes.NUMERIC,
                SemanticDomainTypes.DATETIME,
            ],
            data_context=None,
        )

        # Step-2: Declare "ParameterBuilder" for every metric of interest.

        column_min_length_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_column_min_length_metric_multi_batch_parameter_builder()
        )
        column_max_length_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_column_max_length_metric_multi_batch_parameter_builder()
        )

        # Step-3: Declare "ParameterBuilder" for every "validation" need in "ExpectationConfigurationBuilder" objects.

        evaluation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]]

        evaluation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_min_length_metric_multi_batch_parameter_builder_for_metrics.to_json_dict()
            ),
        ]
        column_min_length_range_parameter_builder_for_validations: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.build_numeric_metric_range_multi_batch_parameter_builder(
            metric_name=None,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
        )

        evaluation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_max_length_metric_multi_batch_parameter_builder_for_metrics.to_json_dict()
            ),
        ]
        column_max_length_range_parameter_builder_for_validations: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.build_numeric_metric_range_multi_batch_parameter_builder(
            metric_name=None,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
        )

        column_values_to_match_regex_parameter_builder_for_validations: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.build_regex_pattern_string_parameter_builder(
            name="column_values.match_regex",
        )

        # Step-4: Pass "validation" "ParameterBuilderConfig" objects to every "DefaultExpectationConfigurationBuilder", responsible for emitting "ExpectationConfiguration" (with specified "expectation_type").

        validation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]]

        validation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_min_length_range_parameter_builder_for_validations.to_json_dict(),
            ),
            ParameterBuilderConfig(
                **column_max_length_range_parameter_builder_for_validations.to_json_dict(),
            ),
        ]
        expect_column_value_lengths_to_be_between_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_value_lengths_to_be_between",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            min_value=f"{column_min_length_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[0]",
            max_value=f"{column_max_length_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[1]",
            mostly=f"{VARIABLES_KEY}mostly",
            strict_min=f"{VARIABLES_KEY}strict_min",
            strict_max=f"{VARIABLES_KEY}strict_max",
            meta={
                "profiler_details": {
                    "column_min_length_range_estimator": f"{column_min_length_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
                    "column_max_length_range_estimator": f"{column_max_length_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
                },
            },
        )

        validation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_values_to_match_regex_parameter_builder_for_validations.to_json_dict(),
            ),
        ]
        expect_column_values_to_match_regex_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_values_to_match_regex",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            condition=f"{column_values_to_match_regex_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}success_ratio >= {VARIABLES_KEY}success_ratio",
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            regex=f"{column_values_to_match_regex_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}",
            mostly=f"{VARIABLES_KEY}mostly",
            meta={
                "profiler_details": f"{column_values_to_match_regex_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
            },
        )

        # Step-5: Instantiate and return "Rule" object, comprised of "variables", "domain_builder", "parameter_builders", and "expectation_configuration_builders" components.

        variables: dict = {
            "mostly": 1.0,
            "strict_min": False,
            "strict_max": False,
            "false_positive_rate": 0.05,
            "estimator": "bootstrap",
            "n_resamples": 9999,
            "random_seed": None,
            "quantile_statistic_interpolation_method": "nearest",
            "quantile_bias_correction": False,
            "quantile_bias_std_error_ratio_threshold": None,
            "include_estimator_samples_histogram_in_details": False,
            "truncate_values": {
                "lower_bound": 0,
                "upper_bound": None,
            },
            "round_decimals": 0,
            "success_ratio": 7.5e-1,
        }
        parameter_builders: List[ParameterBuilder] = [
            column_min_length_metric_multi_batch_parameter_builder_for_metrics,
            column_max_length_metric_multi_batch_parameter_builder_for_metrics,
        ]
        expectation_configuration_builders: List[ExpectationConfigurationBuilder] = [
            expect_column_value_lengths_to_be_between_expectation_configuration_builder,
            expect_column_values_to_match_regex_expectation_configuration_builder,
        ]
        rule = Rule(
            name="text_columns_rule",
            variables=variables,
            domain_builder=text_column_type_domain_builder,
            parameter_builders=parameter_builders,
            expectation_configuration_builders=expectation_configuration_builders,
        )

        return rule

    @staticmethod
    def _build_categorical_columns_rule() -> Rule:
        """
        This method builds "Rule" object focused on emitting "ExpectationConfiguration" objects for categorical columns.
        """

        # Step-1: Instantiate "CategoricalColumnDomainBuilder" for selecting columns containing "FEW" discrete values.

        categorical_column_type_domain_builder: CategoricalColumnDomainBuilder = (
            CategoricalColumnDomainBuilder(
                include_column_names=None,
                exclude_column_names=None,
                include_column_name_suffixes=None,
                exclude_column_name_suffixes=None,
                semantic_type_filter_module_name=None,
                semantic_type_filter_class_name=None,
                include_semantic_types=None,
                exclude_semantic_types=None,
                allowed_semantic_types_passthrough=None,
                cardinality_limit_mode=f"{VARIABLES_KEY}cardinality_limit_mode",
                max_unique_values=None,
                max_proportion_unique=None,
                data_context=None,
            )
        )

        # Step-2: Declare "ParameterBuilder" for every metric of interest.

        column_distinct_values_count_metric_multi_batch_parameter_builder_for_metrics: ParameterBuilder = (
            DataAssistant.commonly_used_parameter_builders.get_column_distinct_values_count_metric_multi_batch_parameter_builder()
        )
        metric_name: str = "column.value_counts"
        name: str = sanitize_parameter_name(name=metric_name)
        column_value_counts_metric_multi_batch_parameter_builder_for_metrics = (
            MetricMultiBatchParameterBuilder(
                name=name,
                metric_name=metric_name,
                metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
                metric_value_kwargs={
                    "sort": "value",
                },
                single_batch_mode=False,
                enforce_numeric_metric=False,
                replace_nan_with_zero=False,
                reduce_scalar_metric=True,
                evaluation_parameter_builder_configs=None,
                data_context=None,
            )
        )

        # Step-3: Declare "ParameterBuilder" for every "validation" need in "ExpectationConfigurationBuilder" objects.

        value_set_multi_batch_parameter_builder_for_validations: ParameterBuilder = (
            ValueSetMultiBatchParameterBuilder(
                name="value_set_estimator",
                metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
                metric_value_kwargs=None,
                evaluation_parameter_builder_configs=None,
                data_context=None,
            )
        )

        evaluation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]]

        evaluation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_distinct_values_count_metric_multi_batch_parameter_builder_for_metrics.to_json_dict()
            ),
        ]
        column_distinct_values_count_range_parameter_builder_for_validations: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.build_numeric_metric_range_multi_batch_parameter_builder(
            metric_name=None,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
        )

        column_unique_proportion_range_parameter_builder_for_validations: ParameterBuilder = DataAssistant.commonly_used_parameter_builders.build_numeric_metric_range_multi_batch_parameter_builder(
            metric_name="column.unique_proportion",
            metric_value_kwargs=None,
        )

        # Step-4: Pass "validation" "ParameterBuilderConfig" objects to every "DefaultExpectationConfigurationBuilder", responsible for emitting "ExpectationConfiguration" (with specified "expectation_type").

        validation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]]

        validation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **value_set_multi_batch_parameter_builder_for_validations.to_json_dict(),
            ),
        ]
        expect_column_values_to_be_in_set_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_values_to_be_in_set",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            value_set=f"{value_set_multi_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}",
            condition=f"{value_set_multi_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}parse_strings_as_datetimes != True",
            mostly=f"{VARIABLES_KEY}mostly",
            meta={
                "profiler_details": f"{value_set_multi_batch_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
            },
        )

        validation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_distinct_values_count_range_parameter_builder_for_validations.to_json_dict(),
            ),
        ]
        expect_column_unique_value_count_to_be_between_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_unique_value_count_to_be_between",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            min_value=f"{column_distinct_values_count_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[0]",
            max_value=f"{column_distinct_values_count_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[1]",
            strict_min=f"{VARIABLES_KEY}strict_min",
            strict_max=f"{VARIABLES_KEY}strict_max",
            meta={
                "profiler_details": f"{column_distinct_values_count_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
            },
        )

        validation_parameter_builder_configs = [
            ParameterBuilderConfig(
                **column_unique_proportion_range_parameter_builder_for_validations.to_json_dict(),
            ),
        ]
        expect_column_proportion_of_unique_values_to_be_between_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_proportion_of_unique_values_to_be_between",
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            column=f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
            min_value=f"{column_unique_proportion_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[0]",
            max_value=f"{column_unique_proportion_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[1]",
            strict_min=f"{VARIABLES_KEY}strict_min",
            strict_max=f"{VARIABLES_KEY}strict_max",
            meta={
                "profiler_details": f"{column_unique_proportion_range_parameter_builder_for_validations.json_serialized_fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
            },
        )

        # Step-5: Instantiate and return "Rule" object, comprised of "variables", "domain_builder", "parameter_builders", and "expectation_configuration_builders" components.

        variables: dict = {
            "cardinality_limit_mode": CardinalityLimitMode.FEW.name,
            "mostly": 1.0,
            "strict_min": False,
            "strict_max": False,
            "false_positive_rate": 0.05,
            "estimator": "bootstrap",
            "n_resamples": 9999,
            "random_seed": None,
            "quantile_statistic_interpolation_method": "nearest",
            "quantile_bias_correction": False,
            "quantile_bias_std_error_ratio_threshold": None,
            "include_estimator_samples_histogram_in_details": False,
            "truncate_values": {
                "lower_bound": 0.0,
                "upper_bound": None,
            },
            "round_decimals": None,
        }
        parameter_builders: List[ParameterBuilder] = [
            column_distinct_values_count_metric_multi_batch_parameter_builder_for_metrics,
            column_value_counts_metric_multi_batch_parameter_builder_for_metrics,
        ]
        expectation_configuration_builders: List[ExpectationConfigurationBuilder] = [
            expect_column_values_to_be_in_set_expectation_configuration_builder,
            expect_column_unique_value_count_to_be_between_expectation_configuration_builder,
            expect_column_proportion_of_unique_values_to_be_between_expectation_configuration_builder,
        ]
        rule = Rule(
            name="categorical_columns_rule",
            variables=variables,
            domain_builder=categorical_column_type_domain_builder,
            parameter_builders=parameter_builders,
            expectation_configuration_builders=expectation_configuration_builders,
        )

        return rule
