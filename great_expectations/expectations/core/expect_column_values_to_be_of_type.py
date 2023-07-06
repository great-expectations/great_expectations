import inspect
import logging
from typing import TYPE_CHECKING, Dict, Optional

import numpy as np
import pandas as pd

from great_expectations.compatibility import aws, pyspark, trino
from great_expectations.compatibility.sqlalchemy import (
    sqlalchemy as sa,
)
from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)
from great_expectations.core._docs_decorators import public_api
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_dialect import GXSqlDialect
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    render_evaluation_parameter_string,
)
from great_expectations.expectations.registry import get_metric_kwargs
from great_expectations.render import LegacyRendererType, RenderedStringTemplateContent
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.renderer_configuration import (
    RendererConfiguration,
    RendererValueType,
)
from great_expectations.render.util import (
    num_to_str,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)
from great_expectations.util import (
    get_clickhouse_sqlalchemy_potential_type,
    get_pyathena_potential_type,
)
from great_expectations.validator.metric_configuration import MetricConfiguration
from great_expectations.validator.validator import (
    ValidationDependencies,
)

if TYPE_CHECKING:
    from great_expectations.render.renderer_configuration import AddParamArgs

logger = logging.getLogger(__name__)


_BIGQUERY_MODULE_NAME = "sqlalchemy_bigquery"
BIGQUERY_GEO_SUPPORT = False
from great_expectations.compatibility.bigquery import (
    GEOGRAPHY,
    bigquery_types_tuple,
)
from great_expectations.compatibility.bigquery import (
    sqlalchemy_bigquery as BigQueryDialect,
)

if GEOGRAPHY:
    BIGQUERY_GEO_SUPPORT = True
else:
    BIGQUERY_GEO_SUPPORT = False

try:
    import teradatasqlalchemy.dialect
    import teradatasqlalchemy.types as teradatatypes
except ImportError:
    teradatasqlalchemy = None

try:
    import clickhouse_sqlalchemy
    import clickhouse_sqlalchemy.types as ch_types
except (ImportError, KeyError):
    clickhouse_sqlalchemy = None
    ch_types = None


class ExpectColumnValuesToBeOfType(ColumnMapExpectation):
    """Expect a column to contain values of a specified data type.

    expect_column_values_to_be_of_type is a \
    [Column Map Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_map_expectations) \
    for typed-column backends, and also for PandasDataset where the column dtype and provided \
    type_ are unambiguous constraints (any dtype except 'object' or dtype of 'object' with \
    type_ specified as 'object').

    For PandasDataset columns with dtype of 'object' expect_column_values_to_be_of_type will
    independently check each row's type.

    Args:
        column (str): \
            The column name.
        type\\_ (str): \
            A string representing the data type that each column should have as entries. Valid types are defined \
            by the current backend implementation and are dynamically loaded. For example, valid types for \
            PandasDataset include any numpy dtype values (such as 'int64') or native python types (such as 'int'), \
            whereas valid types for a SqlAlchemyDataset include types named by the current driver such as 'INTEGER' \
            in most SQL dialects and 'TEXT' in dialects such as postgresql. Valid types for SparkDFDataset include \
            'StringType', 'BooleanType' and other pyspark-defined type names. Note that the strings representing these \
            types are sometimes case-sensitive. For instance, with a Pandas backend `timestamp` will be unrecognized and
            fail the expectation, while `Timestamp` would pass with valid data.

    Keyword Args:
        mostly (None or a float between 0 and 1): \
            Successful if at least mostly fraction of values match the expectation. \
            For more detail, see [mostly](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#mostly).

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: BOOLEAN_ONLY, BASIC, COMPLETE, or SUMMARY. \
            For more detail, see [result_format](https://docs.greatexpectations.io/docs/reference/expectations/result_format).
        include_config (boolean): \
            If True, then include the expectation config as part of the result object.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see [catch_exceptions](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#catch_exceptions).
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see [meta](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#meta).

    Returns:
        An [ExpectationSuiteValidationResult](https://docs.greatexpectations.io/docs/terms/validation_result)

        Exact fields vary depending on the values passed to result_format, include_config, catch_exceptions, and meta.

    See also:
        [expect_column_values_to_be_in_type_list](https://greatexpectations.io/expectations/expect_column_values_to_be_in_type_list)
    """

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "production",
        "tags": ["core expectation", "column map expectation"],
        "contributors": ["@great_expectations"],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": True,
    }

    map_metric = "column_values.of_type"
    success_keys = (
        "type_",
        "mostly",
    )
    default_kwarg_values = {
        "type_": None,
        "mostly": 1,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }
    args_keys = (
        "column",
        "type_",
    )

    @public_api
    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        """Validates the configuration of an Expectation.

        For `expect_column_values_to_be_of_type` it is required that:

        - `_type` has been provided.

        The configuration will also be validated using each of the `validate_configuration` methods in its Expectation
        superclass hierarchy.

        Args:
            configuration: An `ExpectationConfiguration` to validate. If no configuration is provided, it will be pulled
                           from the configuration attribute of the Expectation instance.

        Raises:
            InvalidExpectationConfigurationError: The configuration does not contain the values required by the
                                                  Expectation.
        """
        super().validate_configuration(configuration)
        configuration = configuration or self.configuration
        try:
            assert "type_" in configuration.kwargs, "type_ is required"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    @classmethod
    def _prescriptive_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        add_param_args: AddParamArgs = (
            ("column", RendererValueType.STRING),
            ("type_", RendererValueType.STRING),
            ("mostly", RendererValueType.NUMBER),
        )
        for name, param_type in add_param_args:
            renderer_configuration.add_param(name=name, param_type=param_type)

        params = renderer_configuration.params

        if params.mostly and params.mostly.value < 1.0:  # noqa: PLR2004
            renderer_configuration = cls._add_mostly_pct_param(
                renderer_configuration=renderer_configuration
            )
            template_str = (
                "values must be of type $type_, at least $mostly_pct % of the time."
            )
        else:
            template_str = "values must be of type $type_."

        if renderer_configuration.include_column_name:
            template_str = f"$column {template_str}"

        renderer_configuration.template_str = template_str

        return renderer_configuration

    @classmethod
    @renderer(renderer_type=LegacyRendererType.PRESCRIPTIVE)
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ):
        runtime_configuration = runtime_configuration or {}
        include_column_name = (
            False if runtime_configuration.get("include_column_name") is False else True
        )
        styling = runtime_configuration.get("styling")

        params = substitute_none_for_missing(
            configuration.kwargs,
            ["column", "type_", "mostly", "row_condition", "condition_parser"],
        )

        if params["mostly"] is not None and params["mostly"] < 1.0:  # noqa: PLR2004
            params["mostly_pct"] = num_to_str(
                params["mostly"] * 100, precision=15, no_scientific=True
            )
            # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
            template_str = (
                "values must be of type $type_, at least $mostly_pct % of the time."
            )
        else:
            template_str = "values must be of type $type_."

        if include_column_name:
            template_str = f"$column {template_str}"

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = f"{conditional_template_str}, then {template_str}"
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    def _validate_pandas(
        self,
        actual_column_type,
        expected_type,
    ):
        if expected_type is None:
            success = True
        else:
            comp_types = []

            try:
                comp_types.append(np.dtype(expected_type).type)
            except TypeError:
                try:
                    pd_type = getattr(pd, expected_type)
                    if isinstance(pd_type, type):
                        comp_types.append(pd_type)
                except AttributeError:
                    pass

                try:
                    pd_type = getattr(pd.core.dtypes.dtypes, expected_type)
                    if isinstance(pd_type, type):
                        comp_types.append(pd_type)
                except AttributeError:
                    pass

            native_type = _native_type_type_map(expected_type)
            if native_type is not None:
                comp_types.extend(native_type)

            success = actual_column_type.type in comp_types

        return {
            "success": success,
            "result": {"observed_value": actual_column_type.type.__name__},
        }

    def _validate_sqlalchemy(self, actual_column_type, expected_type, execution_engine):
        # Our goal is to be as explicit as possible. We will match the dialect
        # if that is possible. If there is no dialect available, we *will*
        # match against a top-level SqlAlchemy type.
        #
        # This is intended to be a conservative approach.
        #
        # In particular, we *exclude* types that would be valid under an ORM
        # such as "float" for postgresql with this approach

        if expected_type is None:
            success = True
        else:
            types = []
            type_module = _get_dialect_type_module(execution_engine=execution_engine)
            try:
                # bigquery geography requires installing an extra package
                if (
                    expected_type.lower() == "geography"
                    and execution_engine.engine.dialect.name.lower()
                    == GXSqlDialect.BIGQUERY
                    and not BIGQUERY_GEO_SUPPORT
                ):
                    logger.warning(
                        "BigQuery GEOGRAPHY type is not supported by default. "
                        + "To install support, please run:"
                        + "  $ pip install 'sqlalchemy-bigquery[geography]'"
                    )
                elif type_module.__name__ == "pyathena.sqlalchemy_athena":
                    potential_type = get_pyathena_potential_type(
                        type_module, expected_type
                    )
                    # In the case of the PyAthena dialect we need to verify that
                    # the type returned is indeed a type and not an instance.
                    if not inspect.isclass(potential_type):
                        real_type = type(potential_type)
                    else:
                        real_type = potential_type
                    types.append(real_type)
                elif type_module.__name__ == "clickhouse_sqlalchemy.drivers.base":
                    actual_column_type = get_clickhouse_sqlalchemy_potential_type(
                        type_module, actual_column_type
                    )()
                    potential_type = get_clickhouse_sqlalchemy_potential_type(
                        type_module, expected_type
                    )
                    types.append(potential_type)
                else:
                    potential_type = getattr(type_module, expected_type)
                    types.append(potential_type)
            except AttributeError:
                logger.debug(f"Unrecognized type: {expected_type}")
            if len(types) == 0:
                logger.warning(
                    "No recognized sqlalchemy types in type_list for current dialect."
                )
            types = tuple(types)
            success = isinstance(actual_column_type, types)

        return {
            "success": success,
            "result": {"observed_value": type(actual_column_type).__name__},
        }

    def _validate_spark(
        self,
        actual_column_type,
        expected_type,
    ):
        if expected_type is None:
            success = True
        else:
            types = []
            try:
                type_class = getattr(pyspark.types, expected_type)
                types.append(type_class)
            except AttributeError:
                logger.debug(f"Unrecognized type: {expected_type}")
            if len(types) == 0:
                raise ValueError("No recognized spark types in expected_types_list")
            types = tuple(types)
            success = isinstance(actual_column_type, types)
        return {
            "success": success,
            "result": {"observed_value": type(actual_column_type).__name__},
        }

    def get_validation_dependencies(
        self,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ) -> ValidationDependencies:
        # This calls BatchExpectation.get_validation_dependencies to set baseline validation_dependencies for the aggregate version
        # of the expectation.
        # We need to keep this as super(ColumnMapExpectation, self), which calls
        # BatchExpectation.get_validation_dependencies instead of ColumnMapExpectation.get_validation_dependencies.
        # This is because the map version of this expectation is only supported for Pandas, so we want the aggregate
        # version for the other backends.
        validation_dependencies: ValidationDependencies = super(
            ColumnMapExpectation, self
        ).get_validation_dependencies(
            configuration, execution_engine, runtime_configuration
        )

        # Only PandasExecutionEngine supports the column map version of the expectation.
        if isinstance(execution_engine, PandasExecutionEngine):
            column_name = configuration.kwargs.get("column")
            expected_type = configuration.kwargs.get("type_")
            metric_kwargs = get_metric_kwargs(
                metric_name="table.column_types",
                configuration=configuration,
                runtime_configuration=runtime_configuration,
            )
            metric_domain_kwargs = metric_kwargs.get("metric_domain_kwargs")
            metric_value_kwargs = metric_kwargs.get("metric_value_kwargs")
            table_column_types_configuration = MetricConfiguration(
                "table.column_types",
                metric_domain_kwargs=metric_domain_kwargs,
                metric_value_kwargs=metric_value_kwargs,
            )
            actual_column_types_list = execution_engine.resolve_metrics(
                [table_column_types_configuration]
            )[table_column_types_configuration.id]
            try:
                actual_column_type = [
                    type_dict["type"]
                    for type_dict in actual_column_types_list
                    if type_dict["name"] == column_name
                ][0]
            except IndexError:
                actual_column_type = None

            # only use column map version if column dtype is object
            if (
                actual_column_type
                and actual_column_type.type.__name__ == "object_"
                and expected_type
                not in [
                    "object",
                    "object_",
                    "O",
                    None,
                ]
            ):
                # this resets validation_dependencies using  ColumnMapExpectation.get_validation_dependencies
                validation_dependencies = super().get_validation_dependencies(
                    configuration, execution_engine, runtime_configuration
                )

        # this adds table.column_types dependency for both aggregate and map versions of expectation
        column_types_metric_kwargs = get_metric_kwargs(
            metric_name="table.column_types",
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )
        validation_dependencies.set_metric_configuration(
            metric_name="table.column_types",
            metric_configuration=MetricConfiguration(
                metric_name="table.column_types",
                metric_domain_kwargs=column_types_metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=column_types_metric_kwargs["metric_value_kwargs"],
            ),
        )

        return validation_dependencies

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ):
        column_name = configuration.kwargs.get("column")
        expected_type = configuration.kwargs.get("type_")
        actual_column_types_list = metrics.get("table.column_types")
        actual_column_type = [
            type_dict["type"]
            for type_dict in actual_column_types_list
            if type_dict["name"] == column_name
        ][0]

        if isinstance(execution_engine, PandasExecutionEngine):
            # only PandasExecutionEngine supports map version of expectation and
            # only when column type is object
            if actual_column_type.type.__name__ == "object_" and expected_type not in [
                "object",
                "object_",
                "O",
                None,
            ]:
                # this calls ColumnMapMetric._validate
                return super()._validate(
                    configuration, metrics, runtime_configuration, execution_engine
                )
            return self._validate_pandas(
                actual_column_type=actual_column_type, expected_type=expected_type
            )
        elif isinstance(execution_engine, SqlAlchemyExecutionEngine):
            return self._validate_sqlalchemy(
                actual_column_type=actual_column_type,
                expected_type=expected_type,
                execution_engine=execution_engine,
            )
        elif isinstance(execution_engine, SparkDFExecutionEngine):
            return self._validate_spark(
                actual_column_type=actual_column_type, expected_type=expected_type
            )


def _get_dialect_type_module(  # noqa: PLR0911, PLR0912
    execution_engine,
):
    if execution_engine.dialect_module is None:
        logger.warning(
            "No sqlalchemy dialect found; relying in top-level sqlalchemy types."
        )
        return sa

    # Redshift does not (yet) export types to top level; only recognize base SA types
    if aws.redshiftdialect and isinstance(
        execution_engine.dialect_module,
        aws.redshiftdialect.RedshiftDialect,
    ):
        return execution_engine.dialect_module.sa
    else:
        pass

    # Bigquery works with newer versions, but use a patch if we had to define bigquery_types_tuple
    try:
        if BigQueryDialect and (
            isinstance(
                execution_engine.dialect_module,
                BigQueryDialect,
            )
            and bigquery_types_tuple is not None
        ):
            return bigquery_types_tuple
    except (TypeError, AttributeError):
        pass

    # Teradata types module
    try:
        if (
            issubclass(
                execution_engine.dialect_module,
                teradatasqlalchemy.dialect.TeradataDialect,
            )
            and teradatatypes is not None
        ):
            return teradatatypes
    except (TypeError, AttributeError):
        pass

    try:
        if (
            issubclass(
                execution_engine.dialect_module,
                clickhouse_sqlalchemy.drivers.base.ClickHouseDialect,
            )
            and ch_types is not None
        ):
            return ch_types
    except (TypeError, AttributeError):
        pass

    # Trino types module
    try:
        if (
            trino.trinodialect
            and trino.trinotypes
            and isinstance(
                execution_engine.dialect,
                trino.trinodialect.TrinoDialect,
            )
        ):
            return trino.trinotypes
    except (TypeError, AttributeError):
        pass

    return execution_engine.dialect_module


def _native_type_type_map(type_):  # noqa: PLR0911
    # We allow native python types in cases where the underlying type is "object":
    if type_.lower() == "none":
        return (type(None),)
    elif type_.lower() == "bool":
        return (bool,)
    elif type_.lower() in ["int", "long"]:
        return (int,)
    elif type_.lower() == "float":
        return (float,)
    elif type_.lower() == "bytes":
        return (bytes,)
    elif type_.lower() == "complex":
        return (complex,)
    elif type_.lower() in ["str", "string_types"]:
        return (str,)
    elif type_.lower() == "list":
        return (list,)
    elif type_.lower() == "dict":
        return (dict,)
    elif type_.lower() == "unicode":
        return None
