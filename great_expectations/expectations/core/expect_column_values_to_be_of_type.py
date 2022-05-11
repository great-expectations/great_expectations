import inspect
import logging
import warnings
from typing import Dict, Optional

import numpy as np
import pandas as pd

from great_expectations.core import ExpectationConfiguration
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.registry import get_metric_kwargs
from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.render.util import (
    num_to_str,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)
from great_expectations.util import get_pyathena_potential_type
from great_expectations.validator.metric_configuration import MetricConfiguration

logger = logging.getLogger(__name__)
try:
    import pyspark.sql.types as sparktypes
except ImportError as e:
    logger.debug(str(e))
    logger.debug(
        "Unable to load spark context; install optional spark dependency for support."
    )
try:
    import sqlalchemy as sa
    from sqlalchemy.dialects import registry
except ImportError:
    logger.debug(
        "Unable to load SqlAlchemy context; install optional sqlalchemy dependency for support"
    )
    sa = None
    registry = None
try:
    import sqlalchemy_redshift.dialect
except ImportError:
    sqlalchemy_redshift = None
_BIGQUERY_MODULE_NAME = "sqlalchemy_bigquery"
BIGQUERY_GEO_SUPPORT = False
try:
    import sqlalchemy_bigquery as sqla_bigquery

    registry.register("bigquery", _BIGQUERY_MODULE_NAME, "BigQueryDialect")
    bigquery_types_tuple = None
    try:
        from sqlalchemy_bigquery import GEOGRAPHY

        BIGQUERY_GEO_SUPPORT = True
    except ImportError:
        BIGQUERY_GEO_SUPPORT = False
except ImportError:
    try:
        import pybigquery.sqlalchemy_bigquery as sqla_bigquery

        warnings.warn(
            "The pybigquery package is obsolete and its usage within Great Expectations is deprecated as of v0.14.7. As support will be removed in v0.17, please transition to sqlalchemy-bigquery",
            DeprecationWarning,
        )
        _BIGQUERY_MODULE_NAME = "pybigquery.sqlalchemy_bigquery"
        registry.register("bigquery", _BIGQUERY_MODULE_NAME, "dialect")
        try:
            getattr(sqla_bigquery, "INTEGER")
            bigquery_types_tuple = None
        except AttributeError:
            logger.warning(
                "Old pybigquery driver version detected. Consider upgrading to 0.4.14 or later."
            )
            from collections import namedtuple

            BigQueryTypes = namedtuple("BigQueryTypes", sorted(sqla_bigquery._type_map))
            bigquery_types_tuple = BigQueryTypes(**sqla_bigquery._type_map)
    except ImportError:
        sqla_bigquery = None
        bigquery_types_tuple = None
        pybigquery = None
try:
    import teradatasqlalchemy.dialect
    import teradatasqlalchemy.types as teradatatypes
except ImportError:
    teradatasqlalchemy = None


class ExpectColumnValuesToBeOfType(ColumnMapExpectation):
    "Expect a column to contain values of a specified data type.\n\n    expect_column_values_to_be_of_type is a :func:`column_aggregate_expectation     <great_expectations.dataset.dataset.MetaDataset.column_aggregate_expectation>` for typed-column backends,\n    and also for PandasDataset where the column dtype and provided type_ are unambiguous constraints (any dtype\n    except 'object' or dtype of 'object' with type_ specified as 'object').\n\n    For PandasDataset columns with dtype of 'object' expect_column_values_to_be_of_type is a\n    :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>` and will\n    independently check each row's type.\n\n    Args:\n        column (str):             The column name.\n        type\\_ (str):             A string representing the data type that each column should have as entries. Valid types are defined\n            by the current backend implementation and are dynamically loaded. For example, valid types for\n            PandasDataset include any numpy dtype values (such as 'int64') or native python types (such as 'int'),\n            whereas valid types for a SqlAlchemyDataset include types named by the current driver such as 'INTEGER'\n            in most SQL dialects and 'TEXT' in dialects such as postgresql. Valid types for SparkDFDataset include\n            'StringType', 'BooleanType' and other pyspark-defined type names.\n\n\n    Keyword Args:\n        mostly (None or a float between 0 and 1):             Return `\"success\": True` if at least mostly fraction of values match the expectation.             For more detail, see :ref:`mostly`.\n\n    Other Parameters:\n        result_format (str or None):             Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n            For more detail, see :ref:`result_format <result_format>`.\n        include_config (boolean):             If True, then include the expectation config as part of the result object.             For more detail, see :ref:`include_config`.\n        catch_exceptions (boolean or None):             If True, then catch exceptions and include them as part of the result object.             For more detail, see :ref:`catch_exceptions`.\n        meta (dict or None):             A JSON-serializable dictionary (nesting allowed) that will be included in the output without             modification. For more detail, see :ref:`meta`.\n\n    Returns:\n        An ExpectationSuiteValidationResult\n\n        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n    See also:\n        :func:`expect_column_values_to_be_in_type_list         <great_expectations.dataset.dataset.Dataset.expect_column_values_to_be_in_type_list>`\n\n"
    library_metadata = {
        "maturity": "production",
        "tags": ["core expectation", "column map expectation"],
        "contributors": ["@great_expectations"],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": True,
    }
    map_metric = "column_values.of_type"
    success_keys = ("type_", "mostly")
    default_kwarg_values = {
        "type_": None,
        "mostly": 1,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }
    args_keys = ("column", "type_")

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        super().validate_configuration(configuration)
        try:
            assert "type_" in configuration.kwargs, "type_ is required"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    @classmethod
    def _atomic_prescriptive_template(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name", True)
        include_column_name = (
            include_column_name if (include_column_name is not None) else True
        )
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(
            configuration.kwargs,
            ["column", "type_", "mostly", "row_condition", "condition_parser"],
        )
        params_with_json_schema = {
            "column": {"schema": {"type": "string"}, "value": params.get("column")},
            "type_": {"schema": {"type": "string"}, "value": params.get("type_")},
            "mostly": {"schema": {"type": "number"}, "value": params.get("mostly")},
            "mostly_pct": {
                "schema": {"type": "string"},
                "value": params.get("mostly_pct"),
            },
            "row_condition": {
                "schema": {"type": "string"},
                "value": params.get("row_condition"),
            },
            "condition_parser": {
                "schema": {"type": "string"},
                "value": params.get("condition_parser"),
            },
        }
        if (params["mostly"] is not None) and (params["mostly"] < 1.0):
            params_with_json_schema["mostly_pct"]["value"] = num_to_str(
                (params["mostly"] * 100), precision=15, no_scientific=True
            )
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
            ) = parse_row_condition_string_pandas_engine(
                params["row_condition"], with_schema=True
            )
            template_str = f"{conditional_template_str}, then {template_str}"
            params_with_json_schema.update(conditional_params)
        return (template_str, params_with_json_schema, styling)

    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name", True)
        include_column_name = (
            include_column_name if (include_column_name is not None) else True
        )
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(
            configuration.kwargs,
            ["column", "type_", "mostly", "row_condition", "condition_parser"],
        )
        if (params["mostly"] is not None) and (params["mostly"] < 1.0):
            params["mostly_pct"] = num_to_str(
                (params["mostly"] * 100), precision=15, no_scientific=True
            )
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

    def _validate_pandas(self, actual_column_type, expected_type):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
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
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if expected_type is None:
            success = True
        else:
            types = []
            type_module = _get_dialect_type_module(execution_engine=execution_engine)
            try:
                if (
                    (expected_type.lower() == "geography")
                    and (execution_engine.engine.dialect.name.lower() == "bigquery")
                    and (not BIGQUERY_GEO_SUPPORT)
                ):
                    logger.warning(
                        (
                            "BigQuery GEOGRAPHY type is not supported by default. "
                            + "To install support, please run:"
                        )
                        + "  $ pip install 'sqlalchemy-bigquery[geography]'"
                    )
                elif type_module.__name__ == "pyathena.sqlalchemy_athena":
                    potential_type = get_pyathena_potential_type(
                        type_module, expected_type
                    )
                    if not inspect.isclass(potential_type):
                        real_type = type(potential_type)
                    else:
                        real_type = potential_type
                    types.append(real_type)
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

    def _validate_spark(self, actual_column_type, expected_type):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if expected_type is None:
            success = True
        else:
            types = []
            try:
                type_class = getattr(sparktypes, expected_type)
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
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        dependencies = super(ColumnMapExpectation, self).get_validation_dependencies(
            configuration, execution_engine, runtime_configuration
        )
        if isinstance(execution_engine, PandasExecutionEngine):
            column_name = configuration.kwargs.get("column")
            expected_type = configuration.kwargs.get("type_")
            metric_kwargs = get_metric_kwargs(
                configuration=configuration,
                metric_name="table.column_types",
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
                    if (type_dict["name"] == column_name)
                ][0]
            except IndexError:
                actual_column_type = None
            if (
                actual_column_type
                and (actual_column_type.type.__name__ == "object_")
                and (expected_type not in ["object", "object_", "O", None])
            ):
                dependencies = super().get_validation_dependencies(
                    configuration, execution_engine, runtime_configuration
                )
        column_types_metric_kwargs = get_metric_kwargs(
            metric_name="table.column_types",
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )
        dependencies["metrics"]["table.column_types"] = MetricConfiguration(
            metric_name="table.column_types",
            metric_domain_kwargs=column_types_metric_kwargs["metric_domain_kwargs"],
            metric_value_kwargs=column_types_metric_kwargs["metric_value_kwargs"],
        )
        return dependencies

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        column_name = configuration.kwargs.get("column")
        expected_type = configuration.kwargs.get("type_")
        actual_column_types_list = metrics.get("table.column_types")
        actual_column_type = [
            type_dict["type"]
            for type_dict in actual_column_types_list
            if (type_dict["name"] == column_name)
        ][0]
        if isinstance(execution_engine, PandasExecutionEngine):
            if (actual_column_type.type.__name__ == "object_") and (
                expected_type not in ["object", "object_", "O", None]
            ):
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


def _get_dialect_type_module(execution_engine):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    if execution_engine.dialect_module is None:
        logger.warning(
            "No sqlalchemy dialect found; relying in top-level sqlalchemy types."
        )
        return sa
    try:
        if isinstance(
            execution_engine.dialect_module, sqlalchemy_redshift.dialect.RedshiftDialect
        ):
            return execution_engine.dialect_module.sa
    except (TypeError, AttributeError):
        pass
    try:
        if isinstance(
            execution_engine.dialect_module, sqla_bigquery.BigQueryDialect
        ) and (bigquery_types_tuple is not None):
            return bigquery_types_tuple
    except (TypeError, AttributeError):
        pass
    try:
        if issubclass(
            execution_engine.dialect_module, teradatasqlalchemy.dialect.TeradataDialect
        ) and (teradatatypes is not None):
            return teradatatypes
    except (TypeError, AttributeError):
        pass
    return execution_engine.dialect_module


def _native_type_type_map(type_):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
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
