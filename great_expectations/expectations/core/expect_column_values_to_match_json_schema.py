import json
from typing import Dict, List, Optional, Union

import jsonschema
import numpy as np
import pandas as pd

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)

from ...core.batch import Batch
from ...data_asset.util import parse_result_format
from ...execution_engine.sqlalchemy_execution_engine import SqlAlchemyExecutionEngine
from ...render.types import RenderedStringTemplateContent
from ...render.util import (
    num_to_str,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)
from ..expectation import (
    ColumnMapDatasetExpectation,
    Expectation,
    InvalidExpectationConfigurationError,
    _format_map_output,
    renderer,
)
from ..registry import extract_metrics, get_metric_kwargs

try:
    import sqlalchemy as sa
except ImportError:
    pass


class ExpectColumnValuesToMatchJsonSchema(ColumnMapDatasetExpectation):
    """Expect column entries to be JSON objects matching a given JSON schema.

    expect_column_values_to_match_json_schema is a \
    :func:`column_map_expectation <great_expectations.execution_engine.execution_engine.MetaExecutionEngine
    .column_map_expectation>`.

    Args:
        column (str): \
            The column name.

    Keyword Args:
        mostly (None or a float between 0 and 1): \
            Return `"success": True` if at least mostly fraction of values match the expectation. \
            For more detail, see :ref:`mostly`.

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
            For more detail, see :ref:`result_format <result_format>`.
        include_config (boolean): \
            If True, then include the expectation config as part of the result object. \
            For more detail, see :ref:`include_config`.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see :ref:`catch_exceptions`.
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see :ref:`meta`.

    Returns:
        An ExpectationSuiteValidationResult

        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

    See Also:
        :func:`expect_column_values_to_be_json_parseable \
        <great_expectations.execution_engine.execution_engine.ExecutionEngine
        .expect_column_values_to_be_json_parseable>`


        The `JSON-schema docs <http://json-schema.org/>`_.
    """

    map_metric = "column_values.match_json_schema"
    success_keys = (
        "json_schema",
        "mostly",
    )

    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,  # we expect this to be explicitly set whenever a row_condition is passed
        "mostly": 1,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": True,
    }

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        super().validate_configuration(configuration)

        return True

    @classmethod
    @renderer(renderer_name="descriptive")
    def _descriptive_renderer(
        cls, expectation_configuration, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation_configuration.kwargs,
            ["column", "mostly", "json_schema", "row_condition", "condition_parser"],
        )

        if not params.get("json_schema"):
            template_str = "values must match a JSON Schema but none was specified."
        else:
            params["formatted_json"] = (
                "<pre>" + json.dumps(params.get("json_schema"), indent=4) + "</pre>"
            )
            if params["mostly"] is not None:
                params["mostly_pct"] = num_to_str(
                    params["mostly"] * 100, precision=15, no_scientific=True
                )
                # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
                template_str = "values must match the following JSON Schema, at least $mostly_pct % of the time: $formatted_json"
            else:
                template_str = (
                    "values must match the following JSON Schema: $formatted_json"
                )

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": {"params": {"formatted_json": {"classes": []}}},
                    },
                }
            )
        ]

    # @SqlAlchemyExecutionEngine.column_map_metric(
    #     metric_name="column_values.match_json_schema",
    #     metric_domain_keys=ColumnMapDatasetExpectation.domain_keys,
    #     metric_value_keys=("json",),
    #     metric_dependencies=tuple(),
    # )
    # def _sqlalchemy_match_json_schema(
    #     self,
    #     column: sa.column,
    #     json: str,
    #     runtime_configuration: dict = None,
    #     filter_column_isnull: bool = True,
    # ):
    #     json_expression = execution_engine._get_dialect_json_expression(column, json)
    #     if json_expression is None:
    #         logger.warning(
    #             "json is not supported for dialect %s" % str(self.sql_engine_dialect)
    #         )
    #         raise NotImplementedError
    #
    #     return json_expression
    #     if json is None:
    #         # vacuously true
    #         return True
    #
    #     return column.in_(tuple(json))
    #
    # @SparkDFExecutionEngine.column_map_metric(
    #     metric_name="column_values.match_json_schema",
    #     metric_domain_keys=ColumnMapDatasetExpectation.domain_keys,
    #     metric_value_keys=("json",),
    #     metric_dependencies=tuple(),
    # )
    # def _spark_match_json_schema(
    #     self,
    #     data: "pyspark.sql.DataFrame",
    #     column: str,
    #     json: str,
    #     runtime_configuration: dict = None,
    #     filter_column_isnull: bool = True,
    # ):
    #     import pyspark.sql.functions as F
    #
    #     if json is None:
    #         # vacuously true
    #         return data.withColumn(column + "__success", F.lit(True))
    #
    #     return data.withColumn(column + "__success", F.col(column).isin(json))
