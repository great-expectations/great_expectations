from typing import Dict

from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    TableExpectation,
)
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.render.util import (
    num_to_str,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)


class ExpectColumnValuesToBeInTypeList(TableExpectation):
    """
    Expect a column to contain values from a specified type list.

    expect_column_values_to_be_in_type_list is a :func:`column_aggregate_expectation \
    <great_expectations.dataset.dataset.MetaDataset.column_aggregate_expectation>` for typed-column backends,
    and also for PandasDataset where the column dtype provides an unambiguous constraints (any dtype except
    'object'). For PandasDataset columns with dtype of 'object' expect_column_values_to_be_of_type is a
    :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>` and will
    independently check each row's type.

    Args:
        column (str): \
            The column name.
        type_list (str): \
            A list of strings representing the data type that each column should have as entries. Valid types are
            defined by the current backend implementation and are dynamically loaded. For example, valid types for
            PandasDataset include any numpy dtype values (such as 'int64') or native python types (such as 'int'),
            whereas valid types for a SqlAlchemyDataset include types named by the current driver such as 'INTEGER'
            in most SQL dialects and 'TEXT' in dialects such as postgresql. Valid types for SparkDFDataset include
            'StringType', 'BooleanType' and other pyspark-defined type names.

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

    See also:
        :func:`expect_column_values_to_be_of_type \
        <great_expectations.dataset.dataset.Dataset.expect_column_values_to_be_of_type>`
    """

    metric_dependencies = ("table.column_types",)
    success_keys = (
        "type_list",
        "mostly",
    )
    default_kwarg_values = {
        "type_list": None,
        "mostly": 1,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }

    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    def _prescriptive_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs
    ):
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name", True)
        include_column_name = (
            include_column_name if include_column_name is not None else True
        )
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(
            configuration.kwargs,
            ["column", "type_list", "mostly", "row_condition", "condition_parser"],
        )

        if params["type_list"] is not None:
            for i, v in enumerate(params["type_list"]):
                params["v__" + str(i)] = v
            values_string = " ".join(
                ["$v__" + str(i) for i, v in enumerate(params["type_list"])]
            )

            if params["mostly"] is not None:
                params["mostly_pct"] = num_to_str(
                    params["mostly"] * 100, precision=15, no_scientific=True
                )
                # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
                if include_column_name:
                    template_str = (
                        "$column value types must belong to this set: "
                        + values_string
                        + ", at least $mostly_pct % of the time."
                    )
                else:
                    template_str = (
                        "value types must belong to this set: "
                        + values_string
                        + ", at least $mostly_pct % of the time."
                    )
            else:
                if include_column_name:
                    template_str = (
                        "$column value types must belong to this set: "
                        + values_string
                        + "."
                    )
                else:
                    template_str = (
                        "value types must belong to this set: " + values_string + "."
                    )
        else:
            if include_column_name:
                template_str = "$column value types may be any value, but observed value will be reported"
            else:
                template_str = (
                    "value types may be any value, but observed value will be reported"
                )

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
                        "styling": styling,
                    },
                }
            )
        ]

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        column_name = configuration.kwargs.get("column")
        column_types_list = metrics.get("table.column_types")
        column_type = [
            type_dict["type"]
            for type_dict in column_types_list
            if type_dict["name"] == column_name
        ][0]
        test = 1
        return {"success": True}
