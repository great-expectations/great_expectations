import logging
import traceback
from copy import deepcopy

from great_expectations.render.renderer.content_block.expectation_string import (
    ExpectationStringRenderer,
)
from great_expectations.render.types import (
    CollapseContent,
    RenderedContentBlockContainer,
    RenderedStringTemplateContent,
    RenderedTableContent,
)
from great_expectations.render.util import num_to_str

logger = logging.getLogger(__name__)


class ValidationResultsTableContentBlockRenderer(ExpectationStringRenderer):
    _content_block_type = "table"
    _rendered_component_type = RenderedTableContent
    _rendered_component_default_init_kwargs = {
        "table_options": {"search": True, "icon-size": "sm"}
    }

    _default_element_styling = {
        "default": {"classes": ["badge", "badge-secondary"]},
        "params": {"column": {"classes": ["badge", "badge-primary"]}},
    }

    _default_content_block_styling = {
        "body": {"classes": ["table"],},
        "classes": ["ml-2", "mr-2", "mt-0", "mb-0", "table-responsive"],
    }

    @classmethod
    def _get_status_icon(cls, evr):
        if evr.exception_info["raised_exception"]:
            return RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "$icon",
                        "params": {"icon": "", "markdown_status_icon": "❗"},
                        "styling": {
                            "params": {
                                "icon": {
                                    "classes": [
                                        "fas",
                                        "fa-exclamation-triangle",
                                        "text-warning",
                                    ],
                                    "tag": "i",
                                }
                            }
                        },
                    },
                }
            )

        if evr.success:
            return RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "$icon",
                        "params": {"icon": "", "markdown_status_icon": "✅"},
                        "styling": {
                            "params": {
                                "icon": {
                                    "classes": [
                                        "fas",
                                        "fa-check-circle",
                                        "text-success",
                                    ],
                                    "tag": "i",
                                }
                            }
                        },
                    },
                    "styling": {
                        "parent": {
                            "classes": ["hide-succeeded-validation-target-child"]
                        }
                    },
                }
            )
        else:
            return RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "$icon",
                        "params": {"icon": "", "markdown_status_icon": "❌"},
                        "styling": {
                            "params": {
                                "icon": {
                                    "tag": "i",
                                    "classes": ["fas", "fa-times", "text-danger"],
                                }
                            }
                        },
                    },
                }
            )

    @classmethod
    def _get_unexpected_table(cls, evr):
        try:
            result = evr.result
        except KeyError:
            return None

        if result is None:
            return None

        if not result.get("partial_unexpected_list") and not result.get(
            "partial_unexpected_counts"
        ):
            return None

        table_rows = []

        if result.get("partial_unexpected_counts"):
            # We will check to see whether we have *all* of the unexpected values
            # accounted for in our count, and include counts if we do. If we do not,
            # we will use this as simply a better (non-repeating) source of
            # "sampled" unexpected values
            total_count = 0
            for unexpected_count_dict in result.get("partial_unexpected_counts"):
                if not isinstance(unexpected_count_dict, dict):
                    # handles case: "partial_exception_counts requires a hashable type"
                    # this case is also now deprecated (because the error is moved to an errors key
                    # the error also *should have* been updated to "partial_unexpected_counts ..." long ago.
                    # NOTE: JPC 20200724 - Consequently, this codepath should be removed by approximately Q1 2021
                    continue
                value = unexpected_count_dict.get("value")
                count = unexpected_count_dict.get("count")
                total_count += count
                if value is not None and value != "":
                    table_rows.append([value, count])
                elif value == "":
                    table_rows.append(["EMPTY", count])
                else:
                    table_rows.append(["null", count])

            # Check to see if we have *all* of the unexpected values accounted for. If so,
            # we show counts. If not, we only show "sampled" unexpected values.
            if total_count == result.get("unexpected_count"):
                header_row = ["Unexpected Value", "Count"]
            else:
                header_row = ["Sampled Unexpected Values"]
                table_rows = [[row[0]] for row in table_rows]
        else:
            header_row = ["Sampled Unexpected Values"]
            sampled_values_set = set()
            for unexpected_value in result.get("partial_unexpected_list"):
                if unexpected_value:
                    string_unexpected_value = str(unexpected_value)
                elif unexpected_value == "":
                    string_unexpected_value = "EMPTY"
                else:
                    string_unexpected_value = "null"
                if string_unexpected_value not in sampled_values_set:
                    table_rows.append([unexpected_value])
                    sampled_values_set.add(string_unexpected_value)

        unexpected_table_content_block = RenderedTableContent(
            **{
                "content_block_type": "table",
                "table": table_rows,
                "header_row": header_row,
                "styling": {
                    "body": {"classes": ["table-bordered", "table-sm", "mt-3"]}
                },
            }
        )

        return unexpected_table_content_block

    @classmethod
    def _get_unexpected_statement(cls, evr):
        success = evr.success
        result = evr.result

        if evr.exception_info["raised_exception"]:
            exception_message_template_str = (
                "\n\n$expectation_type raised an exception:\n$exception_message"
            )

            exception_message = RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": exception_message_template_str,
                        "params": {
                            "expectation_type": evr.expectation_config.expectation_type,
                            "exception_message": evr.exception_info[
                                "exception_message"
                            ],
                        },
                        "tag": "strong",
                        "styling": {
                            "classes": ["text-danger"],
                            "params": {
                                "exception_message": {"tag": "code"},
                                "expectation_type": {
                                    "classes": ["badge", "badge-danger", "mb-2"]
                                },
                            },
                        },
                    },
                }
            )

            exception_traceback_collapse = CollapseContent(
                **{
                    "collapse_toggle_link": "Show exception traceback...",
                    "collapse": [
                        RenderedStringTemplateContent(
                            **{
                                "content_block_type": "string_template",
                                "string_template": {
                                    "template": evr.exception_info[
                                        "exception_traceback"
                                    ],
                                    "tag": "code",
                                },
                            }
                        )
                    ],
                }
            )

            return [exception_message, exception_traceback_collapse]

        if success or not result.get("unexpected_count"):
            return []
        else:
            unexpected_count = num_to_str(
                result["unexpected_count"], use_locale=True, precision=20
            )
            unexpected_percent = (
                num_to_str(result["unexpected_percent"], precision=4) + "%"
            )
            element_count = num_to_str(
                result["element_count"], use_locale=True, precision=20
            )

            template_str = (
                "\n\n$unexpected_count unexpected values found. "
                "$unexpected_percent of $element_count total rows."
            )

            return [
                RenderedStringTemplateContent(
                    **{
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": template_str,
                            "params": {
                                "unexpected_count": unexpected_count,
                                "unexpected_percent": unexpected_percent,
                                "element_count": element_count,
                            },
                            "tag": "strong",
                            "styling": {"classes": ["text-danger"]},
                        },
                    }
                )
            ]

    @classmethod
    def _get_kl_divergence_observed_value(cls, evr):
        if not evr.result.get("details"):
            return "--"

        observed_partition_object = evr.result["details"]["observed_partition"]
        observed_distribution = super()._get_kl_divergence_chart(
            observed_partition_object
        )

        observed_value = (
            num_to_str(evr.result.get("observed_value"))
            if evr.result.get("observed_value")
            else evr.result.get("observed_value")
        )

        observed_value_content_block = RenderedStringTemplateContent(
            **{
                "content_block_type": "string_template",
                "string_template": {
                    "template": "KL Divergence: $observed_value",
                    "params": {
                        "observed_value": str(observed_value)
                        if observed_value
                        else "None (-infinity, infinity, or NaN)",
                    },
                    "styling": {"classes": ["mb-2"]},
                },
            }
        )

        return RenderedContentBlockContainer(
            **{
                "content_block_type": "content_block_container",
                "content_blocks": [observed_value_content_block, observed_distribution],
            }
        )

    @classmethod
    def _get_cramers_phi_value_crosstab(cls, evr):
        observed_value = evr.result.get("observed_value")
        column_A = evr.expectation_config.kwargs["column_A"]
        column_B = evr.expectation_config.kwargs["column_B"]
        crosstab = evr.result.get("details", {}).get("crosstab")

        if observed_value is not None:
            observed_value = num_to_str(observed_value, precision=3, use_locale=True)
            if crosstab is not None:
                table = [[""] + list(crosstab.columns)]
                for col in range(len(crosstab)):
                    table.append([crosstab.index[col]] + list(crosstab.iloc[col, :]))

                return RenderedTableContent(
                    **{
                        "content_block_type": "table",
                        "header": f"Observed cramers phi of {observed_value}. \n"
                        f"Crosstab between {column_A} (rows) and {column_B} (columns):",
                        "table": table,
                        "styling": {
                            "body": {
                                "classes": [
                                    "table",
                                    "table-sm",
                                    "table-unbordered",
                                    "col-4",
                                    "mt-2",
                                ],
                            }
                        },
                    }
                )
            else:
                return observed_value
        else:
            return "--"

    @classmethod
    def _get_quantile_values_observed_value(cls, evr):
        if evr.result is None or evr.result.get("observed_value") is None:
            return "--"

        quantiles = evr.result.get("observed_value", {}).get("quantiles", [])
        value_ranges = evr.result.get("observed_value", {}).get("values", [])

        table_header_row = ["Quantile", "Value"]
        table_rows = []

        quantile_strings = {0.25: "Q1", 0.75: "Q3", 0.50: "Median"}

        for idx, quantile in enumerate(quantiles):
            quantile_string = quantile_strings.get(quantile)
            table_rows.append(
                [
                    quantile_string if quantile_string else "{:3.2f}".format(quantile),
                    str(value_ranges[idx]),
                ]
            )

        return RenderedTableContent(
            **{
                "content_block_type": "table",
                "header_row": table_header_row,
                "table": table_rows,
                "styling": {
                    "body": {
                        "classes": ["table", "table-sm", "table-unbordered", "col-4"],
                    }
                },
            }
        )

    @classmethod
    def _get_expect_table_row_count_to_equal_other_table_observed_value(cls, evr):
        if not evr.result.get("observed_value"):
            return "--"

        self_table_row_count = num_to_str(evr.result["observed_value"]["self"])
        other_table_row_count = num_to_str(evr.result["observed_value"]["other"])

        return RenderedStringTemplateContent(
            **{
                "content_block_type": "string_template",
                "string_template": {
                    "template": "Row Count: $self_table_row_count<br>Other Table Row Count: $other_table_row_count",
                    "params": {
                        "self_table_row_count": self_table_row_count,
                        "other_table_row_count": other_table_row_count,
                    },
                    "styling": {"classes": ["mb-2"]},
                },
            }
        )

    @classmethod
    def _get_observed_value(cls, evr):
        result = evr.result
        if result is None:
            return "--"

        expectation_type = evr.expectation_config["expectation_type"]

        if expectation_type == "expect_column_kl_divergence_to_be_less_than":
            return cls._get_kl_divergence_observed_value(evr)
        elif expectation_type == "expect_column_quantile_values_to_be_between":
            return cls._get_quantile_values_observed_value(evr)
        elif expectation_type == "expect_column_pair_cramers_phi_value_to_be_less_than":
            return cls._get_cramers_phi_value_crosstab(evr)
        elif expectation_type == "expect_table_row_count_to_equal_other_table":
            return cls._get_expect_table_row_count_to_equal_other_table_observed_value(
                evr
            )

        if result.get("observed_value"):
            observed_value = result.get("observed_value")
            if isinstance(observed_value, (int, float)) and not isinstance(
                observed_value, bool
            ):
                return num_to_str(observed_value, precision=10, use_locale=True)
            return str(observed_value)
        elif expectation_type == "expect_column_values_to_be_null":
            try:
                notnull_percent = result["unexpected_percent"]
                return (
                    num_to_str(100 - notnull_percent, precision=5, use_locale=True)
                    + "% null"
                )
            except KeyError:
                return "unknown % null"
            except TypeError:
                return "NaN% null"
        elif expectation_type == "expect_column_values_to_not_be_null":
            try:
                null_percent = result["unexpected_percent"]
                return (
                    num_to_str(100 - null_percent, precision=5, use_locale=True)
                    + "% not null"
                )
            except KeyError:
                return "unknown % not null"
            except TypeError:
                return "NaN% not null"
        elif result.get("unexpected_percent") is not None:
            return (
                num_to_str(result.get("unexpected_percent"), precision=5)
                + "% unexpected"
            )
        else:
            return "--"

    @classmethod
    def _process_content_block(cls, content_block, has_failed_evr):
        super()._process_content_block(content_block, has_failed_evr)
        content_block.header_row = ["Status", "Expectation", "Observed Value"]
        content_block.header_row_options = {"Status": {"sortable": True}}

        if has_failed_evr is False:
            styling = deepcopy(content_block.styling) if content_block.styling else {}
            if styling.get("classes"):
                styling["classes"].append(
                    "hide-succeeded-validations-column-section-target-child"
                )
            else:
                styling["classes"] = [
                    "hide-succeeded-validations-column-section-target-child"
                ]

            content_block.styling = styling

    @classmethod
    def _get_content_block_fn(cls, expectation_type):
        expectation_string_fn = getattr(cls, expectation_type, None)
        if expectation_string_fn is None:
            expectation_string_fn = getattr(cls, "_missing_content_block_fn")

        # This function wraps expect_* methods from ExpectationStringRenderer to generate table classes
        def row_generator_fn(evr, styling=None, include_column_name=True):
            expectation = evr.expectation_config
            expectation_string_cell = expectation_string_fn(
                expectation, styling, include_column_name
            )

            status_cell = [cls._get_status_icon(evr)]
            unexpected_statement = []
            unexpected_table = None
            observed_value = ["--"]

            data_docs_exception_message = f"""\
An unexpected Exception occurred during data docs rendering.  Because of this error, certain parts of data docs will \
not be rendered properly and/or may not appear altogether.  Please use the trace, included in this message, to \
diagnose and repair the underlying issue.  Detailed information follows:
            """
            try:
                unexpected_statement = cls._get_unexpected_statement(evr)
            except Exception as e:
                exception_traceback = traceback.format_exc()
                exception_message = (
                    data_docs_exception_message
                    + f'{type(e).__name__}: "{str(e)}".  Traceback: "{exception_traceback}".'
                )
                logger.error(exception_message, e, exc_info=True)
            try:
                unexpected_table = cls._get_unexpected_table(evr)
            except Exception as e:
                exception_traceback = traceback.format_exc()
                exception_message = (
                    data_docs_exception_message
                    + f'{type(e).__name__}: "{str(e)}".  Traceback: "{exception_traceback}".'
                )
                logger.error(exception_message, e, exc_info=True)
            try:
                observed_value = [cls._get_observed_value(evr)]
            except Exception as e:
                exception_traceback = traceback.format_exc()
                exception_message = (
                    data_docs_exception_message
                    + f'{type(e).__name__}: "{str(e)}".  Traceback: "{exception_traceback}".'
                )
                logger.error(exception_message, e, exc_info=True)

            # If the expectation has some unexpected values...:
            if unexpected_statement:
                expectation_string_cell += unexpected_statement
            if unexpected_table:
                expectation_string_cell.append(unexpected_table)

            if len(expectation_string_cell) > 1:
                return [status_cell + [expectation_string_cell] + observed_value]
            else:
                return [status_cell + expectation_string_cell + observed_value]

        return row_generator_fn
