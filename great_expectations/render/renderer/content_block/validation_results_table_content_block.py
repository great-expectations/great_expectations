from six import integer_types

from great_expectations.render.renderer.content_block.expectation_string import ExpectationStringRenderer
from great_expectations.render.types import RenderedComponentContent
from great_expectations.render.util import num_to_str

import pandas as pd
import altair as alt


class ValidationResultsTableContentBlockRenderer(ExpectationStringRenderer):
    _content_block_type = "table"

    _default_element_styling = {
        "default": {
            "classes": ["badge", "badge-secondary"]
        },
        "params": {
            "column": {
                "classes": ["badge", "badge-primary"]
            }
        }
    }
    
    _default_content_block_styling = {
        "body": {
            "classes": ["table"],
        },
        "classes": ["m-3", "table-responsive"],
    }

    @classmethod
    def _get_status_icon(cls, evr):
        if evr["exception_info"]["raised_exception"]:
            return RenderedComponentContent(**{
                "content_block_type": "string_template",
                "string_template": {
                    "template": "$icon",
                    "params": {"icon": ""},
                    "styling": {
                        "params": {
                            "icon": {
                                "classes": ["fas", "fa-exclamation-triangle", "text-warning"],
                                "tag": "i"
                            }
                        }
                    }
                }
            })

        if evr["success"]:
            return RenderedComponentContent(**{
                "content_block_type": "string_template",
                "string_template": {
                    "template": "$icon",
                    "params": {"icon": ""},
                    "styling": {
                        "params": {
                            "icon": {
                                "classes": ["fas", "fa-check-circle", "text-success"],
                                "tag": "i"
                            }
                        }
                    }
                }
            })
        else:
            return RenderedComponentContent(**{
                "content_block_type": "string_template",
                "string_template": {
                    "template": "$icon",
                    "params": {"icon": ""},
                    "styling": {
                        "params": {
                            "icon": {
                                "tag": "i",
                                "classes": ["fas", "fa-times", "text-danger"]
                            }
                        }
                    }
                }
            })

    @classmethod
    def _get_unexpected_table(cls, evr):
        try:
            result = evr["result"]
        except KeyError:
            return None

        if not result.get("partial_unexpected_list") and not result.get("partial_unexpected_counts"):
            return None
        
        table_rows = []
        
        if result.get("partial_unexpected_counts"):
            header_row = ["Unexpected Value", "Count"]
            for unexpected_count in result.get("partial_unexpected_counts"):
                if unexpected_count.get("value"):
                    table_rows.append([unexpected_count.get("value"), unexpected_count.get("count")])
                elif unexpected_count.get("value") == "":
                    table_rows.append(["EMPTY", unexpected_count.get("count")])
                else:
                    table_rows.append(["null", unexpected_count.get("count")])
        else:
            header_row = ["Unexpected Value"]
            for unexpected_value in result.get("partial_unexpected_list"):
                if unexpected_value:
                    table_rows.append([unexpected_value])
                elif unexpected_value == "":
                    table_rows.append(["EMPTY"])
                else:
                    table_rows.append(["null"])
                    
        unexpected_table_content_block = RenderedComponentContent(**{
            "content_block_type": "table",
            "table": table_rows,
            "header_row": header_row,
            "styling": {
                "body": {
                    "classes": ["table-bordered", "table-sm", "mt-3"]
                }
            }
        })
        
        return unexpected_table_content_block

    @classmethod
    def _get_unexpected_statement(cls, evr):
        success = evr["success"]
        result = evr.get("result", {})

        if ("expectation_config" in evr and
                "exception_info" in evr and
                evr["exception_info"]["raised_exception"] is True):
            template_str = "\n\n$expectation_type raised an exception:\n$exception_message"

            return RenderedComponentContent(**{
                "content_block_type": "string_template",
                "string_template": {
                    "template": template_str,
                    "params": {
                        "expectation_type": evr["expectation_config"]["expectation_type"],
                        "exception_message": evr["exception_info"]["exception_message"]
                    },
                    "tag": "strong",
                    "styling": {
                        "classes": ["text-danger"],
                        "params": {
                            "exception_message": {
                                "tag": "code"
                            },
                            "expectation_type": {
                                "classes": ["badge", "badge-danger", "mb-2"]
                            }
                        }
                    }
                },
            })

        if success or not result.get("unexpected_count"):
            return None
        else:
            unexpected_count = num_to_str(result["unexpected_count"], use_locale=True, precision=20)
            unexpected_percent = num_to_str(result["unexpected_percent"], precision=4) + "%"
            element_count = num_to_str(result["element_count"], use_locale=True, precision=20)
            
            template_str = "\n\n$unexpected_count unexpected values found. " \
                           "$unexpected_percent of $element_count total rows."

            return RenderedComponentContent(**{
                "content_block_type": "string_template",
                "string_template": {
                    "template": template_str,
                    "params": {
                        "unexpected_count": unexpected_count,
                        "unexpected_percent": unexpected_percent,
                        "element_count": element_count
                    },
                    "tag": "strong",
                    "styling": {
                        "classes": ["text-danger"]
                    }
                }
            })

    @classmethod
    def _get_observed_value(cls, evr):
        try:
            result = evr["result"]
        except KeyError:
            return "--"
            
        expectation_type = evr["expectation_config"]["expectation_type"]

        if expectation_type == "expect_column_kl_divergence_to_be_less_than":
            if not evr["result"].get("details"):
                return "--"

            weights = evr["result"]["details"]["observed_partition"]["weights"]
            if len(weights) <= 10:
                height = 200
                width = 200
                col_width = 4
            else:
                height = 300
                width = 300
                col_width = 6
                
            if evr["result"]["details"]["observed_partition"].get("bins"):
                bins = evr["result"]["details"]["observed_partition"]["bins"]
                bins_x1 = [round(value, 1) for value in bins[:-1]]
                bins_x2 = [round(value, 1) for value in bins[1:]]
        
                df = pd.DataFrame({
                    "bin_min": bins_x1,
                    "bin_max": bins_x2,
                    "fraction": weights,
                })

                bars = alt.Chart(df).mark_bar().encode(
                    x='bin_min:O',
                    x2='bin_max:O',
                    y="fraction:Q"
                ).properties(width=width, height=height, autosize="fit")
                chart = bars.to_json()
            elif evr["result"]["details"]["observed_partition"].get("values"):
                values = evr["result"]["details"]["observed_partition"]["values"]
    
                df = pd.DataFrame({
                    "values": values,
                    "fraction": weights
                })

                bars = alt.Chart(df).mark_bar().encode(
                    x='values:N',
                    y="fraction:Q"
                ).properties(width=width, height=height, autosize="fit")
                chart = bars.to_json()
            
            return {
                "content_block_type": "graph",
                "graph": chart,
                "styling": {
                    "classes": ["col-" + str(col_width)],
                    "styles": {
                        "margin-top": "20px",
                    }
                }
            }

        if result.get("observed_value"):
            observed_value = result.get("observed_value")
            if isinstance(observed_value, (integer_types, float)) and not isinstance(observed_value, bool):
                return num_to_str(observed_value, precision=10, use_locale=True)
            return str(observed_value)
        elif expectation_type == "expect_column_values_to_be_null":
            try:
                notnull_percent = result["unexpected_percent"]
                return num_to_str(100 - notnull_percent, precision=5, use_locale=True) + "% null"
            except KeyError:
                return "unknown % null"
        elif expectation_type == "expect_column_values_to_not_be_null":
            try:
                null_percent = result["unexpected_percent"]
                return num_to_str(100 - null_percent, precision=5, use_locale=True) + "% not null"
            except KeyError:
                return "unknown % not null"
        elif result.get("unexpected_percent") is not None:
            return num_to_str(result.get("unexpected_percent"), precision=5) + "% unexpected"
        else:
            return "--"

    @classmethod
    def _process_content_block(cls, content_block):
        super(ValidationResultsTableContentBlockRenderer, cls)._process_content_block(content_block)
        content_block.update({
            "header_row": ["Status", "Expectation", "Observed Value"]
        })

    @classmethod
    def _get_content_block_fn(cls, expectation_type):
        expectation_string_fn = getattr(cls, expectation_type, None)
        if expectation_string_fn is None:
            expectation_string_fn = getattr(cls, "_missing_content_block_fn")

        #This function wraps expect_* methods from ExpectationStringRenderer to generate table classes
        def row_generator_fn(evr, styling=None, include_column_name=True):
            expectation = evr["expectation_config"]
            expectation_string_cell = expectation_string_fn(expectation, styling, include_column_name)

            status_cell = [cls._get_status_icon(evr)]
            unexpected_statement = cls._get_unexpected_statement(evr)
            unexpected_table = cls._get_unexpected_table(evr)
            observed_value = [cls._get_observed_value(evr)]

            #If the expectation has some unexpected values...:
            if unexpected_statement or unexpected_table:
                expectation_string_cell.append(unexpected_statement)
                expectation_string_cell.append(unexpected_table)
            
            if len(expectation_string_cell) > 1:
                return [status_cell + [expectation_string_cell] + observed_value]
            else:
                return [status_cell + expectation_string_cell + observed_value]
        
        return row_generator_fn
