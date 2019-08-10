from great_expectations.render.renderer.content_block.expectation_string import ExpectationStringRenderer
from great_expectations.render.types import (
    RenderedComponentContent
)


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
                else:
                    table_rows.append(["null", unexpected_count.get("count")])
        else:
            header_row = ["Unexpected Value"]
            for unexpected_value in result.get("partial_unexpected_list"):
                if unexpected_value.get("value"):
                    table_rows.append([unexpected_value])
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
        try:
            result = evr["result"]
        except KeyError:
            return RenderedComponentContent(**{
                "content_block_type": "string_template",
                "string_template": {
                    "template": "Expectation failed to execute.",
                    "params": {},
                    "tag": "strong",
                    "styling": {
                        "classes": ["text-warning"]
                    }
                }
            })

        if success or not result.get("unexpected_count"):
            return None
        else:
            unexpected_count = result["unexpected_count"]
            unexpected_percent = "%.2f%%" % (result["unexpected_percent"] * 100.0)
            element_count = result["element_count"]
            
            template_str = "\n\n$unexpected_count unexpected values found. $unexpected_percent of $element_count total rows."
            
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

        if result.get("observed_value"):
            return result.get("observed_value")
        elif expectation_type == "expect_column_values_to_be_null":
            element_count = result["element_count"]
            unexpected_count = result["unexpected_count"]
            null_count = element_count - unexpected_count
            return "{null_count} null".format(null_count=null_count)
        elif expectation_type == "expect_column_values_to_not_be_null":
            null_count = result["unexpected_count"]
            return "{null_count} null".format(null_count=null_count)
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
        expectation_string_fn = getattr(ExpectationStringRenderer, expectation_type, None)
        if expectation_string_fn is None:
            expectation_string_fn = getattr(ExpectationStringRenderer, "_missing_content_block_fn")

        #This function wraps expect_* methods from ExpectationStringRenderer to generate table classes
        def row_generator_fn(evr, styling=None, include_column_name=True):
            expectation = evr["expectation_config"]
            expectation_string_obj = expectation_string_fn(expectation, styling, include_column_name)

            # if expectation["exception_info"]["raised_exception"] == True:
            status_cell = [cls._get_status_icon(evr)]
            unexpected_statement = cls._get_unexpected_statement(evr)
            unexpected_table = cls._get_unexpected_table(evr)
            expectation_cell = expectation_string_obj
            observed_value = [str(cls._get_observed_value(evr))]

            #If the expectation has some unexpected values...:
            if unexpected_statement or unexpected_table:
                expectation_string_obj.append(unexpected_statement)
                expectation_string_obj.append(unexpected_table)
                return [status_cell + [expectation_cell] + observed_value]

            else:
                return [status_cell + expectation_cell + observed_value]
        
        return row_generator_fn