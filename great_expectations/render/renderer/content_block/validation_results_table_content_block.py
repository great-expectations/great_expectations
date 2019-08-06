from great_expectations.render.renderer.content_block.expectation_string import ExpectationStringRenderer


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
        if evr["success"]:
            return {
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
            }
        else:
            return {
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
            }

    @classmethod
    def _get_exception_table(cls, evr):
        result = evr["result"]
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
                    
        exception_table_content_block = {
            "content_block_type": "table",
            "table": table_rows,
            "header_row": header_row,
            "styling": {
                "body": {
                    "classes": ["table-bordered", "table-sm", "mt-3"]
                }
            }
        }
        
        return exception_table_content_block

    @classmethod
    def _get_exception_statement(cls, evr):
        success = evr["success"]
        result = evr["result"]
        
        if success or not result.get("unexpected_count"):
            return None
        else:
            unexpected_count = result["unexpected_count"]
            unexpected_percent = "%.2f%%" % (result["unexpected_percent"] * 100.0)
            element_count = result["element_count"]
            
            template_str = "\n\n$unexpected_count exceptions found. $unexpected_percent of $element_count total rows."
            
            return {
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
            }

    @classmethod
    def _get_observed_value(cls, evr):
        result = evr["result"]
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
    def generate_expectation_row(cls, expectation_type):
        expectation_string_fn = getattr(ExpectationStringRenderer, expectation_type, None)
        if expectation_string_fn is None:
            expectation_string_fn = getattr(ExpectationStringRenderer, "_missing_content_block_fn")
        
        def row_generator_fn(evr, styling=None, include_column_name=True):
            expectation = evr["expectation_config"]
            expectation_string_obj = expectation_string_fn(expectation, styling, include_column_name)
            
            status_cell = [cls._get_status_icon(evr)]
            exception_statement = cls._get_exception_statement(evr)
            exception_table = cls._get_exception_table(evr)
            expectation_cell = expectation_string_obj
            observed_value = [str(cls._get_observed_value(evr))]

            if exception_statement or exception_table:
                expectation_string_obj.append(exception_statement)
                expectation_string_obj.append(exception_table)
                return [status_cell + [expectation_cell] + observed_value]
            
            return [status_cell + expectation_cell + observed_value]
        
        return row_generator_fn