import json

from . import SnippetRenderer
from .util import render_parameter


class ExpectationBulletPointSnippetRenderer(SnippetRenderer):
    @classmethod
    def validate_input(cls):
        return True

    @classmethod
    def render(cls, expectation, include_column_name=False):

        #!!! What about expectations without column names?
        if include_column_name:
            optional_column_name_prefix = expectation["kwargs"]["column"]
        else:
            optional_column_name_prefix = ""

        if expectation["expectation_type"] == "expect_column_to_exist":
            return optional_column_name_prefix+" is a required field."

        elif expectation["expectation_type"] == "expect_column_values_to_be_of_type":
            return " is of type %s." % (
                render_parameter(expectation["kwargs"]["type_"], "s")
            )

        # elif expectation["expectation_type"] in ["expect_column_values_to_be_of_type", "expect_column_values_to_be_of_semantic_type"]:
            # print(json.dumps(expectation, indent=2))
            # column_type = result["expectation_config"]["kwargs"]["type_"]

        elif expectation["expectation_type"] == "expect_column_values_to_not_be_null":
            if "mostly" in expectation["kwargs"]:
                return " must not be missing more than %s%% of the time." % (
                    render_parameter(
                        100*expectation["kwargs"]["mostly"], ".1f")
                )
            else:
                return " must never be missing."

        elif expectation["expectation_type"] == "expect_column_values_to_be_null":
            if "mostly" in expectation["kwargs"]:
                return " must missing at least %s%% of the time." % (
                    render_parameter(
                        100*(1-expectation["kwargs"]["mostly"]), ".1f")
                )

            else:
                return " must always be missing."

        elif expectation["expectation_type"] == "expect_column_values_to_be_dateutil_parseable":
            if "mostly" in expectation["kwargs"]:
                return " must be formatted as date or time at least %.1f\% of the time." % (expectation["kwargs"]["mostly"])
            else:
                return " must always be formatted as a date or time."

        elif expectation["expectation_type"] == "expect_column_value_lengths_to_equal":
            # print(json.dumps(expectation, indent=2))
            if "mostly" in expectation["kwargs"]:
                return "must be exactly %s characters long at least %s%% of the time." % (
                    render_parameter(expectation["kwargs"]["value"], "d"),
                    render_parameter(
                        100*expectation["kwargs"]["mostly"], ".1f"),
                )
            else:
                return "must be exactly <span class=\"param-span\">%d</span> characters long." % (expectation["kwargs"]["value"])

        elif expectation["expectation_type"] == "expect_column_value_lengths_to_be_between":
            if (expectation["kwargs"]["min_value"] == None) and (expectation["kwargs"]["max_value"] == None):
                return " has a bogus %s expectation." % (
                    render_parameter(
                        "expect_column_value_lengths_to_be_between", "s")
                )

            if "mostly" in expectation["kwargs"]:
                if expectation["kwargs"]["min_value"] != None and expectation["kwargs"]["max_value"] != None:
                    return " must be between %s and %s characters long at least %s%% of the time." % (
                        render_parameter(
                            expectation["kwargs"]["min_value"], "d"),
                        render_parameter(
                            expectation["kwargs"]["max_value"], "d"),
                        render_parameter(
                            expectation["kwargs"]["mostly"], ".1f"),
                    )

                elif expectation["kwargs"]["min_value"] == None:
                    return " must be less than %d characters long at least %.1f\% of the time." % (
                        expectation["kwargs"]["max_value"],
                        expectation["kwargs"]["mostly"],
                    )

                elif expectation["kwargs"]["max_value"] == None:
                    return " must be more than %d characters long at least %.1f\% of the time." % (
                        expectation["kwargs"]["min_value"],
                        expectation["kwargs"]["mostly"],
                    )

            else:
                if expectation["kwargs"]["min_value"] != None and expectation["kwargs"]["max_value"] != None:
                    return " must always be between %s and %s characters long." % (
                        render_parameter(
                            expectation["kwargs"]["min_value"], "d"),
                        render_parameter(
                            expectation["kwargs"]["max_value"], "d"),
                    )

                elif expectation["kwargs"]["min_value"] == None:
                    return " must always be less than %s characters long." % (
                        render_parameter(
                            expectation["kwargs"]["max_value"], "d"),
                    )

                elif expectation["kwargs"]["max_value"] == None:
                    return " must always be more than %s characters long." % (
                        render_parameter(
                            expectation["kwargs"]["min_value"], "d"),
                    )

        elif expectation["expectation_type"] == "expect_column_values_to_be_between":
            # print(json.dumps(expectation, indent=2))
            if "mostly" in expectation["kwargs"]:
                return " must be between %d and %d at least %.1f\% of the time."
            else:
                if "parse_strings_as_datetimes" in expectation["kwargs"]:
                    return " must always be a date between %s and %s." % (str(expectation["kwargs"]["min_value"]), str(expectation["kwargs"]["max_value"]))
                else:
                    return " must always be between %d and %d." % (expectation["kwargs"]["min_value"], expectation["kwargs"]["max_value"])

        elif expectation["expectation_type"] == "expect_column_values_to_be_unique":
            # print(json.dumps(expectation, indent=2))
            if "mostly" in expectation["kwargs"]:
                return " must be unique at least %.1f\% of the time."
            else:
                return " must always be unique."

        elif expectation["expectation_type"] == "expect_column_mean_to_be_between":
            return " must have a mean value between %d and %d." % (expectation["kwargs"]["min_value"], expectation["kwargs"]["max_value"])

        elif expectation["expectation_type"] == "expect_column_median_to_be_between":
            return " must have a median value between %d and %d." % (expectation["kwargs"]["min_value"], expectation["kwargs"]["max_value"])

        elif expectation["expectation_type"] == "expect_column_stdev_to_be_between":
            return " must have a standard deviation between %d and %d." % (expectation["kwargs"]["min_value"], expectation["kwargs"]["max_value"])

        elif expectation["expectation_type"] == "expect_column_unique_value_count_to_be_between":
            if (expectation["kwargs"]["min_value"] == None) and (expectation["kwargs"]["max_value"] == None):
                return " has a bogus %s expectation." % (
                    render_parameter(
                        "expect_column_unique_value_count_to_be_between", "s")
                )

            elif expectation["kwargs"]["min_value"] == None:
                return " must have fewer than %s unique values." % (
                    render_parameter(expectation["kwargs"]["max_value"], "d")
                )

            elif expectation["kwargs"]["max_value"] == None:
                return " must have at least %s unique values." % (
                    render_parameter(expectation["kwargs"]["min_value"], "d")
                )

            else:
                return " must have between %s and %s unique values." % (
                    render_parameter(expectation["kwargs"]["min_value"], "d"),
                    render_parameter(expectation["kwargs"]["max_value"], "d"),
                )

        elif expectation["expectation_type"] == "expect_column_values_to_not_match_regex":
            # FIXME: Need to add logic for mostly
            return " must not match this regular expression: <span class=\"example-list\">%s</span>." % (expectation["kwargs"]["regex"],)

        elif expectation["expectation_type"] == "expect_column_values_to_match_regex":
            # FIXME: Need to add logic for mostly
            return " must match this regular expression: <span class=\"example-list\">%s</span>." % (expectation["kwargs"]["regex"],)

        elif expectation["expectation_type"] == "expect_column_values_to_match_regex_list":
            # FIXME: Need to add logic for mostly
            return " must match at least one of these regular expressions: <span class=\"example-list\">%s</span>" % (
                " ".join([render_parameter(regex, "s")
                          for regex in expectation["kwargs"]["regex_list"]]),
            )

        elif expectation["expectation_type"] == "expect_column_values_to_not_match_regex_list":
            # FIXME: Need to add logic for mostly
            return " must not match this regular expression: <span class=\"example-list\">%s</span>." % (expectation["kwargs"]["regex_list"],)

        elif expectation["expectation_type"] == "expect_column_values_to_be_json_parseable":
            # print(json.dumps(expectation["kwargs"], indent=2))
            # FIXME: Need to add logic for mostly
            return " must be a parseable JSON object."

        # elif expectation["expectation_type"] == "expect_column_values_to_not_match_regex":

        elif expectation["expectation_type"] == "expect_column_proportion_of_unique_values_to_be_between":
            if (expectation["kwargs"]["min_value"] == None) and (expectation["kwargs"]["max_value"] == None):
                return " has a bogus %s expectation." % (
                    render_parameter(
                        "expect_column_proportion_of_unique_values_to_be_between", "s")
                )

            elif expectation["kwargs"]["min_value"] == None:
                return " must have fewer than %s%% unique values." % (
                    render_parameter(
                        100*expectation["kwargs"]["max_value"], ".1f")
                )

            elif expectation["kwargs"]["max_value"] == None:
                return " must have at least %s%% unique values." % (
                    render_parameter(
                        100*expectation["kwargs"]["min_value"], ".1f")
                )

            else:
                return " must have between %s and %s%% unique values." % (
                    render_parameter(
                        100*expectation["kwargs"]["min_value"], ".1f"),
                    render_parameter(
                        100*expectation["kwargs"]["max_value"], ".1f"),
                )

        elif expectation["expectation_type"] == "expect_column_values_to_be_in_set":
            return " must belong to this set: <span class=\"example-list\">%s</span>" % (
                " ".join([render_parameter(value, "s")
                          for value in expectation["kwargs"]["values_set"]]),
            )
           # example_list = {
           #     "box_type": "Report-ExampleList",
           #     "props": {
           #         "subtitle": "Common values include:",
           #         "list": expectation["kwargs"]["values_set"][:20],
           #     }
           # }

        # Note: This is a fake expectation generated as a quasi_expectation by shackleton.
        elif expectation["expectation_type"] == "expect_common_values_to_be_in_list":
            example_list = {
                "box_type": "Report-ExampleList",
                "props": {
                    "subtitle": "Common values include:",
                    "list": expectation["kwargs"]["values_list"][:20],
                }
            }

        elif expectation["expectation_type"] == "expect_column_kl_divergence_to_be_less_than":
            # print(json.dumps(expectation["kwargs"], indent=2))
            # FIXME: Potentially very brittle
            data_values = []
            for i, v in enumerate(expectation["kwargs"]["partition_object"]["values"]):
                data_values.append({
                    "value": v,
                    "weight": expectation["kwargs"]["partition_object"]["weights"][i],
                })

            # FIXME: This graph code is okay, but not great.
            graph = {
                "box_type": "Report-Graph",
                "props": {
                    "graph_type": "bar",
                    "subtitle": "Distribution of common values",
                    "vega_lite_object": {
                        "data": {
                            "values": data_values
                        },
                        "$schema": "https://vega.github.io/schema/vega-lite/v1.2.1.json",
                        "encoding": {
                            "y": {
                                "sort": {"field": "weight", "order": "ascending", "op": "values"},
                                "field": "value",
                                "type": "nominal"
                            },
                            "x": {"field": "weight", "type": "quantitative"}
                        },
                        "config": {"cell": {"width": 550, "height": 350}},
                        "mark": "bar"
                    }
                }
            }

        else:
            # FIXME: This warning is actually pretty helpful
            print("WARNING: Unhandled expectation_type %s" %
                  expectation["expectation_type"],)
